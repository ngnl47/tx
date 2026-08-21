package tests

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
)

type recordingRedisStringClient struct {
	values      map[string]string
	evals       []redisEvalCall
	mgets       [][]string
	mgetValues  []string
	mgetBatches [][]string
	evalResult  string
	evalResults []string
	evalIndex   int
	hasResult   bool
	evalErr     error
}

type redisEvalCall struct {
	script string
	keys   []string
	args   []any
}

type recordingRedisEvalOnlyClient struct {
	results []string
	evals   []redisEvalCall
}

type recordingRedisRouter struct {
	names map[any]string
	calls []recordingRedisRouteCall
}

type recordingRedisRouteCall struct {
	routeType string
	shardKey  any
}

func (c *recordingRedisStringClient) Get(_ context.Context, key string) (string, bool, error) {
	if c.values == nil {
		return "", false, nil
	}
	value, ok := c.values[key]
	return value, ok, nil
}

func (c *recordingRedisStringClient) Set(_ context.Context, key string, value string) error {
	if c.values == nil {
		c.values = map[string]string{}
	}
	c.values[key] = value
	return nil
}

func (c *recordingRedisStringClient) Eval(_ context.Context, script string, keys []string, args ...any) (string, error) {
	c.evals = append(c.evals, redisEvalCall{
		script: script,
		keys:   append([]string(nil), keys...),
		args:   append([]any(nil), args...),
	})
	if c.evalErr != nil {
		return "", c.evalErr
	}
	if c.hasResult {
		return c.evalResult, nil
	}
	if c.evalIndex < len(c.evalResults) {
		result := c.evalResults[c.evalIndex]
		c.evalIndex++
		return result, nil
	}
	return "ok", nil
}

func (c *recordingRedisStringClient) MGet(_ context.Context, keys ...string) ([]string, error) {
	c.mgets = append(c.mgets, append([]string(nil), keys...))
	if c.values != nil {
		out := make([]string, len(keys))
		for i, key := range keys {
			out[i] = c.values[key]
		}
		return out, nil
	}
	if len(c.mgetBatches) > 0 {
		out := append([]string(nil), c.mgetBatches[0]...)
		c.mgetBatches = c.mgetBatches[1:]
		return out, nil
	}
	return append([]string(nil), c.mgetValues...), nil
}

func (c *recordingRedisEvalOnlyClient) Eval(_ context.Context, script string, keys []string, args ...any) (string, error) {
	c.evals = append(c.evals, redisEvalCall{
		script: script,
		keys:   append([]string(nil), keys...),
		args:   append([]any(nil), args...),
	})
	if len(c.results) == 0 {
		return "", nil
	}
	result := c.results[0]
	c.results = c.results[1:]
	return result, nil
}

func (r *recordingRedisRouter) RouteRedis(_ context.Context, routeType string, shardKey any) (string, error) {
	r.calls = append(r.calls, recordingRedisRouteCall{routeType: routeType, shardKey: shardKey})
	return r.names[shardKey], nil
}

func TestRedisAdapterAppliesChangesWithSingleLuaEval(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet},
		{Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)), Op: change.OpSet},
	}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   deltaTestGoNames(),
		change.FieldNameJSON: deltaTestJSONNames(),
	})
	values := testDeltaValues(map[string]any{
		"Name":                "alice",
		"Items.1001.Quantity": int64(9),
	})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	call := client.evals[0]
	if len(call.keys) != 1 || call.keys[0] != "role:{1001}" {
		t.Fatalf("eval keys = %#v, want role:{1001}", call.keys)
	}
	if len(call.args) != 8 {
		t.Fatalf("eval args = %#v, want two suffix/document/mode triples plus upsert and ttlMode", call.args)
	}
	if call.args[len(call.args)-2] != "1" || call.args[len(call.args)-1] != "0" {
		t.Fatalf("eval tail args = %#v, want upsert=1 ttlMode=0", call.args[len(call.args)-2:])
	}
}

func TestRedisAdapterReturnsTransportError(t *testing.T) {
	transportErr := errors.New("redis unavailable")
	adapter := txredis.NewAdapter(&recordingRedisStringClient{evalErr: storage.NewBackendError(storage.BackendRedis, "eval", transportErr)})
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: true})
	if !errors.Is(err, transportErr) {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want transport error", err)
	}
	if backend, ok := storage.BackendFromError(err); !ok || backend != storage.BackendRedis {
		t.Fatalf("BackendFromError = %v, %t, want redis, true", backend, ok)
	}
}

func TestRedisAdapterDoesNotClassifyUnmarkedClientError(t *testing.T) {
	adapter := txredis.NewAdapter(&recordingRedisStringClient{evalErr: errors.New("invalid redis response")})
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: true})
	if _, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError classified unmarked client error: %v", err)
	}
}

func TestRedisAdapterRejectsNilClientForDocumentOperations(t *testing.T) {
	adapter := txredis.NewAdapter(nil)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	key := storage.RecordKey{ID: int64(1001)}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, key, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	assertRedisClientNotConfigured(t, err)

	err = adapter.ApplyDelete(context.Background(), spec, key)
	assertRedisClientNotConfigured(t, err)

	_, _, err = adapter.LoadDocument(context.Background(), spec, key)
	assertRedisClientNotConfigured(t, err)

	_, _, err = adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{key})
	assertRedisClientNotConfigured(t, err)

	err = adapter.ReplaceDocumentWithOptions(context.Background(), spec, key, map[string]any{"name": "alice"}, storage.ApplyOptions{})
	assertRedisClientNotConfigured(t, err)

	_, _, err = adapter.ApplyDeleteWithRevision(context.Background(), spec, key, "1")
	assertRedisClientNotConfigured(t, err)

	_, err = adapter.ReadRecordRevision(context.Background(), spec, key)
	assertRedisClientNotConfigured(t, err)

	_, err = adapter.LockRecord(context.Background(), spec, key, storage.LockOptions{TTL: time.Second})
	assertRedisClientNotConfigured(t, err)

	var typedNil *recordingRedisStringClient
	typedNilAdapter := txredis.NewAdapter(typedNil)
	err = typedNilAdapter.ApplyDelete(context.Background(), spec, key)
	assertRedisClientNotConfigured(t, err)
}

func TestRedisAdapterRejectsDeltaSetWithoutEffectivePatch(t *testing.T) {
	adapter := txredis.NewAdapter(&recordingRedisStringClient{})
	err := adapter.ApplyDeltasWithOptions(
		context.Background(),
		meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}},
		storage.RecordKey{ID: int64(1001)},
		change.DeltaSet{{Op: change.OpSet}},
		testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()),
		testDeltaValues(nil),
		storage.ApplyOptions{},
	)
	if !errors.Is(err, storage.ErrNoEffectiveDeltas) {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want ErrNoEffectiveDeltas", err)
	}
}

func assertRedisClientNotConfigured(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, txredis.ErrClientNotConfigured) {
		t.Fatalf("error = %v, want ErrClientNotConfigured", err)
	}
}

func TestRedisAdapterReplaceDocumentWithOptionsUsesFullReplaceAndTTL(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	err := adapter.ReplaceDocumentWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, map[string]any{
		"name":  "alice",
		"items": map[string]any{"1001": map[string]any{"quantity": int64(9)}},
	}, storage.ApplyOptions{TTLSeconds: 3600})
	if err != nil {
		t.Fatalf("ReplaceDocumentWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	call := client.evals[0]
	if len(call.keys) != 1 || call.keys[0] != "role:{1001}" {
		t.Fatalf("eval keys = %#v, want role:{1001}", call.keys)
	}
	if len(call.args) != 8 || call.args[len(call.args)-2] != "1" || call.args[len(call.args)-1] != "3600" {
		t.Fatalf("eval args = %#v, want upsert true and ttl 3600", call.args)
	}
	triples := evalTriples(t, call.args)
	if triples[""].mode != "1" || !strings.Contains(triples[""].body, `"name":"alice"`) {
		t.Fatalf("main triple = %#v, want full replacement with name", triples[""])
	}
	if triples["items"].mode != "1" || !strings.Contains(triples["items"].body, `"items"`) {
		t.Fatalf("items triple = %#v, want full replacement with items", triples["items"])
	}
}

func TestRedisAdapterReplaceDocumentKeepsEmptyMainDocumentForSubKeyOnlyData(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	err := adapter.ReplaceDocumentWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
		map[string]any{"items": map[string]any{"1001": map[string]any{"quantity": int64(9)}}},
		storage.ApplyOptions{},
	)
	if err != nil {
		t.Fatalf("ReplaceDocumentWithOptions returned error: %v", err)
	}
	triples := evalTriples(t, client.evals[0].args)
	main, ok := triples[""]
	if !ok || main.mode != "4" || main.body != "{}" {
		t.Fatalf("main replacement = %#v, want empty root document", main)
	}
	if _, ok := triples["items"]; !ok {
		t.Fatalf("replace triples = %#v, want items subKey", triples)
	}
}

func TestRedisAdapterReplaceDocumentClearsOmittedSubKey(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"name":  {},
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	err := adapter.ReplaceDocumentWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
		map[string]any{"name": "alice"},
		storage.ApplyOptions{},
	)
	if err != nil {
		t.Fatalf("ReplaceDocumentWithOptions returned error: %v", err)
	}
	triples := evalTriples(t, client.evals[0].args)
	items, ok := triples["items"]
	if !ok || items.mode != "1" || items.body != "{}" {
		t.Fatalf("items replacement = %#v, want stale subKey deletion", items)
	}
}

func TestRedisAdapterReplaceDocumentRoutesCanonicalJSONNameToSubKey(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Type: meta.Type{Fields: []meta.Field{
			{Name: "Items", JSONName: "items"},
		}},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	err := adapter.ReplaceDocumentWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
		map[string]any{"items": map[string]any{"1001": map[string]any{"quantity": int64(9)}}},
		storage.ApplyOptions{},
	)
	if err != nil {
		t.Fatalf("ReplaceDocumentWithOptions returned error: %v", err)
	}
	triples := evalTriples(t, client.evals[0].args)
	items, ok := triples["items"]
	if !ok || !strings.Contains(items.body, `"items"`) {
		t.Fatalf("items replacement = %#v, want canonical JSON field routed to Redis subKey", items)
	}
}

func TestRedisAdapterRejectsNilReplacementDocument(t *testing.T) {
	adapter := txredis.NewAdapter(&recordingRedisStringClient{})
	err := adapter.ReplaceDocumentWithOptions(
		context.Background(),
		meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}},
		storage.RecordKey{ID: int64(1001)},
		nil,
		storage.ApplyOptions{},
	)
	if !errors.Is(err, storage.ErrNilDocument) {
		t.Fatalf("ReplaceDocumentWithOptions error = %v, want ErrNilDocument", err)
	}
}

func TestRedisAdapterUsesShardKeyAsHashTagWhenRouteByDiffersFromID(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{
			Key:     "role",
			IDField: "roleId",
			Route:   meta.RouteSpec{By: "serverId"},
		},
	}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001), ShardKey: int64(7)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	if got, want := client.evals[0].keys, []string{"role:{7}:1001"}; !sameStrings(got, want) {
		t.Fatalf("eval keys = %#v, want %#v", got, want)
	}
}

func TestRedisAdapterLuaUsesCompatibleArrayDetection(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, map[change.FieldID]string{deltaFieldName: "tags"}), testDeltaValues(map[string]any{"Name": []string{"a"}}), storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	script := client.evals[0].script
	for _, want := range []string{
		"cjson.decode_array_with_array_mt(true)",
		"local function get_type(v)",
		"getmetatable(v) == cjson.array_mt",
		"return \"array\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("eval script does not contain %q:\n%s", want, script)
		}
	}
}

func TestRedisAdapterApplyDeltasWithOptionsDisablesUpsert(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if args[len(args)-2] != "0" {
		t.Fatalf("upsert arg = %#v, want 0; args=%#v", args[len(args)-2], args)
	}
}

func TestRedisAdapterApplyDeltasWithOptionsPassesTTL(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{
		Upsert:     true,
		TTLSeconds: 120,
	})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if args[len(args)-1] != "120" {
		t.Fatalf("ttl arg = %#v, want 120; args=%#v", args[len(args)-1], args)
	}
}

func TestRedisAdapterApplyDeltasWithOptionsAddsTTLOnlySubKeys(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items":  {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
			"heroes": {Redis: &meta.RedisFieldSpec{SubKey: "heroes"}},
		},
	}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{
		Upsert:     true,
		TTLSeconds: 120,
	})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	triples := evalTriples(t, client.evals[0].args)
	if triples[""].mode != "0" {
		t.Fatalf("main mode = %#v, want 0; triples=%#v", triples[""].mode, triples)
	}
	for _, suffix := range []string{"heroes", "items"} {
		triple, ok := triples[suffix]
		if !ok {
			t.Fatalf("missing ttl-only suffix %q in %#v", suffix, triples)
		}
		if triple.body != "" || triple.mode != "-1" {
			t.Fatalf("ttl-only triple %q = %#v, want empty body mode -1", suffix, triple)
		}
	}
}

func TestRedisAdapterApplyDeltasWithOptionsReturnsNotExists(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "ne", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if !errors.Is(err, storage.ErrNotExists) {
		t.Fatalf("ApplyDeltasWithOptions err = %v, want ErrNotExists", err)
	}
}

func TestRedisAdapterApplyDeltasWithOptionsReturnsPathTypeMismatch(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "pt:dailyTask", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if !errors.Is(err, storage.ErrPathTypeMismatch) {
		t.Fatalf("ApplyDeltasWithOptions err = %v, want ErrPathTypeMismatch", err)
	}
	if !strings.Contains(err.Error(), "dailyTask") {
		t.Fatalf("ApplyDeltasWithOptions err = %v, want path", err)
	}
}

func TestRedisAdapterApplyDeltasWithOptionsRejectsUnknownScriptResult(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "unexpected", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if err == nil {
		t.Fatal("ApplyDeltasWithOptions error = nil, want unknown script result error")
	}
	if !strings.Contains(err.Error(), "unexpected script result") || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want unknown result details", err)
	}
}

func TestRedisAdapterApplyDeleteDeletesMainSubKeysAndVersion(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Type:  meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items":  {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
			"heroes": {Redis: &meta.RedisFieldSpec{SubKey: "heroes"}},
		},
	}

	err := adapter.ApplyDelete(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want one", client.evals)
	}
	want := []string{"role:{1001}", "role:{1001}:heroes", "role:{1001}:items", "role:{1001}@v"}
	if got := client.evals[0].keys; !sameStrings(got, want) {
		t.Fatalf("delete keys = %#v, want %#v", got, want)
	}
	if len(client.evals[0].args) != 0 {
		t.Fatalf("delete args = %#v, want empty", client.evals[0].args)
	}
	if !strings.Contains(client.evals[0].script, "redis.call(\"del\"") {
		t.Fatalf("delete script = %q, want del command", client.evals[0].script)
	}
}

func TestRedisAdapterApplyDeleteRejectsUnknownScriptResult(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "unexpected", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

	err := adapter.ApplyDelete(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err == nil {
		t.Fatal("ApplyDelete error = nil, want unknown script result error")
	}
	if !strings.Contains(err.Error(), "unexpected script result") || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("ApplyDelete error = %v, want unknown result details", err)
	}
}

func TestRedisAdapterApplyDeleteWithRevisionUsesVersionKeyCompare(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	deleted, current, err := adapter.ApplyDeleteWithRevision(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "5")
	if err != nil {
		t.Fatalf("ApplyDeleteWithRevision returned error: %v", err)
	}
	if !deleted || current != "" {
		t.Fatalf("ApplyDeleteWithRevision deleted/current = %v/%q, want deleted", deleted, current)
	}
	if got, want := client.evals[0].keys, []string{"role:{1001}", "role:{1001}:items", "role:{1001}@v"}; !sameStrings(got, want) {
		t.Fatalf("delete keys = %#v, want %#v", got, want)
	}
	if got, want := client.evals[0].args, []any{"5"}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("delete args = %#v, want %#v", got, want)
	}
	if !strings.Contains(client.evals[0].script, "ARGV[1]") || !strings.Contains(client.evals[0].script, "get") {
		t.Fatalf("delete script = %q, want revision compare", client.evals[0].script)
	}
}

func TestRedisAdapterApplyDeleteWithRevisionReportsChangedRevision(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "6", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

	deleted, current, err := adapter.ApplyDeleteWithRevision(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "5")
	if err != nil {
		t.Fatalf("ApplyDeleteWithRevision returned error: %v", err)
	}
	if deleted || current != "6" {
		t.Fatalf("ApplyDeleteWithRevision deleted/current = %v/%q, want changed revision 6", deleted, current)
	}
}

func TestRedisAdapterApplyDeleteWithRevisionRejectsInvalidRevisionProtocol(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "unexpected", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

	deleted, current, err := adapter.ApplyDeleteWithRevision(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "5")
	if err == nil {
		t.Fatal("ApplyDeleteWithRevision error = nil, want invalid revision protocol error")
	}
	if deleted || current != "" {
		t.Fatalf("ApplyDeleteWithRevision deleted/current = %v/%q, want no delete result on protocol error", deleted, current)
	}
	if !strings.Contains(err.Error(), "unexpected script result") || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("ApplyDeleteWithRevision error = %v, want unknown result details", err)
	}
}

func TestRedisAdapterApplyDeleteWithRevisionRejectsMissingExpectedRevision(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

	_, _, err := adapter.ApplyDeleteWithRevision(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "")
	if err == nil || !strings.Contains(err.Error(), "missing record revision") {
		t.Fatalf("ApplyDeleteWithRevision error = %v, want missing record revision", err)
	}
	if len(client.evals) != 0 {
		t.Fatalf("eval calls = %#v, want no unconditional delete", client.evals)
	}
}

func TestRedisAdapterReadRecordRevisionUsesVersionKey(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "7", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

	revision, err := adapter.ReadRecordRevision(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("ReadRecordRevision returned error: %v", err)
	}
	if revision != "7" {
		t.Fatalf("revision = %q, want 7", revision)
	}
	if got, want := client.evals[0].keys, []string{"role:{1001}@v"}; !sameStrings(got, want) {
		t.Fatalf("revision keys = %#v, want %#v", got, want)
	}
}

func TestRedisAdapterReadRecordRevisionRejectsMissingOrInvalidRevision(t *testing.T) {
	for _, result := range []string{"", "unexpected"} {
		t.Run(result, func(t *testing.T) {
			client := &recordingRedisStringClient{evalResult: result, hasResult: true}
			adapter := txredis.NewAdapter(client)
			spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

			_, err := adapter.ReadRecordRevision(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
			if err == nil || !strings.Contains(err.Error(), "invalid record revision") {
				t.Fatalf("ReadRecordRevision error = %v, want invalid record revision", err)
			}
		})
	}
}

func TestRedisAdapterLockRecordUsesMainKeyHashTag(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{
			Key:     "arena_record",
			Route:   meta.RouteSpec{By: "RoleId"},
			IDField: "Id",
		},
	}
	key := storage.RecordKey{ID: int64(90001), ShardKey: int64(470001)}

	lock, err := adapter.LockRecord(context.Background(), spec, key, storage.LockOptions{TTL: 10 * time.Second, Wait: time.Millisecond})
	if err != nil {
		t.Fatalf("LockRecord returned error: %v", err)
	}
	if lock == nil {
		t.Fatal("LockRecord returned nil lock")
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want one lock call", client.evals)
	}
	if got, want := client.evals[0].keys, []string{"arena_record:{470001}:90001@lock"}; !sameStrings(got, want) {
		t.Fatalf("lock keys = %#v, want %#v", got, want)
	}
	if len(client.evals[0].args) != 2 || client.evals[0].args[1] != int64(10000) {
		t.Fatalf("lock args = %#v, want token and ttl millis", client.evals[0].args)
	}

	if err := lock.Unlock(context.Background()); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
	if len(client.evals) != 2 {
		t.Fatalf("eval calls = %#v, want lock and unlock", client.evals)
	}
	if got, want := client.evals[1].keys, []string{"arena_record:{470001}:90001@lock"}; !sameStrings(got, want) {
		t.Fatalf("unlock keys = %#v, want %#v", got, want)
	}
}

func TestRedisAdapterLockRecordRejectsUnknownScriptResult(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "unexpected", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

	_, err := adapter.LockRecord(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, storage.LockOptions{TTL: time.Second})
	if err == nil {
		t.Fatal("LockRecord error = nil, want unknown script result error")
	}
	if !strings.Contains(err.Error(), "unexpected script result") || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("LockRecord error = %v, want unknown result details", err)
	}
}

func TestRedisRecordLockUnlockRejectsUnknownScriptResult(t *testing.T) {
	client := &recordingRedisStringClient{evalResults: []string{"ok", "unexpected"}}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}

	lock, err := adapter.LockRecord(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, storage.LockOptions{TTL: time.Second})
	if err != nil {
		t.Fatalf("LockRecord returned error: %v", err)
	}
	err = lock.Unlock(context.Background())
	if err == nil {
		t.Fatal("Unlock error = nil, want unknown script result error")
	}
	if !strings.Contains(err.Error(), "unexpected script result") || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("Unlock error = %v, want unknown result details", err)
	}
}

func TestRedisAdapterAppliesStringDocumentsWithRecordID(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	const redisFieldHeadImg change.FieldID = 4
	const redisFieldQuality change.FieldID = 5
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet},
		{Path: deltaPath(fieldNode(redisFieldHeadImg)), Op: change.OpClear},
		{Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)), Op: change.OpSet},
		{Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(redisFieldQuality)), Op: change.OpClear},
	}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo: {
			deltaFieldName:    "Name",
			redisFieldHeadImg: "HeadImg",
			deltaFieldItems:   "Items",
			deltaFieldQty:     "Quantity",
			redisFieldQuality: "Quality",
		},
		change.FieldNameJSON: {
			deltaFieldName:    "name",
			redisFieldHeadImg: "headImg",
			deltaFieldItems:   "items",
			deltaFieldQty:     "quantity",
			redisFieldQuality: "quality",
		},
	})
	values := testDeltaValues(map[string]any{
		"Name":                "alice",
		"Items.1001.Quantity": int64(3),
	})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}

	patches := evalPatchDocuments(t, client)
	mainDoc := patches[""]
	if mainDoc["name"] != "alice" {
		t.Fatalf("main doc = %#v", mainDoc)
	}
	if headImg, ok := mainDoc["headImg"].(map[string]any); !ok || len(headImg) != 0 {
		t.Fatalf("main doc headImg = %#v, want empty document marker in %#v", mainDoc["headImg"], mainDoc)
	}

	itemsDoc := patches["items"]
	items := itemsDoc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != float64(3) {
		t.Fatalf("items doc = %#v", itemsDoc)
	}
	if quality, ok := item["quality"].(map[string]any); !ok || len(quality) != 0 {
		t.Fatalf("items doc quality = %#v, want empty document marker in %#v", item["quality"], itemsDoc)
	}
}

func TestRedisAdapterAppliesFieldIDChangesWithResolver(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	deltas := change.DeltaSet{{Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)), Op: change.OpSet}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   deltaTestGoNames(),
		change.FieldNameJSON: deltaTestJSONNames(),
	})
	values := testDeltaValues(map[string]any{"Items.1001.Quantity": int64(9)})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	itemsDoc := evalPatchDocuments(t, client)["items"]
	items := itemsDoc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != float64(9) {
		t.Fatalf("items doc = %#v", itemsDoc)
	}
}

func TestRedisAdapterUsesPathReplaceModeForCollectionElement(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	deltas := change.DeltaSet{{
		Path: deltaPath(intKeyNode(deltaFieldItems, 1001)),
		Op:   change.OpReplace,
	}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   deltaTestGoNames(),
		change.FieldNameJSON: deltaTestJSONNames(),
	})
	values := testDeltaValues(map[string]any{
		"Items.1001": map[string]any{"itemId": int64(1001), "quantity": int64(3)},
	})

	if err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true}); err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if args[2] != "3" {
		t.Fatalf("mode = %#v, want path replace mode 3; args=%#v", args[2], args)
	}
	operations := decodeJSONDocument(t, args[1].(string))["operations"].([]any)
	if len(operations) != 1 {
		t.Fatalf("operations = %#v, want one replace operation", operations)
	}
	op := operations[0].(map[string]any)
	if op["kind"] != "replace" {
		t.Fatalf("operation kind = %#v, want replace", op["kind"])
	}
	if got := op["path"]; !reflect.DeepEqual(got, []any{"items", "1001"}) {
		t.Fatalf("operation path = %#v, want [items 1001]", got)
	}
}

func TestRedisAdapterUsesFullUpdateModeForWholeSubKeyReplace(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items", FullUpdate: true}},
		},
	}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldItems)), Op: change.OpReplace}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   deltaTestGoNames(),
		change.FieldNameJSON: deltaTestJSONNames(),
	})
	values := testDeltaValues(map[string]any{"Items": map[string]any{"1001": map[string]any{"quantity": int64(3)}}})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if len(args) != 5 {
		t.Fatalf("eval args = %#v, want one suffix/document/mode triple plus tail", args)
	}
	if args[0] != "items" {
		t.Fatalf("suffix = %#v, want items; args=%#v", args[0], args)
	}
	if args[2] != "1" {
		t.Fatalf("mode = %#v, want 1; args=%#v", args[2], args)
	}
	doc := decodeJSONDocument(t, args[1].(string))
	items := doc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != float64(3) {
		t.Fatalf("full update doc = %#v", doc)
	}
}

func TestRedisAdapterUsesPartialFullUpdateModeWhenSubKeyHasFullAndDiffChanges(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "bag", FullUpdate: true}},
			"Equip": {Redis: &meta.RedisFieldSpec{SubKey: "bag"}},
		},
	}
	const redisFieldEquip change.FieldID = 6
	const redisFieldWeapon change.FieldID = 7
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldItems)), Op: change.OpSet},
		{Path: deltaPath(fieldNode(redisFieldEquip), fieldNode(redisFieldWeapon)), Op: change.OpSet},
	}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo: {
			deltaFieldItems:  "Items",
			redisFieldEquip:  "Equip",
			redisFieldWeapon: "Weapon",
		},
		change.FieldNameJSON: {
			deltaFieldItems:  "items",
			redisFieldEquip:  "equip",
			redisFieldWeapon: "weapon",
		},
	})
	values := change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		switch {
		case path.Equal(deltaPath(fieldNode(deltaFieldItems))):
			return map[string]any{"1001": map[string]any{"quantity": int64(3)}}, true
		case path.Equal(deltaPath(fieldNode(redisFieldEquip), fieldNode(redisFieldWeapon))):
			return "sword", true
		default:
			return nil, false
		}
	})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if len(args) != 5 {
		t.Fatalf("eval args = %#v, want one suffix/document/mode triple plus tail", args)
	}
	if args[0] != "bag" {
		t.Fatalf("suffix = %#v, want bag; args=%#v", args[0], args)
	}
	if args[2] != "3" {
		t.Fatalf("mode = %#v, want 3; args=%#v", args[2], args)
	}
	doc := evalPatchDocuments(t, client)["bag"]
	if doc["equip"].(map[string]any)["weapon"] != "sword" {
		t.Fatalf("operation update doc = %#v", doc)
	}
}

func TestRedisAdapterLoadsStringDocumentWithSubKeys(t *testing.T) {
	client := &recordingRedisStringClient{
		evalResult: `{"roleId":1001,"name":"alice","items":{"1001":{"quantity":3}}}`,
		hasResult:  true,
	}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	doc, exists, err := adapter.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists {
		t.Fatal("LoadDocument exists = false, want true")
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	if got, want := client.evals[0].keys, []string{"role:{1001}", "role:{1001}:items"}; !sameStrings(got, want) {
		t.Fatalf("eval keys = %#v, want %#v", got, want)
	}
	if doc["name"] != "alice" {
		t.Fatalf("loaded doc = %#v", doc)
	}
	items := doc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != json.Number("3") {
		t.Fatalf("loaded items = %#v", items)
	}
}

func TestRedisAdapterLoadDocumentReadsSubKeyOnly(t *testing.T) {
	client := &recordingRedisStringClient{
		evalResult: `{"items":{"1001":{"quantity":3}}}`,
		hasResult:  true,
	}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	doc, exists, err := adapter.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "items")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists {
		t.Fatal("LoadDocument exists = false, want true")
	}
	if got, want := client.evals[0].keys, []string{"role:{1001}:items"}; !sameStrings(got, want) {
		t.Fatalf("eval keys = %#v, want %#v", got, want)
	}
	items := doc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != json.Number("3") {
		t.Fatalf("loaded items = %#v", items)
	}
}

func TestRedisAdapterLoadDocumentMissing(t *testing.T) {
	client := &recordingRedisStringClient{hasResult: true}
	adapter := txredis.NewAdapter(client)

	doc, exists, err := adapter.LoadDocument(context.Background(), meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
	}, storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if exists {
		t.Fatalf("LoadDocument exists = true, doc = %#v", doc)
	}
	if doc != nil {
		t.Fatalf("LoadDocument doc = %#v, want nil", doc)
	}
}

func TestRedisAdapterLoadDocumentRejectsNonObjectOrTrailingJSON(t *testing.T) {
	for _, body := range []string{`null`, `[]`, `{"name":"alice"}{"name":"bob"}`} {
		t.Run(body, func(t *testing.T) {
			client := &recordingRedisStringClient{evalResult: body, hasResult: true}
			adapter := txredis.NewAdapter(client)

			_, exists, err := adapter.LoadDocument(context.Background(), meta.TypeSpec{
				Redis: &meta.RedisSpec{Key: "role"},
			}, storage.RecordKey{ID: int64(1001)})
			if err == nil || exists {
				t.Fatalf("LoadDocument exists/error = %v/%v, want false/non-nil", exists, err)
			}
		})
	}
}

func TestRedisAdapterLoadDocumentsReadsSubKeyOnly(t *testing.T) {
	client := &recordingRedisStringClient{
		mgetValues: []string{
			`{"items":{"1001":{"quantity":3}}}`,
			"",
		},
	}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	docs, exists, err := adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{
		{ID: int64(1001)},
		{ID: int64(1002)},
	}, "items")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if got, want := client.mgets[0], []string{"role:{1001}:items", "role:{1002}:items"}; !sameStrings(got, want) {
		t.Fatalf("mget keys = %#v, want %#v", got, want)
	}
	if got, want := exists, []bool{true, false}; !sameBools(got, want) {
		t.Fatalf("exists = %#v, want %#v", got, want)
	}
	items := docs[0]["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != json.Number("3") {
		t.Fatalf("loaded items = %#v", items)
	}
}

func TestRedisAdapterLoadDocumentsRejectsMalformedDocumentPart(t *testing.T) {
	client := &recordingRedisStringClient{
		mgetValues: []string{
			`{"roleId":1001,"name":"alice"}`,
			`null`,
		},
	}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	_, _, err := adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{{ID: int64(1001)}})
	if err == nil || !strings.Contains(err.Error(), "expected JSON object") {
		t.Fatalf("LoadDocuments error = %v, want malformed document part error", err)
	}
}

func TestRedisAdapterLoadsStringDocumentsWithSubKeys(t *testing.T) {
	client := &recordingRedisStringClient{
		mgetValues: []string{
			`{"roleId":1001,"name":"alice"}`,
			`{"items":{"1001":{"quantity":3}}}`,
			"",
			"",
		},
	}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	docs, exists, err := adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{
		{ID: int64(1001)},
		{ID: int64(1002)},
	})
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if len(client.mgets) != 1 {
		t.Fatalf("mget calls = %#v, want exactly one", client.mgets)
	}
	if got, want := client.mgets[0], []string{"role:{1001}", "role:{1001}:items", "role:{1002}", "role:{1002}:items"}; !sameStrings(got, want) {
		t.Fatalf("mget keys = %#v, want %#v", got, want)
	}
	if got, want := exists, []bool{true, false}; !sameBools(got, want) {
		t.Fatalf("exists = %#v, want %#v", got, want)
	}
	if docs[1] != nil {
		t.Fatalf("docs[1] = %#v, want nil", docs[1])
	}
	if docs[0]["name"] != "alice" {
		t.Fatalf("docs[0] = %#v", docs[0])
	}
	items := docs[0]["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != json.Number("3") {
		t.Fatalf("loaded items = %#v", items)
	}
}

func TestRedisAdapterLoadDocumentsFallsBackToEval(t *testing.T) {
	client := &recordingRedisEvalOnlyClient{
		results: []string{
			`{"roleId":1001,"name":"alice","items":{"1001":{"quantity":3}}}`,
			"",
		},
	}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	docs, exists, err := adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{
		{ID: int64(1001)},
		{ID: int64(1002)},
	})
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if len(client.evals) != 2 {
		t.Fatalf("eval calls = %#v, want two calls", client.evals)
	}
	if got, want := client.evals[0].keys, []string{"role:{1001}", "role:{1001}:items"}; !sameStrings(got, want) {
		t.Fatalf("first eval keys = %#v, want %#v", got, want)
	}
	if got, want := client.evals[1].keys, []string{"role:{1002}", "role:{1002}:items"}; !sameStrings(got, want) {
		t.Fatalf("second eval keys = %#v, want %#v", got, want)
	}
	if got, want := exists, []bool{true, false}; !sameBools(got, want) {
		t.Fatalf("exists = %#v, want %#v", got, want)
	}
	if docs[1] != nil {
		t.Fatalf("docs[1] = %#v, want nil", docs[1])
	}
	if docs[0]["name"] != "alice" {
		t.Fatalf("docs[0] = %#v", docs[0])
	}
	items := docs[0]["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != json.Number("3") {
		t.Fatalf("loaded items = %#v", items)
	}
}

func TestRedisAdapterLoadDocumentsWithOptionsChunksMGet(t *testing.T) {
	client := &recordingRedisStringClient{
		mgetBatches: [][]string{
			{
				`{"roleId":1001,"name":"alice"}`,
				`{"items":{"1001":{"quantity":3}}}`,
				`{"roleId":1002,"name":"bob"}`,
				`{"items":{"1001":{"quantity":4}}}`,
			},
			{
				`{"roleId":1003,"name":"carl"}`,
				`{"items":{"1001":{"quantity":5}}}`,
			},
		},
	}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}

	docs, exists, err := adapter.LoadDocumentsWithOptions(context.Background(), spec, []storage.RecordKey{
		{ID: int64(1001)},
		{ID: int64(1002)},
		{ID: int64(1003)},
	}, txredis.LoadOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("LoadDocumentsWithOptions returned error: %v", err)
	}
	if len(client.mgets) != 2 {
		t.Fatalf("mget calls = %#v, want two calls", client.mgets)
	}
	if got, want := client.mgets[0], []string{"role:{1001}", "role:{1001}:items", "role:{1002}", "role:{1002}:items"}; !sameStrings(got, want) {
		t.Fatalf("first mget keys = %#v, want %#v", got, want)
	}
	if got, want := client.mgets[1], []string{"role:{1003}", "role:{1003}:items"}; !sameStrings(got, want) {
		t.Fatalf("second mget keys = %#v, want %#v", got, want)
	}
	if got, want := exists, []bool{true, true, true}; !sameBools(got, want) {
		t.Fatalf("exists = %#v, want %#v", got, want)
	}
	if docs[2]["name"] != "carl" {
		t.Fatalf("docs[2] = %#v", docs[2])
	}
	items := docs[2]["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != json.Number("5") {
		t.Fatalf("docs[2] items = %#v", items)
	}
}

func TestRedisClusterRoutesSingleWriteByShardKey(t *testing.T) {
	roleShard := &recordingRedisStringClient{}
	userShard := &recordingRedisStringClient{}
	router := &recordingRedisRouter{names: map[any]string{
		int64(7): "role-shard",
	}}
	cluster := txredis.NewCluster(router, map[string]txredis.ScriptClient{
		"role-shard": roleShard,
		"user-shard": userShard,
	})
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{
		Key:     "role",
		IDField: "roleId",
		Route:   meta.RouteSpec{Type: "role", By: "serverId"},
	}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := cluster.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001), ShardKey: int64(7)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if len(router.calls) != 1 || router.calls[0].routeType != "role" || router.calls[0].shardKey != int64(7) {
		t.Fatalf("router calls = %#v, want role shard key 7", router.calls)
	}
	if len(roleShard.evals) != 1 || !sameStrings(roleShard.evals[0].keys, []string{"role:{7}:1001"}) {
		t.Fatalf("role shard evals = %#v, want one write for role:{7}:1001", roleShard.evals)
	}
	if len(userShard.evals) != 0 {
		t.Fatalf("unexpected user shard evals = %#v", userShard.evals)
	}
}

func TestRedisAdapterRejectsMissingDistinctRouteShardKey(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{
		Key:     "role",
		IDField: "roleId",
		Route:   meta.RouteSpec{Type: "role", By: "serverId"},
	}}

	err := adapter.ReplaceDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, map[string]any{"roleId": int64(1001)})
	if !errors.Is(err, storage.ErrInvalidRecordKey) {
		t.Fatalf("ReplaceDocument error = %v, want ErrInvalidRecordKey", err)
	}
	if len(client.evals) != 0 {
		t.Fatalf("redis evals = %#v, want no I/O", client.evals)
	}
}

func TestRedisClusterSkipsEmptyDeltasWithoutRouting(t *testing.T) {
	cluster := txredis.NewCluster(nil, nil)

	err := cluster.ApplyDeltasWithOptions(
		context.Background(),
		meta.TypeSpec{},
		storage.RecordKey{},
		nil,
		nil,
		nil,
		storage.ApplyOptions{},
	)
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error for empty deltas: %v", err)
	}
}

func TestRedisClusterRejectsTypedNilRouter(t *testing.T) {
	var router *recordingRedisRouter
	cluster := txredis.NewCluster(router, map[string]txredis.ScriptClient{"role": &recordingRedisStringClient{}})
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role", Route: meta.RouteSpec{Type: "role"}}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := cluster.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameJSON, deltaTestJSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if err == nil || !strings.Contains(err.Error(), "router is not configured") {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want router configuration error", err)
	}
}

func TestRedisClusterRoutesRecordLockByShardKey(t *testing.T) {
	roleShard := &recordingRedisStringClient{}
	userShard := &recordingRedisStringClient{}
	router := &recordingRedisRouter{names: map[any]string{
		int64(7): "role-shard",
	}}
	cluster := txredis.NewCluster(router, map[string]txredis.ScriptClient{
		"role-shard": roleShard,
		"user-shard": userShard,
	})
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{
		Key:     "role",
		IDField: "roleId",
		Route:   meta.RouteSpec{Type: "role", By: "serverId"},
	}}

	lock, err := cluster.LockRecord(context.Background(), spec, storage.RecordKey{ID: int64(1001), ShardKey: int64(7)}, storage.LockOptions{TTL: time.Second, Wait: time.Millisecond})
	if err != nil {
		t.Fatalf("LockRecord returned error: %v", err)
	}
	if lock == nil {
		t.Fatal("LockRecord returned nil lock")
	}
	if len(router.calls) != 1 || router.calls[0].routeType != "role" || router.calls[0].shardKey != int64(7) {
		t.Fatalf("router calls = %#v, want role shard key 7", router.calls)
	}
	if len(roleShard.evals) != 1 || !sameStrings(roleShard.evals[0].keys, []string{"role:{7}:1001@lock"}) {
		t.Fatalf("role shard evals = %#v, want one lock for role:{7}:1001@lock", roleShard.evals)
	}
	if len(userShard.evals) != 0 {
		t.Fatalf("unexpected user shard evals = %#v", userShard.evals)
	}
}

func TestRedisClusterRoutesRecordRevisionByShardKey(t *testing.T) {
	roleShard := &recordingRedisStringClient{evalResult: "9", hasResult: true}
	userShard := &recordingRedisStringClient{}
	router := &recordingRedisRouter{names: map[any]string{
		int64(7): "role-shard",
	}}
	cluster := txredis.NewCluster(router, map[string]txredis.ScriptClient{
		"role-shard": roleShard,
		"user-shard": userShard,
	})
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{
		Key:     "role",
		IDField: "roleId",
		Route:   meta.RouteSpec{Type: "role", By: "serverId"},
	}}

	revision, err := cluster.ReadRecordRevision(context.Background(), spec, storage.RecordKey{ID: int64(1001), ShardKey: int64(7)})
	if err != nil {
		t.Fatalf("ReadRecordRevision returned error: %v", err)
	}
	if revision != "9" {
		t.Fatalf("revision = %q, want 9", revision)
	}
	if len(roleShard.evals) != 1 || !sameStrings(roleShard.evals[0].keys, []string{"role:{7}:1001@v"}) {
		t.Fatalf("role shard evals = %#v, want one read for role:{7}:1001@v", roleShard.evals)
	}
	if len(userShard.evals) != 0 {
		t.Fatalf("unexpected user shard evals = %#v", userShard.evals)
	}
}

func TestRedisClusterBatchLoadsByShardGroupAndRestoresOrder(t *testing.T) {
	shardA := &recordingRedisStringClient{
		mgetValues: []string{
			`{"roleId":1001,"name":"alice"}`,
			`{"items":{"1001":{"quantity":3}}}`,
			`{"roleId":1003,"name":"carol"}`,
			`{"items":{"1001":{"quantity":5}}}`,
		},
	}
	shardB := &recordingRedisStringClient{
		mgetValues: []string{"", ""},
	}
	router := &recordingRedisRouter{names: map[any]string{
		int64(7): "a",
		int64(8): "b",
	}}
	cluster := txredis.NewCluster(router, map[string]txredis.ScriptClient{
		"a": shardA,
		"b": shardB,
	})
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{
			Key:     "role",
			IDField: "roleId",
			Route:   meta.RouteSpec{Type: "role", By: "serverId"},
		},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	keys := []storage.RecordKey{
		{ID: int64(1001), ShardKey: int64(7)},
		{ID: int64(1002), ShardKey: int64(8)},
		{ID: int64(1003), ShardKey: int64(7)},
	}

	docs, exists, err := cluster.LoadDocuments(context.Background(), spec, keys)
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if len(docs) != 3 || len(exists) != 3 {
		t.Fatalf("LoadDocuments lengths docs=%d exists=%d, want 3", len(docs), len(exists))
	}
	if !exists[0] || docs[0]["name"] != "alice" || exists[1] || docs[1] != nil || !exists[2] || docs[2]["name"] != "carol" {
		t.Fatalf("docs=%#v exists=%#v, want original order", docs, exists)
	}
	if len(shardA.mgets) != 1 || !sameStrings(shardA.mgets[0], []string{"role:{7}:1001", "role:{7}:1001:items", "role:{7}:1003", "role:{7}:1003:items"}) {
		t.Fatalf("shard A mgets = %#v, want one endpoint-level MGET", shardA.mgets)
	}
	if len(shardB.mgets) != 1 || !sameStrings(shardB.mgets[0], []string{"role:{8}:1002", "role:{8}:1002:items"}) {
		t.Fatalf("shard B mgets = %#v, want one endpoint-level MGET", shardB.mgets)
	}
}

func TestRedisClusterBatchLoadClusterSafeGroupsByHashTag(t *testing.T) {
	shard := &recordingRedisStringClient{
		values: map[string]string{
			"role:{7}:1001":       `{"roleId":1001,"name":"alice"}`,
			"role:{7}:1001:items": `{"items":{"1001":{"quantity":3}}}`,
			"role:{8}:1002":       `{"roleId":1002,"name":"bob"}`,
			"role:{8}:1002:items": `{"items":{"1001":{"quantity":4}}}`,
			"role:{7}:1003":       `{"roleId":1003,"name":"carol"}`,
			"role:{7}:1003:items": `{"items":{"1001":{"quantity":5}}}`,
		},
	}
	router := &recordingRedisRouter{names: map[any]string{
		int64(7): "role",
		int64(8): "role",
	}}
	cluster := txredis.NewCluster(router, map[string]txredis.ScriptClient{"role": shard})
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{
			Key:     "role",
			IDField: "roleId",
			Route:   meta.RouteSpec{Type: "role", By: "serverId"},
		},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	keys := []storage.RecordKey{
		{ID: int64(1001), ShardKey: int64(7)},
		{ID: int64(1002), ShardKey: int64(8)},
		{ID: int64(1003), ShardKey: int64(7)},
	}

	docs, exists, err := cluster.LoadDocumentsWithOptions(context.Background(), spec, keys, txredis.LoadOptions{ClusterSafe: true})
	if err != nil {
		t.Fatalf("LoadDocumentsWithOptions returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "alice" || !exists[1] || docs[1]["name"] != "bob" || !exists[2] || docs[2]["name"] != "carol" {
		t.Fatalf("docs=%#v exists=%#v, want original order", docs, exists)
	}
	if !hasMGetCall(shard.mgets, []string{"role:{7}:1001", "role:{7}:1001:items", "role:{7}:1003", "role:{7}:1003:items"}) {
		t.Fatalf("mgets = %#v, want hash tag 7 grouped call", shard.mgets)
	}
	if !hasMGetCall(shard.mgets, []string{"role:{8}:1002", "role:{8}:1002:items"}) {
		t.Fatalf("mgets = %#v, want hash tag 8 grouped call", shard.mgets)
	}
}

func evalPatchDocuments(t *testing.T, client *recordingRedisStringClient) map[string]map[string]any {
	t.Helper()
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if len(args) < 2 || (len(args)-2)%3 != 0 {
		t.Fatalf("eval args = %#v, want suffix/document/mode triples plus tail", args)
	}
	patches := map[string]map[string]any{}
	for i := 0; i < len(args)-2; i += 3 {
		suffix, ok := args[i].(string)
		if !ok {
			t.Fatalf("eval arg %d = %#v, want suffix string", i, args[i])
		}
		body, ok := args[i+1].(string)
		if !ok {
			t.Fatalf("eval arg %d = %#v, want document string", i+1, args[i+1])
		}
		mode, ok := args[i+2].(string)
		if !ok {
			t.Fatalf("eval arg %d = %#v, want mode string", i+2, args[i+2])
		}
		switch mode {
		case "0":
			patches[suffix] = decodeJSONDocument(t, body)
		case "3":
			patches[suffix] = applyOperationPatch(t, body)
		default:
			t.Fatalf("eval arg %d = %#v, want merge or operation mode", i+2, args[i+2])
		}
	}
	return patches
}

func applyOperationPatch(t *testing.T, body string) map[string]any {
	t.Helper()
	patch := decodeJSONDocument(t, body)
	operations, ok := patch["operations"].([]any)
	if !ok {
		t.Fatalf("operation patch = %#v, want operations", patch)
	}
	doc := map[string]any{}
	for _, rawOperation := range operations {
		operation, ok := rawOperation.(map[string]any)
		if !ok {
			t.Fatalf("operation = %#v, want object", rawOperation)
		}
		path, ok := operation["path"].([]any)
		if !ok || len(path) == 0 {
			t.Fatalf("operation path = %#v, want non-empty path", operation["path"])
		}
		target := doc
		for _, rawSegment := range path[:len(path)-1] {
			segment, ok := rawSegment.(string)
			if !ok {
				t.Fatalf("operation path segment = %#v, want string", rawSegment)
			}
			child, ok := target[segment].(map[string]any)
			if !ok {
				child = map[string]any{}
				target[segment] = child
			}
			target = child
		}
		last, ok := path[len(path)-1].(string)
		if !ok {
			t.Fatalf("operation path final segment = %#v, want string", path[len(path)-1])
		}
		if operation["kind"] == "delete" {
			delete(target, last)
			continue
		}
		target[last] = operation["value"]
	}
	return doc
}

type redisEvalTriple struct {
	body string
	mode string
}

func evalTriples(t *testing.T, args []any) map[string]redisEvalTriple {
	t.Helper()
	if len(args) < 2 || (len(args)-2)%3 != 0 {
		t.Fatalf("eval args = %#v, want suffix/document/mode triples plus tail", args)
	}
	triples := map[string]redisEvalTriple{}
	for i := 0; i < len(args)-2; i += 3 {
		suffix, ok := args[i].(string)
		if !ok {
			t.Fatalf("eval arg %d = %#v, want suffix string", i, args[i])
		}
		body, ok := args[i+1].(string)
		if !ok {
			t.Fatalf("eval arg %d = %#v, want document string", i+1, args[i+1])
		}
		mode, ok := args[i+2].(string)
		if !ok {
			t.Fatalf("eval arg %d = %#v, want mode string", i+2, args[i+2])
		}
		triples[suffix] = redisEvalTriple{body: body, mode: mode}
	}
	return triples
}

func decodeJSONDocument(t *testing.T, body string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("json.Unmarshal(%q) returned error: %v", body, err)
	}
	return normalizeJSONNull(doc).(map[string]any)
}

func normalizeJSONNull(value any) any {
	switch typed := value.(type) {
	case nil:
		return map[string]any{"__json_null__": true}
	case map[string]any:
		for key, child := range typed {
			typed[key] = normalizeJSONNull(child)
		}
		return typed
	case []any:
		for i, child := range typed {
			typed[i] = normalizeJSONNull(child)
		}
		return typed
	default:
		return value
	}
}

func sameStrings(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sameBools(got []bool, want []bool) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func hasMGetCall(calls [][]string, want []string) bool {
	for _, call := range calls {
		if sameStrings(call, want) {
			return true
		}
	}
	return false
}
