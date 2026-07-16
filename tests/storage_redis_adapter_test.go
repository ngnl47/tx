package tests

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
)

type recordingRedisStringClient struct {
	values     map[string]string
	evals      []redisEvalCall
	mgets      [][]string
	mgetValues []string
	evalResult string
	hasResult  bool
}

type redisEvalCall struct {
	script string
	keys   []string
	args   []any
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
	if c.hasResult {
		return c.evalResult, nil
	}
	return "ok", nil
}

func (c *recordingRedisStringClient) MGet(_ context.Context, keys ...string) ([]string, error) {
	c.mgets = append(c.mgets, append([]string(nil), keys...))
	return append([]string(nil), c.mgetValues...), nil
}

func TestRedisAdapterAppliesChangesWithSingleLuaEval(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapterWithResolver(client, testFieldResolver{3: "items"})
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})
	_ = set.Add(change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	})

	err := adapter.ApplyChanges(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set)
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	call := client.evals[0]
	if len(call.keys) != 1 || call.keys[0] != "role:1001" {
		t.Fatalf("eval keys = %#v, want role:1001", call.keys)
	}
	if len(call.args) != 8 {
		t.Fatalf("eval args = %#v, want two suffix/document/mode triples plus upsert and ttlMode", call.args)
	}
	if call.args[len(call.args)-2] != "1" || call.args[len(call.args)-1] != "0" {
		t.Fatalf("eval tail args = %#v, want upsert=1 ttlMode=0", call.args[len(call.args)-2:])
	}
}

func TestRedisAdapterApplyChangesWithOptionsDisablesUpsert(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, txredis.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if args[len(args)-2] != "0" {
		t.Fatalf("upsert arg = %#v, want 0; args=%#v", args[len(args)-2], args)
	}
}

func TestRedisAdapterApplyChangesWithOptionsPassesTTL(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, txredis.ApplyOptions{
		Upsert: true,
		TTL:    120,
	})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(client.evals) != 1 {
		t.Fatalf("eval calls = %#v, want exactly one", client.evals)
	}
	args := client.evals[0].args
	if args[len(args)-1] != "120" {
		t.Fatalf("ttl arg = %#v, want 120; args=%#v", args[len(args)-1], args)
	}
}

func TestRedisAdapterApplyChangesWithOptionsReturnsNotExists(t *testing.T) {
	client := &recordingRedisStringClient{evalResult: "ne", hasResult: true}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{Redis: &meta.RedisSpec{Key: "role"}}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, txredis.ApplyOptions{})
	if !errors.Is(err, storage.ErrNotExists) {
		t.Fatalf("ApplyChangesWithOptions err = %v, want ErrNotExists", err)
	}
}

func TestRedisAdapterAppliesStringDocumentsWithRecordID(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})
	_ = set.Add(change.FieldChange{Path: change.Path{"headImg"}, Kind: change.KindUnset})
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1001", "quantity"}, Kind: change.KindSet, Value: int64(3)})
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1001", "quality"}, Kind: change.KindUnset})

	err := adapter.ApplyChanges(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set)
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}

	patches := evalPatchDocuments(t, client)
	mainDoc := patches[""]
	if mainDoc["name"] != "alice" {
		t.Fatalf("main doc = %#v", mainDoc)
	}
	headImg := mainDoc["headImg"].(map[string]any)
	if headImg["__json_null__"] != true {
		t.Fatalf("main doc headImg marker = %#v", mainDoc)
	}

	itemsDoc := patches["items"]
	items := itemsDoc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != float64(3) {
		t.Fatalf("items doc = %#v", itemsDoc)
	}
	quality := item["quality"].(map[string]any)
	if quality["__json_null__"] != true {
		t.Fatalf("items doc quality marker = %#v", itemsDoc)
	}
}

func TestRedisAdapterAppliesFieldIDChangesWithResolver(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapterWithResolver(client, testFieldResolver{3: "items"})
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	})

	err := adapter.ApplyChanges(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set)
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	itemsDoc := evalPatchDocuments(t, client)["items"]
	items := itemsDoc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != float64(9) {
		t.Fatalf("items doc = %#v", itemsDoc)
	}
}

func TestRedisAdapterUsesFullUpdateModeForWholeSubKeyReplace(t *testing.T) {
	client := &recordingRedisStringClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items", FullUpdate: true}},
		},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{
		Path:  change.Path{"items"},
		Kind:  change.KindReplace,
		Value: map[string]any{"1001": map[string]any{"quantity": int64(3)}},
	})

	err := adapter.ApplyChanges(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set)
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
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
	if got, want := client.evals[0].keys, []string{"role:1001", "role:1001", "role:1001:items"}; !sameStrings(got, want) {
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
	if got, want := client.mgets[0], []string{"role:1001", "role:1001:items", "role:1002", "role:1002:items"}; !sameStrings(got, want) {
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
		if args[i+2] != "0" {
			t.Fatalf("eval arg %d = %#v, want mode 0", i+2, args[i+2])
		}
		patches[suffix] = decodeJSONDocument(t, body)
	}
	return patches
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
