package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
)

type recordingRedisStringClient struct {
	values map[string]string
	evals  []redisEvalCall
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
	return "ok", nil
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
