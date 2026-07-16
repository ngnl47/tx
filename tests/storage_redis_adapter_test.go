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

	client.values = map[string]string{
		"role:1001":       `{"name":"old","headImg":"old"}`,
		"role:1001:items": `{"items":{"1001":{"quantity":1,"quality":2}}}`,
	}

	err := adapter.ApplyChanges(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set)
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}

	mainDoc := decodeJSONDocument(t, client.values["role:1001"])
	if mainDoc["name"] != "alice" {
		t.Fatalf("main doc = %#v", mainDoc)
	}
	if _, exists := mainDoc["headImg"]; exists {
		t.Fatalf("main doc still contains headImg: %#v", mainDoc)
	}

	itemsDoc := decodeJSONDocument(t, client.values["role:1001:items"])
	items := itemsDoc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != float64(3) {
		t.Fatalf("items doc = %#v", itemsDoc)
	}
	if _, exists := item["quality"]; exists {
		t.Fatalf("items doc still contains quality: %#v", itemsDoc)
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
	itemsDoc := decodeJSONDocument(t, client.values["role:1001:items"])
	items := itemsDoc["items"].(map[string]any)
	item := items["1001"].(map[string]any)
	if item["quantity"] != float64(9) {
		t.Fatalf("items doc = %#v", itemsDoc)
	}
}

func decodeJSONDocument(t *testing.T, body string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("json.Unmarshal(%q) returned error: %v", body, err)
	}
	return doc
}
