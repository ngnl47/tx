package tests

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
)

type recordingRedisHashClient struct {
	sets    map[string]map[string]any
	deletes map[string][]string
}

func (c *recordingRedisHashClient) HSet(_ context.Context, key string, fields map[string]any) error {
	if c.sets == nil {
		c.sets = map[string]map[string]any{}
	}
	c.sets[key] = fields
	return nil
}

func (c *recordingRedisHashClient) HDel(_ context.Context, key string, fields []string) error {
	if c.deletes == nil {
		c.deletes = map[string][]string{}
	}
	c.deletes[key] = fields
	return nil
}

func TestRedisAdapterAppliesHashChangesWithRecordID(t *testing.T) {
	client := &recordingRedisHashClient{}
	adapter := txredis.NewAdapter(client)
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1"}, Kind: change.KindUnset})

	err := adapter.ApplyChanges(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set)
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	if client.sets["role:1001:name"]["name"] != "alice" {
		t.Fatalf("sets = %#v", client.sets)
	}
	if len(client.deletes["role:1001:items"]) != 1 || client.deletes["role:1001:items"][0] != "1" {
		t.Fatalf("deletes = %#v", client.deletes)
	}
}

func TestRedisAdapterAppliesFieldIDChangesWithResolver(t *testing.T) {
	client := &recordingRedisHashClient{}
	adapter := txredis.NewAdapterWithResolver(client, testFieldResolver{3: "items"})
	spec := meta.TypeSpec{
		Redis: &meta.RedisSpec{Key: "role"},
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
	if client.sets["role:1001:items"]["1001.quantity"] != int64(9) {
		t.Fatalf("sets = %#v", client.sets)
	}
}
