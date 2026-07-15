package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/mongo"
	"github.com/ngnl47/tx/storage/redis"
)

func TestChangeSetToDocumentWithResolverUsesFieldID(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(2),
		Kind:  change.KindSet,
		Value: "alice",
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	doc := storage.ChangeSetToDocumentWithResolver(set, testFieldResolver{2: "name"})
	if doc["name"] != "alice" {
		t.Fatalf("doc = %#v, want name=alice", doc)
	}
}

func TestRedisHashChangesWithResolverUsesFieldIDAndTailPath(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	hashes := redis.BuildHashChangesWithResolver(set, testFieldResolver{3: "items"})
	if hashes["items"]["1001.quantity"] != int64(9) {
		t.Fatalf("hashes = %#v, want items[1001.quantity]=9", hashes)
	}
}

func TestRedisHashDeletesWithResolverUsesFieldIDAndTailPath(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001"},
		Kind:  change.KindUnset,
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	deletes := redis.BuildHashDeletesWithResolver(set, testFieldResolver{3: "items"})
	if len(deletes["items"]) != 1 || deletes["items"][0] != "1001" {
		t.Fatalf("deletes = %#v, want items[1001]", deletes)
	}
}

func TestMongoUpdateDocumentWithResolverUsesFieldIDAndTailPath(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	update := mongo.BuildUpdateDocumentWithResolver(set, testFieldResolver{3: "items"})
	setDoc := update["$set"].(map[string]any)
	if setDoc["items.1001.quantity"] != int64(9) {
		t.Fatalf("update = %#v, want $set items.1001.quantity=9", update)
	}
}
