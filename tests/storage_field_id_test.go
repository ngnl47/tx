package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
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

func TestRedisDocumentPatchesWithResolverUsesFieldIDAndTailPath(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	patches := redis.BuildDocumentPatchesWithResolver(redisFieldIDSpec(), set, testFieldResolver{3: "items"})
	if patches["items"].Set[0].Path.String() != "items.1001.quantity" || patches["items"].Set[0].Value != int64(9) {
		t.Fatalf("patches = %#v, want items.1001.quantity=9", patches)
	}
}

func TestRedisDocumentPatchesWithResolverUsesFieldIDUnsetTailPath(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001"},
		Kind:  change.KindUnset,
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	patches := redis.BuildDocumentPatchesWithResolver(redisFieldIDSpec(), set, testFieldResolver{3: "items"})
	if len(patches["items"].Unset) != 1 || patches["items"].Unset[0].String() != "items.1001" {
		t.Fatalf("patches = %#v, want unset items.1001", patches)
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

func redisFieldIDSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
}
