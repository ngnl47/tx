package tests

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

type recordingMongoUpdater struct {
	collection string
	id         any
	update     map[string]any
}

func (u *recordingMongoUpdater) UpdateByID(_ context.Context, collection string, id any, update map[string]any) error {
	u.collection = collection
	u.id = id
	u.update = update
	return nil
}

func TestMongoAdapterAppliesUpdateByID(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(updater)
	spec := meta.TypeSpec{
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChanges(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set)
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	if updater.collection != "roles" || updater.id != int64(1001) {
		t.Fatalf("collection/id = %q/%v", updater.collection, updater.id)
	}
	setDoc := updater.update["$set"].(map[string]any)
	if setDoc["name"] != "alice" {
		t.Fatalf("update = %#v", updater.update)
	}
}

func TestMongoAdapterAppliesFieldIDChangesWithResolver(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapterWithResolver(updater, testFieldResolver{3: "items"})
	spec := meta.TypeSpec{
		Mongo: &meta.MongoSpec{Collection: "roles"},
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
	setDoc := updater.update["$set"].(map[string]any)
	if setDoc["items.1001.quantity"] != int64(9) {
		t.Fatalf("update = %#v", updater.update)
	}
}
