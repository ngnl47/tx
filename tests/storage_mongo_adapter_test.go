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
	fields     []string
	doc        map[string]any
	exists     bool

	manyCollection string
	manyIDs        []any
	manyFields     []string
	manyDocs       []map[string]any
	manyExists     []bool
}

func (u *recordingMongoUpdater) UpdateByID(_ context.Context, collection string, id any, update map[string]any) error {
	u.collection = collection
	u.id = id
	u.update = update
	return nil
}

func (u *recordingMongoUpdater) FindByID(_ context.Context, collection string, id any, fields ...string) (map[string]any, bool, error) {
	u.collection = collection
	u.id = id
	u.fields = append([]string(nil), fields...)
	return u.doc, u.exists, nil
}

func (u *recordingMongoUpdater) FindManyByID(_ context.Context, collection string, ids []any, fields ...string) ([]map[string]any, []bool, error) {
	u.manyCollection = collection
	u.manyIDs = append([]any(nil), ids...)
	u.manyFields = append([]string(nil), fields...)
	return u.manyDocs, u.manyExists, nil
}

func TestMongoAdapterAppliesUpdateByID(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(updater)
	spec := meta.TypeSpec{
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{})
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

func TestMongoAdapterLoadDocumentPassesProjection(t *testing.T) {
	client := &recordingMongoUpdater{
		doc:    map[string]any{"name": "alice"},
		exists: true,
	}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}

	doc, exists, err := adapter.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "name", "roleId")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists {
		t.Fatal("LoadDocument exists = false, want true")
	}
	if client.collection != "roles" || client.id != int64(1001) {
		t.Fatalf("collection/id = %q/%v", client.collection, client.id)
	}
	if len(client.fields) != 2 || client.fields[0] != "name" || client.fields[1] != "roleId" {
		t.Fatalf("fields = %#v, want [name roleId]", client.fields)
	}
	if doc["name"] != "alice" {
		t.Fatalf("doc = %#v", doc)
	}
}

func TestMongoAdapterLoadDocumentsPassesProjection(t *testing.T) {
	client := &recordingMongoUpdater{
		manyDocs: []map[string]any{
			{"name": "alice"},
			nil,
		},
		manyExists: []bool{true, false},
	}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}

	docs, exists, err := adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{
		{ID: int64(1001)},
		{ID: int64(1002)},
	}, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if client.manyCollection != "roles" {
		t.Fatalf("collection = %q, want roles", client.manyCollection)
	}
	if len(client.manyIDs) != 2 || client.manyIDs[0] != int64(1001) || client.manyIDs[1] != int64(1002) {
		t.Fatalf("ids = %#v, want [1001 1002]", client.manyIDs)
	}
	if len(client.manyFields) != 1 || client.manyFields[0] != "name" {
		t.Fatalf("fields = %#v, want [name]", client.manyFields)
	}
	if len(docs) != 2 || docs[0]["name"] != "alice" || docs[1] != nil {
		t.Fatalf("docs = %#v", docs)
	}
	if len(exists) != 2 || !exists[0] || exists[1] {
		t.Fatalf("exists = %#v", exists)
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

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	setDoc := updater.update["$set"].(map[string]any)
	if setDoc["items.1001.quantity"] != int64(9) {
		t.Fatalf("update = %#v", updater.update)
	}
}
