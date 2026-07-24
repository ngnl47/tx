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
	replace    map[string]any
	deleted    bool
	fields     []string
	doc        map[string]any
	exists     bool
	field      string
	value      any

	manyCollection string
	manyIDs        []any
	manyFields     []string
	manyDocs       []map[string]any
	manyExists     []bool
	manyField      string
	manyValues     []any
}

type recordingMongoFinder struct {
	collection string
	ids        []any
	fields     []string
	docs       map[any]map[string]any
}

type recordingMongoRouter struct {
	calls []recordingMongoRouteCall
	names map[any]string
}

type recordingMongoRouteCall struct {
	routeType string
	shardKey  any
}

func (u *recordingMongoUpdater) UpdateByID(_ context.Context, collection string, id any, update map[string]any) error {
	u.collection = collection
	u.id = id
	u.update = update
	return nil
}

func (u *recordingMongoUpdater) ReplaceByID(_ context.Context, collection string, id any, doc map[string]any) error {
	u.collection = collection
	u.id = id
	u.replace = doc
	return nil
}

func (u *recordingMongoUpdater) DeleteByID(_ context.Context, collection string, id any) error {
	u.collection = collection
	u.id = id
	u.deleted = true
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

func (u *recordingMongoUpdater) UpdateByField(_ context.Context, collection string, field string, value any, update map[string]any) error {
	u.collection = collection
	u.field = field
	u.value = value
	u.update = update
	return nil
}

func (u *recordingMongoUpdater) ReplaceByField(_ context.Context, collection string, field string, value any, doc map[string]any) error {
	u.collection = collection
	u.field = field
	u.value = value
	u.replace = doc
	return nil
}

func (u *recordingMongoUpdater) DeleteByField(_ context.Context, collection string, field string, value any) error {
	u.collection = collection
	u.field = field
	u.value = value
	u.deleted = true
	return nil
}

func (u *recordingMongoUpdater) FindByField(_ context.Context, collection string, field string, value any, fields ...string) (map[string]any, bool, error) {
	u.collection = collection
	u.field = field
	u.value = value
	u.fields = append([]string(nil), fields...)
	return u.doc, u.exists, nil
}

func (u *recordingMongoUpdater) FindManyByField(_ context.Context, collection string, field string, values []any, fields ...string) ([]map[string]any, []bool, error) {
	u.manyCollection = collection
	u.manyField = field
	u.manyValues = append([]any(nil), values...)
	u.manyFields = append([]string(nil), fields...)
	return u.manyDocs, u.manyExists, nil
}

func (f *recordingMongoFinder) FindByID(_ context.Context, collection string, id any, fields ...string) (map[string]any, bool, error) {
	f.collection = collection
	f.ids = append(f.ids, id)
	f.fields = append([]string(nil), fields...)
	doc, exists := f.docs[id]
	return doc, exists, nil
}

func (r *recordingMongoRouter) RouteMongo(_ context.Context, routeType string, shardKey any) (string, error) {
	r.calls = append(r.calls, recordingMongoRouteCall{routeType: routeType, shardKey: shardKey})
	name, ok := r.names[shardKey]
	if !ok {
		return "", nil
	}
	return name, nil
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

func TestMongoAdapterUsesBusinessIDFieldWhenMongoObjectIDEnabled(t *testing.T) {
	client := &recordingMongoUpdater{
		doc:    map[string]any{"roleId": int64(1001), "name": "alice"},
		exists: true,
		manyDocs: []map[string]any{
			{"roleId": int64(1001), "name": "alice"},
			nil,
		},
		manyExists: []bool{true, false},
	}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{
		Collection:       "roles",
		IDField:          "roleId",
		UseMongoObjectID: true,
	}}
	key := storage.RecordKey{ID: int64(1001)}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	if err := adapter.ApplyChangesWithOptions(context.Background(), spec, key, set, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if client.collection != "roles" || client.field != "roleId" || client.value != int64(1001) {
		t.Fatalf("update target = collection %q field %q value %v", client.collection, client.field, client.value)
	}
	if client.id != nil {
		t.Fatalf("UpdateByID was used with id %v, want business field filter", client.id)
	}

	if err := adapter.ReplaceDocument(context.Background(), spec, key, map[string]any{"roleId": int64(1001), "name": "bob"}); err != nil {
		t.Fatalf("ReplaceDocument returned error: %v", err)
	}
	if client.field != "roleId" || client.value != int64(1001) || client.replace["name"] != "bob" {
		t.Fatalf("replace target field/value/doc = %q/%v/%#v", client.field, client.value, client.replace)
	}

	if _, exists, err := adapter.LoadDocument(context.Background(), spec, key, "name"); err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	} else if !exists {
		t.Fatal("LoadDocument exists = false, want true")
	}
	if client.field != "roleId" || client.value != int64(1001) || len(client.fields) != 1 || client.fields[0] != "name" {
		t.Fatalf("load target field/value/fields = %q/%v/%#v", client.field, client.value, client.fields)
	}

	docs, exists, err := adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{
		{ID: int64(1001)},
		{ID: int64(1002)},
	}, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if client.manyField != "roleId" || len(client.manyValues) != 2 || client.manyValues[0] != int64(1001) || client.manyValues[1] != int64(1002) {
		t.Fatalf("batch target field/values = %q/%#v", client.manyField, client.manyValues)
	}
	if len(docs) != 2 || docs[0]["name"] != "alice" || docs[1] != nil || len(exists) != 2 || !exists[0] || exists[1] {
		t.Fatalf("batch docs=%#v exists=%#v", docs, exists)
	}

	if err := adapter.ApplyDelete(context.Background(), spec, key); err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if client.field != "roleId" || client.value != int64(1001) || !client.deleted {
		t.Fatalf("delete target field/value/deleted = %q/%v/%v", client.field, client.value, client.deleted)
	}
}

func TestMongoAdapterReplacesDocumentByID(t *testing.T) {
	client := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}
	doc := map[string]any{"roleId": int64(1001), "name": "alice"}

	err := adapter.ReplaceDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, doc)
	if err != nil {
		t.Fatalf("ReplaceDocument returned error: %v", err)
	}
	if client.collection != "roles" || client.id != int64(1001) {
		t.Fatalf("collection/id = %q/%v", client.collection, client.id)
	}
	if client.replace["name"] != "alice" {
		t.Fatalf("replace = %#v", client.replace)
	}
}

func TestMongoAdapterDeletesDocumentByID(t *testing.T) {
	client := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}

	err := adapter.ApplyDelete(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if client.collection != "roles" || client.id != int64(1001) || !client.deleted {
		t.Fatalf("delete collection/id/deleted = %q/%v/%v", client.collection, client.id, client.deleted)
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

func TestMongoAdapterLoadDocumentsFallsBackToFindByID(t *testing.T) {
	client := &recordingMongoFinder{
		docs: map[any]map[string]any{
			int64(1001): {"name": "alice"},
		},
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
	if client.collection != "roles" {
		t.Fatalf("collection = %q, want roles", client.collection)
	}
	if len(client.ids) != 2 || client.ids[0] != int64(1001) || client.ids[1] != int64(1002) {
		t.Fatalf("ids = %#v, want [1001 1002]", client.ids)
	}
	if len(client.fields) != 1 || client.fields[0] != "name" {
		t.Fatalf("fields = %#v, want [name]", client.fields)
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

func TestMongoClusterRoutesSingleWriteByShardKey(t *testing.T) {
	roleShard := &recordingMongoUpdater{}
	userShard := &recordingMongoUpdater{}
	router := &recordingMongoRouter{names: map[any]string{
		int64(7): "role-shard",
	}}
	cluster := txmongo.NewCluster(router, map[string]any{
		"role-shard": roleShard,
		"user-shard": userShard,
	})
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{
		Route:      meta.RouteSpec{Type: "role"},
		Collection: "roles",
	}}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := cluster.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001), ShardKey: int64(7)}, set, storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(router.calls) != 1 || router.calls[0].routeType != "role" || router.calls[0].shardKey != int64(7) {
		t.Fatalf("router calls = %#v, want role shard key 7", router.calls)
	}
	if roleShard.collection != "roles" || roleShard.id != int64(1001) {
		t.Fatalf("role shard collection/id = %q/%v", roleShard.collection, roleShard.id)
	}
	if userShard.update != nil {
		t.Fatalf("unexpected user shard update = %#v", userShard.update)
	}
}

func TestMongoClusterBatchLoadsByShardGroupAndRestoresOrder(t *testing.T) {
	shardA := &recordingMongoUpdater{
		manyDocs: []map[string]any{
			{"name": "alice"},
			{"name": "carol"},
		},
		manyExists: []bool{true, true},
	}
	shardB := &recordingMongoUpdater{
		manyDocs:   []map[string]any{nil},
		manyExists: []bool{false},
	}
	router := &recordingMongoRouter{names: map[any]string{
		int64(7): "a",
		int64(8): "b",
	}}
	cluster := txmongo.NewCluster(router, map[string]any{
		"a": shardA,
		"b": shardB,
	})
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{
		Route:      meta.RouteSpec{Type: "role"},
		Collection: "roles",
	}}
	keys := []storage.RecordKey{
		{ID: int64(1001), ShardKey: int64(7)},
		{ID: int64(1002), ShardKey: int64(8)},
		{ID: int64(1003), ShardKey: int64(7)},
	}

	docs, exists, err := cluster.LoadDocuments(context.Background(), spec, keys, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if len(docs) != 3 || len(exists) != 3 {
		t.Fatalf("LoadDocuments lengths docs=%d exists=%d, want 3", len(docs), len(exists))
	}
	if !exists[0] || docs[0]["name"] != "alice" || exists[1] || docs[1] != nil || !exists[2] || docs[2]["name"] != "carol" {
		t.Fatalf("docs=%#v exists=%#v, want original order", docs, exists)
	}
	if len(shardA.manyIDs) != 2 || shardA.manyIDs[0] != int64(1001) || shardA.manyIDs[1] != int64(1003) {
		t.Fatalf("shard A ids = %#v, want [1001 1003]", shardA.manyIDs)
	}
	if len(shardB.manyIDs) != 1 || shardB.manyIDs[0] != int64(1002) {
		t.Fatalf("shard B ids = %#v, want [1002]", shardB.manyIDs)
	}
}
