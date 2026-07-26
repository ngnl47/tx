package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

type recordingMongoUpdater struct {
	collection string
	filter     txmongo.Filter
	update     map[string]any
	replace    map[string]any
	deleted    bool
	fields     []string
	doc        map[string]any
	exists     bool
	upsert     bool

	manyCollection string
	manyFilter     txmongo.ValuesFilter
	manyFields     []string
	manyDocs       []map[string]any
	manyExists     []bool
}

type recordingMongoFinder struct {
	collection string
	filters    []txmongo.Filter
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

func (u *recordingMongoUpdater) UpdateOne(_ context.Context, collection string, filter txmongo.Filter, update map[string]any, options storage.ApplyOptions) error {
	u.collection = collection
	u.filter = filter
	u.update = update
	u.upsert = options.Upsert
	return nil
}

func (u *recordingMongoUpdater) ReplaceOne(_ context.Context, collection string, filter txmongo.Filter, doc map[string]any) error {
	u.collection = collection
	u.filter = filter
	u.replace = doc
	return nil
}

func (u *recordingMongoUpdater) DeleteOne(_ context.Context, collection string, filter txmongo.Filter) error {
	u.collection = collection
	u.filter = filter
	u.deleted = true
	return nil
}

func (u *recordingMongoUpdater) FindOne(_ context.Context, collection string, filter txmongo.Filter, fields ...string) (map[string]any, bool, error) {
	u.collection = collection
	u.filter = filter
	u.fields = append([]string(nil), fields...)
	return u.doc, u.exists, nil
}

func (u *recordingMongoUpdater) FindMany(_ context.Context, collection string, filter txmongo.ValuesFilter, fields ...string) ([]map[string]any, []bool, error) {
	u.manyCollection = collection
	u.manyFilter = txmongo.ValuesFilter{Field: filter.Field, Values: append([]any(nil), filter.Values...)}
	u.manyFields = append([]string(nil), fields...)
	return u.manyDocs, u.manyExists, nil
}

func (f *recordingMongoFinder) FindOne(_ context.Context, collection string, filter txmongo.Filter, fields ...string) (map[string]any, bool, error) {
	f.collection = collection
	f.filters = append(f.filters, filter)
	f.fields = append([]string(nil), fields...)
	doc, exists := f.docs[filter.Value]
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

func TestMongoAdapterAppliesUpdateByDefaultIDFilter(t *testing.T) {
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
	if updater.collection != "roles" || updater.filter.Field != "_id" || updater.filter.Value != int64(1001) {
		t.Fatalf("collection/filter = %q/%#v", updater.collection, updater.filter)
	}
	setDoc := updater.update["$set"].(map[string]any)
	if setDoc["name"] != "alice" {
		t.Fatalf("update = %#v", updater.update)
	}
}

func TestMongoAdapterMapsUpdatePathToBSONName(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(updater)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "Power",
				BSONName: "p",
			}},
		},
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"Power"}, Kind: change.KindSet, Value: int64(9)})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	setDoc := updater.update["$set"].(map[string]any)
	if setDoc["p"] != int64(9) {
		t.Fatalf("update = %#v, want $set p=9", updater.update)
	}
	if _, exists := setDoc["Power"]; exists {
		t.Fatalf("update = %#v, should not use Go field name", updater.update)
	}
}

func TestMongoAdapterUsesBSONIDFieldForFilter(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(updater)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "RoleId",
				BSONName: "_id",
			}},
		},
		Mongo: &meta.MongoSpec{Collection: "roles", IDField: "RoleId"},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"Name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if updater.filter.Field != "_id" || updater.filter.Value != int64(1001) {
		t.Fatalf("filter = %#v, want _id/1001", updater.filter)
	}
}

func TestMongoAdapterMapsProjectionToBSONName(t *testing.T) {
	client := &recordingMongoUpdater{
		doc:    map[string]any{"p": int64(9)},
		exists: true,
	}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "Power",
				BSONName: "p",
			}},
		},
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}

	_, exists, err := adapter.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "Power")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists {
		t.Fatal("LoadDocument exists = false, want true")
	}
	if len(client.fields) != 1 || client.fields[0] != "p" {
		t.Fatalf("fields = %#v, want [p]", client.fields)
	}
}

func TestMongoAdapterRejectsIDFieldUpdate(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(updater)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "RoleId",
				BSONName: "_id",
			}},
		},
		Mongo: &meta.MongoSpec{Collection: "roles", IDField: "RoleId"},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"RoleId"}, Kind: change.KindSet, Value: int64(1002)})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{})
	if err == nil {
		t.Fatal("ApplyChangesWithOptions returned nil error, want immutable _id error")
	}
	if !strings.Contains(err.Error(), "RoleId") || !strings.Contains(err.Error(), "_id") {
		t.Fatalf("error = %v, want field context", err)
	}
	if updater.update != nil {
		t.Fatalf("update was sent despite immutable _id error: %#v", updater.update)
	}
}

func TestMongoAdapterPassesUpsertOptionToUpdate(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(updater)
	spec := meta.TypeSpec{
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{Upsert: false})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if updater.upsert {
		t.Fatal("upsert = true, want false for update path")
	}

	err = adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if !updater.upsert {
		t.Fatal("upsert = false, want true for insert/replace path")
	}
}

func TestMongoAdapterPassesUpsertOptionToBusinessIDFieldUpdate(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(updater)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{
		Collection: "roles",
		IDField:    "roleId",
	}}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, set, storage.ApplyOptions{Upsert: false})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if updater.filter.Field != "roleId" || updater.filter.Value != int64(1001) {
		t.Fatalf("filter = %#v, want roleId/1001", updater.filter)
	}
	if updater.upsert {
		t.Fatal("upsert = true, want false for update path")
	}
}

func TestMongoAdapterUsesBusinessIDFieldWhenConfigured(t *testing.T) {
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
		Collection: "roles",
		IDField:    "roleId",
	}}
	key := storage.RecordKey{ID: int64(1001)}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	if err := adapter.ApplyChangesWithOptions(context.Background(), spec, key, set, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if client.collection != "roles" || client.filter.Field != "roleId" || client.filter.Value != int64(1001) {
		t.Fatalf("update target = collection %q filter %#v", client.collection, client.filter)
	}

	if err := adapter.ReplaceDocument(context.Background(), spec, key, map[string]any{"roleId": int64(1001), "name": "bob"}); err != nil {
		t.Fatalf("ReplaceDocument returned error: %v", err)
	}
	if client.filter.Field != "roleId" || client.filter.Value != int64(1001) || client.replace["name"] != "bob" {
		t.Fatalf("replace target filter/doc = %#v/%#v", client.filter, client.replace)
	}

	if _, exists, err := adapter.LoadDocument(context.Background(), spec, key, "name"); err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	} else if !exists {
		t.Fatal("LoadDocument exists = false, want true")
	}
	if client.filter.Field != "roleId" || client.filter.Value != int64(1001) || len(client.fields) != 1 || client.fields[0] != "name" {
		t.Fatalf("load target filter/fields = %#v/%#v", client.filter, client.fields)
	}

	docs, exists, err := adapter.LoadDocuments(context.Background(), spec, []storage.RecordKey{
		{ID: int64(1001)},
		{ID: int64(1002)},
	}, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if client.manyFilter.Field != "roleId" || len(client.manyFilter.Values) != 2 || client.manyFilter.Values[0] != int64(1001) || client.manyFilter.Values[1] != int64(1002) {
		t.Fatalf("batch target filter = %#v", client.manyFilter)
	}
	if len(docs) != 2 || docs[0]["name"] != "alice" || docs[1] != nil || len(exists) != 2 || !exists[0] || exists[1] {
		t.Fatalf("batch docs=%#v exists=%#v", docs, exists)
	}

	if err := adapter.ApplyDelete(context.Background(), spec, key); err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if client.filter.Field != "roleId" || client.filter.Value != int64(1001) || !client.deleted {
		t.Fatalf("delete target filter/deleted = %#v/%v", client.filter, client.deleted)
	}
}

func TestMongoAdapterReplacesDocumentByDefaultIDFilter(t *testing.T) {
	client := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}
	doc := map[string]any{"roleId": int64(1001), "name": "alice"}

	err := adapter.ReplaceDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, doc)
	if err != nil {
		t.Fatalf("ReplaceDocument returned error: %v", err)
	}
	if client.collection != "roles" || client.filter.Field != "_id" || client.filter.Value != int64(1001) {
		t.Fatalf("collection/filter = %q/%#v", client.collection, client.filter)
	}
	if client.replace["name"] != "alice" {
		t.Fatalf("replace = %#v", client.replace)
	}
}

func TestMongoAdapterDeletesDocumentByDefaultIDFilter(t *testing.T) {
	client := &recordingMongoUpdater{}
	adapter := txmongo.NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}

	err := adapter.ApplyDelete(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if client.collection != "roles" || client.filter.Field != "_id" || client.filter.Value != int64(1001) || !client.deleted {
		t.Fatalf("delete collection/filter/deleted = %q/%#v/%v", client.collection, client.filter, client.deleted)
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
	if client.collection != "roles" || client.filter.Field != "_id" || client.filter.Value != int64(1001) {
		t.Fatalf("collection/filter = %q/%#v", client.collection, client.filter)
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
	if client.manyFilter.Field != "_id" || len(client.manyFilter.Values) != 2 || client.manyFilter.Values[0] != int64(1001) || client.manyFilter.Values[1] != int64(1002) {
		t.Fatalf("filter = %#v, want _id [1001 1002]", client.manyFilter)
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

func TestMongoAdapterLoadDocumentsFallsBackToFindOne(t *testing.T) {
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
	if len(client.filters) != 2 || client.filters[0].Field != "_id" || client.filters[0].Value != int64(1001) || client.filters[1].Field != "_id" || client.filters[1].Value != int64(1002) {
		t.Fatalf("filters = %#v, want _id [1001 1002]", client.filters)
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
	if roleShard.collection != "roles" || roleShard.filter.Field != "_id" || roleShard.filter.Value != int64(1001) {
		t.Fatalf("role shard collection/filter = %q/%#v", roleShard.collection, roleShard.filter)
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
	if shardA.manyFilter.Field != "_id" || len(shardA.manyFilter.Values) != 2 || shardA.manyFilter.Values[0] != int64(1001) || shardA.manyFilter.Values[1] != int64(1003) {
		t.Fatalf("shard A filter = %#v, want _id [1001 1003]", shardA.manyFilter)
	}
	if shardB.manyFilter.Field != "_id" || len(shardB.manyFilter.Values) != 1 || shardB.manyFilter.Values[0] != int64(1002) {
		t.Fatalf("shard B filter = %#v, want _id [1002]", shardB.manyFilter)
	}
}
