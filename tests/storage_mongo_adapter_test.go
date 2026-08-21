package tests

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

type recordingMongoUpdater struct {
	collection    string
	filter        txmongo.Filter
	update        map[string]any
	replace       map[string]any
	deleted       bool
	fields        []string
	doc           map[string]any
	exists        bool
	upsert        bool
	updateErr     error
	updateResult  *txmongo.UpdateResult
	replaceResult *txmongo.ReplaceResult
	deleteResult  *txmongo.DeleteResult

	manyCollection string
	manyFilter     txmongo.ValuesFilter
	manyFields     []string
	manyDocs       []map[string]any
	manyExists     []bool
}

type recordingMongoRouter struct {
	calls []recordingMongoRouteCall
	names map[any]string
}

type recordingMongoRouteCall struct {
	routeType string
	shardKey  any
}

type mongoDocumentRole struct {
	RoleID int64             `json:"roleId" bson:"_id"`
	Power  int64             `json:"power" bson:"p"`
	Rank   mongoDocumentRank `json:"rank" bson:"rank"`
}

type mongoDocumentRank struct {
	Level int64 `json:"level" bson:"lv"`
}

func mongoDocumentSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{
			{Name: "RoleID", JSONName: "roleId", BSONName: "_id"},
			{Name: "Power", JSONName: "power", BSONName: "p"},
			{Name: "Rank", JSONName: "rank", BSONName: "rank"},
		}},
		DocumentFactory: func() any { return new(mongoDocumentRole) },
		Mongo:           &meta.MongoSpec{Collection: "roles", IDField: "RoleID"},
	}
}

func mongoMapDocumentFactory() any {
	value := map[string]any{}
	return &value
}

func (u *recordingMongoUpdater) UpdateOne(_ context.Context, collection string, filter txmongo.Filter, update map[string]any, options storage.ApplyOptions) (txmongo.UpdateResult, error) {
	u.collection = collection
	u.filter = filter
	u.update = update
	u.upsert = options.Upsert
	if u.updateErr != nil {
		return txmongo.UpdateResult{}, u.updateErr
	}
	if u.updateResult != nil {
		return *u.updateResult, nil
	}
	return txmongo.UpdateResult{MatchedCount: 1}, nil
}

func (u *recordingMongoUpdater) ReplaceOne(_ context.Context, collection string, filter txmongo.Filter, doc map[string]any) (txmongo.ReplaceResult, error) {
	u.collection = collection
	u.filter = filter
	u.replace = doc
	if u.replaceResult != nil {
		return *u.replaceResult, nil
	}
	return txmongo.ReplaceResult{MatchedCount: 1}, nil
}

func (u *recordingMongoUpdater) DeleteOne(_ context.Context, collection string, filter txmongo.Filter) (txmongo.DeleteResult, error) {
	u.collection = collection
	u.filter = filter
	u.deleted = true
	if u.deleteResult != nil {
		return *u.deleteResult, nil
	}
	return txmongo.DeleteResult{DeletedCount: 1}, nil
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

func (r *recordingMongoRouter) RouteMongo(_ context.Context, routeType string, shardKey any) (string, error) {
	r.calls = append(r.calls, recordingMongoRouteCall{routeType: routeType, shardKey: shardKey})
	name, ok := r.names[shardKey]
	if !ok {
		return "", nil
	}
	return name, nil
}

func mustMongoAdapter(t *testing.T, client txmongo.Client) *txmongo.Adapter {
	t.Helper()
	adapter, err := txmongo.NewAdapter(client)
	if err != nil {
		t.Fatalf("NewAdapter returned error: %v", err)
	}
	return adapter
}

func mustMongoCluster(t *testing.T, router txmongo.Router, shards map[string]txmongo.Client) *txmongo.Cluster {
	t.Helper()
	cluster, err := txmongo.NewCluster(router, shards)
	if err != nil {
		t.Fatalf("NewCluster returned error: %v", err)
	}
	return cluster
}

func TestMongoAdapterAppliesUpdateByDefaultIDFilter(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if updater.collection != "roles" || updater.filter.Field != "_id" || updater.filter.Value != int64(1001) {
		t.Fatalf("collection/filter = %q/%#v", updater.collection, updater.filter)
	}
	setDoc := updater.update["$set"].(map[string]any)
	if setDoc["name"] != "alice" {
		t.Fatalf("update = %#v", updater.update)
	}
}

func TestMongoAdapterReturnsTransportError(t *testing.T) {
	transportErr := errors.New("mongo unavailable")
	adapter := mustMongoAdapter(t, &recordingMongoUpdater{updateErr: storage.NewBackendError(storage.BackendMongo, "update one", transportErr)})
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if !errors.Is(err, transportErr) {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want transport error", err)
	}
	if backend, ok := storage.BackendFromError(err); !ok || backend != storage.BackendMongo {
		t.Fatalf("BackendFromError = %v, %t, want mongo, true", backend, ok)
	}
}

func TestMongoAdapterDoesNotClassifyUnmarkedClientError(t *testing.T) {
	adapter := mustMongoAdapter(t, &recordingMongoUpdater{updateErr: errors.New("invalid bson value")})
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if _, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError classified unmarked client error: %v", err)
	}
}

func TestMongoAdapterRejectsMissingRecordOnNonUpsertUpdate(t *testing.T) {
	updater := &recordingMongoUpdater{updateResult: &txmongo.UpdateResult{}}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if !errors.Is(err, storage.ErrNotExists) {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want ErrNotExists", err)
	}
}

func TestMongoAdapterRejectsUnexpectedUpsertResult(t *testing.T) {
	updater := &recordingMongoUpdater{updateResult: &txmongo.UpdateResult{MatchedCount: 1, UpsertedCount: 1}}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: true})
	if err == nil || !strings.Contains(err.Error(), "unexpected update result") {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want unexpected update result", err)
	}
}

func TestMongoAdapterAcceptsUpsertedCreate(t *testing.T) {
	updater := &recordingMongoUpdater{updateResult: &txmongo.UpdateResult{UpsertedCount: 1}}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
}

func TestMongoAdapterRejectsUnexpectedReplacementResult(t *testing.T) {
	updater := &recordingMongoUpdater{replaceResult: &txmongo.ReplaceResult{}}
	err := mustMongoAdapter(t, updater).ReplaceDocument(context.Background(), mongoDocumentSpec(), storage.RecordKey{ID: int64(1001)}, map[string]any{"roleId": int64(1001)})
	if err == nil || !strings.Contains(err.Error(), "unexpected replace result") {
		t.Fatalf("ReplaceDocument error = %v, want unexpected replace result", err)
	}
}

func TestMongoAdapterAcceptsMissingDeleteAsIdempotent(t *testing.T) {
	updater := &recordingMongoUpdater{deleteResult: &txmongo.DeleteResult{}}
	err := mustMongoAdapter(t, updater).ApplyDelete(context.Background(), mongoDocumentSpec(), storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("ApplyDelete returned error for missing record: %v", err)
	}
}

func TestMongoAdapterRejectsUnexpectedDeleteResult(t *testing.T) {
	updater := &recordingMongoUpdater{deleteResult: &txmongo.DeleteResult{DeletedCount: 2}}
	err := mustMongoAdapter(t, updater).ApplyDelete(context.Background(), mongoDocumentSpec(), storage.RecordKey{ID: int64(1001)})
	if err == nil || !strings.Contains(err.Error(), "unexpected delete result") {
		t.Fatalf("ApplyDelete error = %v, want unexpected delete result", err)
	}
}

func TestMongoAdapterRejectsTypedNilClient(t *testing.T) {
	var client *recordingMongoUpdater
	adapter, err := txmongo.NewAdapter(client)
	if adapter != nil || !errors.Is(err, txmongo.ErrClientNotConfigured) {
		t.Fatalf("NewAdapter = %#v, %v, want nil ErrClientNotConfigured", adapter, err)
	}
}

func TestMongoAdapterRejectsDeltaSetWithoutEffectiveUpdate(t *testing.T) {
	adapter := mustMongoAdapter(t, &recordingMongoUpdater{})
	err := adapter.ApplyDeltasWithOptions(
		context.Background(),
		meta.TypeSpec{Mongo: &meta.MongoSpec{Collection: "roles"}},
		storage.RecordKey{ID: int64(1001)},
		change.DeltaSet{{Op: change.OpSet}},
		testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()),
		testDeltaValues(nil),
		storage.ApplyOptions{},
	)
	if !errors.Is(err, storage.ErrNoEffectiveDeltas) {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want ErrNoEffectiveDeltas", err)
	}
}

func TestMongoAdapterRejectsNilReplacementDocument(t *testing.T) {
	adapter := mustMongoAdapter(t, &recordingMongoUpdater{})
	err := adapter.ReplaceDocument(context.Background(), mongoDocumentSpec(), storage.RecordKey{ID: int64(1001)}, nil)
	if !errors.Is(err, storage.ErrNilDocument) {
		t.Fatalf("ReplaceDocument error = %v, want ErrNilDocument", err)
	}
}

func TestMongoAdapterMapsUpdatePathToBSONName(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "Power",
				BSONName: "p",
			}},
		},
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	const mongoFieldPower change.FieldID = 4
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(mongoFieldPower)), Op: change.OpSet}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   {mongoFieldPower: "Power"},
		change.FieldNameBSON: {mongoFieldPower: "p"},
	})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Equal(deltaPath(fieldNode(mongoFieldPower))) {
			return int64(9), true
		}
		return nil, false
	}), storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
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
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "RoleId",
				BSONName: "_id",
			}},
		},
		Mongo: &meta.MongoSpec{Collection: "roles", IDField: "RoleId"},
	}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
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
	adapter := mustMongoAdapter(t, client)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "Power",
				JSONName: "power",
				BSONName: "p",
			}},
		},
		DocumentFactory: mongoMapDocumentFactory,
		Mongo:           &meta.MongoSpec{Collection: "roles"},
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

func TestMongoAdapterConvertsWholeDocumentsBetweenJSONAndBSON(t *testing.T) {
	client := &recordingMongoUpdater{
		doc: map[string]any{
			"_id": int64(1001),
			"p":   int64(9),
			"rank": map[string]any{
				"lv": int64(4),
			},
		},
		exists: true,
	}
	adapter := mustMongoAdapter(t, client)
	spec := mongoDocumentSpec()

	input := map[string]any{
		"roleId": int64(1001),
		"power":  int64(9),
		"rank": map[string]any{
			"level": int64(4),
		},
	}
	if err := adapter.ReplaceDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, input); err != nil {
		t.Fatalf("ReplaceDocument returned error: %v", err)
	}
	if client.replace["_id"] != int64(1001) || client.replace["p"] != int64(9) {
		t.Fatalf("mongo document = %#v, want BSON field names", client.replace)
	}
	if _, ok := client.replace["roleId"]; ok {
		t.Fatalf("mongo document must not retain JSON roleId: %#v", client.replace)
	}
	rank, ok := client.replace["rank"].(map[string]any)
	if !ok || rank["lv"] != int64(4) {
		t.Fatalf("nested mongo document = %#v, want rank.lv=4", client.replace["rank"])
	}

	doc, exists, err := adapter.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists {
		t.Fatal("LoadDocument exists = false, want true")
	}
	if doc["roleId"] != json.Number("1001") || doc["power"] != json.Number("9") {
		t.Fatalf("canonical document = %#v, want JSON field names", doc)
	}
	if _, ok := doc["_id"]; ok {
		t.Fatalf("canonical document must not retain BSON _id: %#v", doc)
	}
	loadedRank, ok := doc["rank"].(map[string]any)
	if !ok || loadedRank["level"] != json.Number("4") {
		t.Fatalf("nested canonical document = %#v, want rank.level=4", doc["rank"])
	}
}

func TestMongoAdapterRejectsMalformedBatchFinderResult(t *testing.T) {
	client := &recordingMongoUpdater{
		manyDocs:   []map[string]any{{"_id": int64(1001), "p": int64(9)}},
		manyExists: nil,
	}
	adapter := mustMongoAdapter(t, client)
	spec := mongoDocumentSpec()
	keys := []storage.RecordKey{{ID: int64(1001)}}

	_, _, err := adapter.LoadDocuments(context.Background(), spec, keys)
	if err == nil {
		t.Fatal("LoadDocuments returned nil error for malformed batch result")
	}
	if !strings.Contains(err.Error(), "documents=1") || !strings.Contains(err.Error(), "exists=0") || !strings.Contains(err.Error(), "keys=1") {
		t.Fatalf("LoadDocuments error = %v, want malformed batch result context", err)
	}
}

func TestMongoAdapterRejectsMissingDocumentForExistingResult(t *testing.T) {
	client := &recordingMongoUpdater{exists: true}
	adapter := mustMongoAdapter(t, client)
	spec := mongoDocumentSpec()
	key := storage.RecordKey{ID: int64(1001)}

	_, exists, err := adapter.LoadDocument(context.Background(), spec, key)
	if err == nil {
		t.Fatal("LoadDocument returned nil error for an existing nil document")
	}
	if exists {
		t.Fatal("LoadDocument exists = true, want false for an invalid existing result")
	}
	if !strings.Contains(err.Error(), "existing document") {
		t.Fatalf("LoadDocument error = %v, want existing document context", err)
	}
}

func TestMongoAdapterRejectsDataForMissingResult(t *testing.T) {
	client := &recordingMongoUpdater{doc: map[string]any{"p": int64(9)}}
	adapter := mustMongoAdapter(t, client)

	_, exists, err := adapter.LoadDocument(context.Background(), mongoDocumentSpec(), storage.RecordKey{ID: int64(1001)})
	if err == nil {
		t.Fatal("LoadDocument returned nil error for missing result with data")
	}
	if exists {
		t.Fatal("LoadDocument exists = true, want false for an invalid missing result")
	}
	if !strings.Contains(err.Error(), "missing document with data") {
		t.Fatalf("LoadDocument error = %v, want missing document context", err)
	}
}

func TestMongoAdapterRejectsMissingBatchDocumentForExistingResult(t *testing.T) {
	client := &recordingMongoUpdater{
		manyDocs:   []map[string]any{nil},
		manyExists: []bool{true},
	}
	adapter := mustMongoAdapter(t, client)

	_, _, err := adapter.LoadDocuments(context.Background(), mongoDocumentSpec(), []storage.RecordKey{{ID: int64(1001)}})
	if err == nil {
		t.Fatal("LoadDocuments returned nil error for an existing nil document")
	}
	if !strings.Contains(err.Error(), "existing document") || !strings.Contains(err.Error(), "index 0") {
		t.Fatalf("LoadDocuments error = %v, want existing document and index context", err)
	}
}

func TestMongoAdapterRejectsBatchDataForMissingResult(t *testing.T) {
	client := &recordingMongoUpdater{
		manyDocs:   []map[string]any{{"p": int64(9)}},
		manyExists: []bool{false},
	}
	adapter := mustMongoAdapter(t, client)

	_, _, err := adapter.LoadDocuments(context.Background(), mongoDocumentSpec(), []storage.RecordKey{{ID: int64(1001)}})
	if err == nil {
		t.Fatal("LoadDocuments returned nil error for missing batch result with data")
	}
	if !strings.Contains(err.Error(), "missing document with data") {
		t.Fatalf("LoadDocuments error = %v, want missing document context", err)
	}
}

func TestMongoAdapterRejectsDocumentOperationWithoutGeneratedFactory(t *testing.T) {
	client := &recordingMongoUpdater{
		doc:    map[string]any{"name": "alice"},
		exists: true,
	}
	adapter := mustMongoAdapter(t, client)
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "data.GDRole"}}, Mongo: &meta.MongoSpec{Collection: "roles"}}
	key := storage.RecordKey{ID: int64(1001)}

	err := adapter.ReplaceDocument(context.Background(), spec, key, map[string]any{"name": "alice"})
	if err == nil || !strings.Contains(err.Error(), "missing generated document factory") {
		t.Fatalf("ReplaceDocument error = %v, want missing generated document factory", err)
	}
	_, _, err = adapter.LoadDocument(context.Background(), spec, key)
	if err == nil || !strings.Contains(err.Error(), "missing generated document factory") {
		t.Fatalf("LoadDocument error = %v, want missing generated document factory", err)
	}
}

func TestMongoAdapterRejectsIDFieldUpdate(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:     "RoleId",
				BSONName: "_id",
			}},
		},
		Mongo: &meta.MongoSpec{Collection: "roles", IDField: "RoleId"},
	}
	const mongoFieldRoleID change.FieldID = 5
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(mongoFieldRoleID)), Op: change.OpSet}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   {mongoFieldRoleID: "RoleId"},
		change.FieldNameBSON: {mongoFieldRoleID: "_id"},
	})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Equal(deltaPath(fieldNode(mongoFieldRoleID))) {
			return int64(1002), true
		}
		return nil, false
	}), storage.ApplyOptions{})
	if err == nil {
		t.Fatal("ApplyDeltasWithOptions returned nil error, want immutable _id error")
	}
	if !strings.Contains(err.Error(), "RoleId") || !strings.Contains(err.Error(), "_id") {
		t.Fatalf("error = %v, want field context", err)
	}
	if updater.update != nil {
		t.Fatalf("update was sent despite immutable _id error: %#v", updater.update)
	}
}

func TestMongoAdapterUpsertSkipsImmutableIDDelta(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{
			{Name: "RoleId", BSONName: "_id"},
			{Name: "Name", BSONName: "name"},
		}},
		Mongo: &meta.MongoSpec{Collection: "roles", IDField: "RoleId"},
	}
	const (
		mongoFieldRoleID change.FieldID = 5
		mongoFieldName   change.FieldID = 6
	)
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(mongoFieldRoleID)), Op: change.OpSet},
		{Path: deltaPath(fieldNode(mongoFieldName)), Op: change.OpSet},
	}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   {mongoFieldRoleID: "RoleId", mongoFieldName: "Name"},
		change.FieldNameBSON: {mongoFieldRoleID: "_id", mongoFieldName: "name"},
	})
	values := change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		switch path.Nodes[0].Field {
		case mongoFieldRoleID:
			return int64(1001), true
		case mongoFieldName:
			return "alice", true
		default:
			return nil, false
		}
	})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if !updater.upsert {
		t.Fatal("upsert = false, want true")
	}
	setDoc := updater.update["$set"].(map[string]any)
	if setDoc["name"] != "alice" {
		t.Fatalf("update = %#v, want name", updater.update)
	}
	if _, exists := setDoc["_id"]; exists {
		t.Fatalf("upsert update must not contain immutable _id: %#v", updater.update)
	}
}

func TestMongoAdapterRejectsUpsertWithOnlyImmutableIDDelta(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Type:  meta.Type{Fields: []meta.Field{{Name: "RoleId", BSONName: "_id"}}},
		Mongo: &meta.MongoSpec{Collection: "roles", IDField: "RoleId"},
	}
	const mongoFieldRoleID change.FieldID = 5
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(mongoFieldRoleID)), Op: change.OpSet}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   {mongoFieldRoleID: "RoleId"},
		change.FieldNameBSON: {mongoFieldRoleID: "_id"},
	})
	values := change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Equal(deltaPath(fieldNode(mongoFieldRoleID))) {
			return int64(1001), true
		}
		return nil, false
	})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{Upsert: true})
	if err == nil || !strings.Contains(err.Error(), "no mutable deltas") {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want empty upsert error", err)
	}
	if updater.update != nil {
		t.Fatalf("UpdateOne was called for invalid empty upsert: %#v", updater.update)
	}
}

func TestMongoAdapterPassesUpsertOptionToUpdate(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: false})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if updater.upsert {
		t.Fatal("upsert = true, want false for update path")
	}

	err = adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: true})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
	}
	if !updater.upsert {
		t.Fatal("upsert = false, want true for insert/replace path")
	}
}

func TestMongoAdapterPassesUpsertOptionToBusinessIDFieldUpdate(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{
		Collection: "roles",
		IDField:    "roleId",
	}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{Upsert: false})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
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
	adapter := mustMongoAdapter(t, client)
	spec := meta.TypeSpec{
		DocumentFactory: mongoMapDocumentFactory,
		Mongo: &meta.MongoSpec{
			Collection: "roles",
			IDField:    "roleId",
		},
	}
	key := storage.RecordKey{ID: int64(1001)}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	if err := adapter.ApplyDeltasWithOptions(context.Background(), spec, key, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
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
	adapter := mustMongoAdapter(t, client)
	spec := meta.TypeSpec{DocumentFactory: mongoMapDocumentFactory, Mongo: &meta.MongoSpec{Collection: "roles"}}
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
	adapter := mustMongoAdapter(t, client)
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
	adapter := mustMongoAdapter(t, client)
	spec := meta.TypeSpec{DocumentFactory: mongoMapDocumentFactory, Mongo: &meta.MongoSpec{Collection: "roles"}}

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
	adapter := mustMongoAdapter(t, client)
	spec := meta.TypeSpec{DocumentFactory: mongoMapDocumentFactory, Mongo: &meta.MongoSpec{Collection: "roles"}}

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

func TestMongoAdapterAppliesFieldIDChangesWithResolver(t *testing.T) {
	updater := &recordingMongoUpdater{}
	adapter := mustMongoAdapter(t, updater)
	spec := meta.TypeSpec{
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	deltas := change.DeltaSet{{Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)), Op: change.OpSet}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   deltaTestGoNames(),
		change.FieldNameBSON: deltaTestBSONNames(),
	})
	values := testDeltaValues(map[string]any{"Items.1001.Quantity": int64(9)})

	err := adapter.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, resolver, values, storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
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
	cluster := mustMongoCluster(t, router, map[string]txmongo.Client{
		"role-shard": roleShard,
		"user-shard": userShard,
	})
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{
		Route:      meta.RouteSpec{Type: "role"},
		Collection: "roles",
	}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := cluster.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001), ShardKey: int64(7)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyDeltasWithOptions returned error: %v", err)
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

func TestMongoClusterRejectsEmptyAssembly(t *testing.T) {
	router := &recordingMongoRouter{}
	cluster, err := txmongo.NewCluster(router, nil)
	if cluster != nil || err == nil || !strings.Contains(err.Error(), "shards are not configured") {
		t.Fatalf("NewCluster = %#v, %v, want shards configuration error", cluster, err)
	}
}

func TestMongoClusterRejectsMissingDistinctRouteShardKey(t *testing.T) {
	router := &recordingMongoRouter{names: map[any]string{int64(1001): "role-shard"}}
	cluster := mustMongoCluster(t, router, map[string]txmongo.Client{"role-shard": &recordingMongoUpdater{}})
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{
		IDField:    "roleId",
		Route:      meta.RouteSpec{Type: "role", By: "serverId"},
		Collection: "roles",
	}}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}

	err := cluster.ApplyDeltasWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, deltas, testResolverForKind(change.FieldNameBSON, deltaTestBSONNames()), testDeltaValues(map[string]any{"Name": "alice"}), storage.ApplyOptions{})
	if !errors.Is(err, storage.ErrInvalidRecordKey) {
		t.Fatalf("ApplyDeltasWithOptions error = %v, want ErrInvalidRecordKey", err)
	}
	if len(router.calls) != 0 {
		t.Fatalf("router calls = %#v, want no routing", router.calls)
	}
}

func TestMongoClusterRejectsTypedNilRouter(t *testing.T) {
	var router *recordingMongoRouter
	cluster, err := txmongo.NewCluster(router, map[string]txmongo.Client{"role": &recordingMongoUpdater{}})
	if cluster != nil || err == nil || !strings.Contains(err.Error(), "router is not configured") {
		t.Fatalf("NewCluster = %#v, %v, want router configuration error", cluster, err)
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
	cluster := mustMongoCluster(t, router, map[string]txmongo.Client{
		"a": shardA,
		"b": shardB,
	})
	spec := meta.TypeSpec{
		DocumentFactory: mongoMapDocumentFactory,
		Mongo: &meta.MongoSpec{
			Route:      meta.RouteSpec{Type: "role"},
			Collection: "roles",
		},
	}
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
