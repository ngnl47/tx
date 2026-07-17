package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
)

type recordingQueue struct {
	ops []fusion.DelayedOperation
}

func (q *recordingQueue) Enqueue(_ context.Context, op fusion.DelayedOperation) error {
	q.ops = append(q.ops, op)
	return nil
}

type recordingApplier struct {
	optionRuns int
	spec       meta.TypeSpec
	key        storage.RecordKey
	set        change.ChangeSet
	options    storage.ApplyOptions
}

func (a *recordingApplier) ApplyChangesWithOptions(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet, options storage.ApplyOptions) error {
	a.optionRuns++
	a.spec = spec
	a.key = key
	a.set = set
	a.options = options
	return nil
}

type recordingDeleteApplier struct {
	calls int
	spec  meta.TypeSpec
	key   storage.RecordKey
}

func (a *recordingDeleteApplier) ApplyDelete(_ context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	a.calls++
	a.spec = spec
	a.key = key
	return nil
}

type recordingDocumentLoader struct {
	doc        map[string]any
	exists     bool
	docs       []map[string]any
	existsMany []bool
	calls      int
	manyCalls  int
	spec       meta.TypeSpec
	key        storage.RecordKey
	keys       []storage.RecordKey
	fields     []string
}

type recordingSingleDocumentLoader struct {
	docs   map[any]map[string]any
	calls  int
	keys   []storage.RecordKey
	fields []string
}

func (l *recordingDocumentLoader) LoadDocument(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	l.calls++
	l.spec = spec
	l.key = key
	l.fields = append([]string(nil), fields...)
	return l.doc, l.exists, nil
}

func (l *recordingDocumentLoader) LoadDocuments(_ context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	l.manyCalls++
	l.spec = spec
	l.keys = append([]storage.RecordKey(nil), keys...)
	l.fields = append([]string(nil), fields...)
	docs := make([]map[string]any, len(keys))
	exists := make([]bool, len(keys))
	copy(docs, l.docs)
	copy(exists, l.existsMany)
	return docs, exists, nil
}

func (l *recordingSingleDocumentLoader) LoadDocument(_ context.Context, _ meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	l.calls++
	l.keys = append(l.keys, key)
	l.fields = append([]string(nil), fields...)
	doc, exists := l.docs[key.ID]
	return doc, exists, nil
}

type recordingRedisStore struct {
	recordingApplier
	recordingDeleteApplier
	recordingDocumentLoader
}

func TestFusionStrategyEnqueuesMongoSave(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelaySave: meta.Duration{Duration: 600 * time.Second},
		},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})
	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, set, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
	if queue.ops[0].RunAfter != 600*time.Second {
		t.Fatalf("RunAfter = %s", queue.ops[0].RunAfter)
	}
}

func TestFusionStrategyAppliesRedisBeforeQueue(t *testing.T) {
	queue := &recordingQueue{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{Redis: redis, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})
	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, set, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.optionRuns != 1 {
		t.Fatalf("redis option calls = %d, want 1", redis.optionRuns)
	}
	if redis.key.ID != int64(1001) {
		t.Fatalf("redis key = %#v", redis.key)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
}

func TestFusionStrategyPassesOptionsOnlyToRedis(t *testing.T) {
	queue := &recordingQueue{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{Redis: redis, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
	}
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, set, storage.ApplyOptions{Upsert: true, TTL: 600})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.optionRuns != 1 {
		t.Fatalf("redis option calls = %d, want 1", redis.optionRuns)
	}
	if redis.options.TTL != 600 || !redis.options.Upsert {
		t.Fatalf("redis options = %#v, want TTL 600 and upsert", redis.options)
	}
	if redis.key != key {
		t.Fatalf("redis key = %#v, want %#v", redis.key, key)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v, want one save op", queue.ops)
	}
	if queue.ops[0].ID != key.ID || queue.ops[0].RunAfter != time.Second {
		t.Fatalf("queued op = %#v, want ID and delay only", queue.ops[0])
	}
}

func TestFusionStrategySkipsEmptyChangeSet(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{MongoQueue: queue}
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), meta.TypeSpec{}, key, change.ChangeSet{}, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(queue.ops) != 0 {
		t.Fatalf("queued ops = %#v, want empty", queue.ops)
	}
}

func TestFusionStrategyEnqueuesMongoDelete(t *testing.T) {
	queue := &recordingQueue{}
	redisDelete := &recordingDeleteApplier{}
	strategy := fusion.Strategy{RedisDeleter: redisDelete, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}

	if err := strategy.ApplyDelete(context.Background(), spec, storage.RecordKey{ID: int64(1001)}); err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if redisDelete.calls != 1 {
		t.Fatalf("redis delete calls = %d, want 1", redisDelete.calls)
	}
	if redisDelete.key.ID != int64(1001) {
		t.Fatalf("redis delete key = %#v, want ID=1001", redisDelete.key)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
	if queue.ops[0].Kind != fusion.DelayedDelete || queue.ops[0].RunAfter != 24*time.Hour {
		t.Fatalf("queued op = %#v", queue.ops[0])
	}
}

func TestFusionStrategyDeleteUsesRedisApplierWhenDeleterIsUnset(t *testing.T) {
	queue := &recordingQueue{}
	redis := &recordingRedisStore{}
	strategy := fusion.Strategy{Redis: redis, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}
	key := storage.RecordKey{ID: int64(1001)}

	if err := strategy.ApplyDelete(context.Background(), spec, key); err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if redis.recordingDeleteApplier.calls != 1 {
		t.Fatalf("redis delete calls = %d, want 1", redis.recordingDeleteApplier.calls)
	}
	if redis.recordingDeleteApplier.key.ID != key.ID {
		t.Fatalf("redis delete key = %#v, want %#v", redis.recordingDeleteApplier.key, key)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v, want one delete op", queue.ops)
	}
}

func TestFusionStrategyLoadDocumentUsesRedisFirst(t *testing.T) {
	redis := &recordingDocumentLoader{doc: map[string]any{"name": "hot"}, exists: true}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	strategy := fusion.Strategy{RedisLoader: redis, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "hot" {
		t.Fatalf("LoadDocument = %#v, %v; want Redis document", doc, exists)
	}
	if redis.calls != 1 || mongo.calls != 0 {
		t.Fatalf("loader calls redis=%d mongo=%d, want redis only", redis.calls, mongo.calls)
	}
	if len(redis.fields) != 1 || redis.fields[0] != "name" {
		t.Fatalf("redis fields = %#v, want name", redis.fields)
	}
}

func TestFusionStrategyLoadDocumentUsesRedisApplierWhenLoaderIsUnset(t *testing.T) {
	redis := &recordingRedisStore{
		recordingDocumentLoader: recordingDocumentLoader{
			doc:    map[string]any{"name": "hot"},
			exists: true,
		},
	}
	strategy := fusion.Strategy{Redis: redis}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "hot" {
		t.Fatalf("LoadDocument = %#v, %v; want Redis document", doc, exists)
	}
	if redis.recordingDocumentLoader.calls != 1 {
		t.Fatalf("redis loader calls = %d, want 1", redis.recordingDocumentLoader.calls)
	}
}

func TestFusionStrategyLoadDocumentFallsBackToMongo(t *testing.T) {
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	strategy := fusion.Strategy{RedisLoader: redis, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "cold" {
		t.Fatalf("LoadDocument = %#v, %v; want Mongo document", doc, exists)
	}
	if redis.calls != 1 || mongo.calls != 1 {
		t.Fatalf("loader calls redis=%d mongo=%d, want both loaders", redis.calls, mongo.calls)
	}
}

func TestFusionStrategyLoadDocumentsFallsBackMissingRowsToMongo(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}, {ID: int64(1002)}, {ID: int64(1003)}}
	redis := &recordingDocumentLoader{
		docs:       []map[string]any{{"name": "hot-1"}, nil, {"name": "hot-3"}},
		existsMany: []bool{true, false, true},
	}
	mongo := &recordingDocumentLoader{
		docs:       []map[string]any{{"name": "cold-2"}},
		existsMany: []bool{true},
	}
	strategy := fusion.Strategy{RedisLoader: redis, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if len(docs) != 3 || len(exists) != 3 {
		t.Fatalf("LoadDocuments returned docs=%d exists=%d, want 3", len(docs), len(exists))
	}
	if !exists[0] || docs[0]["name"] != "hot-1" {
		t.Fatalf("docs[0] = %#v exists=%v, want Redis row", docs[0], exists[0])
	}
	if !exists[1] || docs[1]["name"] != "cold-2" {
		t.Fatalf("docs[1] = %#v exists=%v, want Mongo fallback row", docs[1], exists[1])
	}
	if !exists[2] || docs[2]["name"] != "hot-3" {
		t.Fatalf("docs[2] = %#v exists=%v, want Redis row", docs[2], exists[2])
	}
	if redis.manyCalls != 1 || mongo.manyCalls != 1 {
		t.Fatalf("loader calls redis=%d mongo=%d, want one batch call each", redis.manyCalls, mongo.manyCalls)
	}
	if len(mongo.keys) != 1 || mongo.keys[0].ID != int64(1002) {
		t.Fatalf("mongo fallback keys = %#v, want only missing key", mongo.keys)
	}
}

func TestFusionStrategyLoadDocumentsUsesSingleRedisLoaderBeforeMongo(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}, {ID: int64(1002)}, {ID: int64(1003)}}
	redis := &recordingSingleDocumentLoader{
		docs: map[any]map[string]any{
			int64(1001): {"name": "hot-1"},
			int64(1003): {"name": "hot-3"},
		},
	}
	mongo := &recordingDocumentLoader{
		docs:       []map[string]any{{"name": "cold-2"}},
		existsMany: []bool{true},
	}
	strategy := fusion.Strategy{RedisLoader: redis, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "hot-1" {
		t.Fatalf("docs[0] = %#v exists=%v, want Redis row", docs[0], exists[0])
	}
	if !exists[1] || docs[1]["name"] != "cold-2" {
		t.Fatalf("docs[1] = %#v exists=%v, want Mongo fallback row", docs[1], exists[1])
	}
	if !exists[2] || docs[2]["name"] != "hot-3" {
		t.Fatalf("docs[2] = %#v exists=%v, want Redis row", docs[2], exists[2])
	}
	if redis.calls != 3 {
		t.Fatalf("redis single calls = %d, want 3", redis.calls)
	}
	if len(mongo.keys) != 1 || mongo.keys[0].ID != int64(1002) {
		t.Fatalf("mongo fallback keys = %#v, want only missing key", mongo.keys)
	}
}

func TestFusionStrategyLoadDocumentsUsesRedisApplierWhenLoaderIsUnset(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}, {ID: int64(1002)}}
	redis := &recordingRedisStore{
		recordingDocumentLoader: recordingDocumentLoader{
			docs:       []map[string]any{{"name": "hot-1"}, {"name": "hot-2"}},
			existsMany: []bool{true, true},
		},
	}
	strategy := fusion.Strategy{Redis: redis}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if len(docs) != 2 || len(exists) != 2 {
		t.Fatalf("LoadDocuments lengths docs=%d exists=%d, want 2", len(docs), len(exists))
	}
	if !exists[0] || docs[0]["name"] != "hot-1" || !exists[1] || docs[1]["name"] != "hot-2" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want Redis rows", docs, exists)
	}
	if redis.manyCalls != 1 {
		t.Fatalf("redis batch calls = %d, want 1", redis.manyCalls)
	}
}
