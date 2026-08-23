package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
)

type recordingQueue struct {
	ops []fusion.DelayedOperation
	err error
}

func (q *recordingQueue) Enqueue(_ context.Context, op fusion.DelayedOperation) error {
	q.ops = append(q.ops, op)
	return q.err
}

type recordingApplier struct {
	optionRuns int
	spec       meta.TypeSpec
	key        storage.RecordKey
	changes    recordedChanges
	options    storage.ApplyOptions
	err        error
}

func (a *recordingApplier) ApplyChangesWithOptions(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, _ change.FieldNameResolver, _ change.ValueResolver, options storage.ApplyOptions) error {
	a.optionRuns++
	a.spec = spec
	a.key = key
	changes, err := collectRecordedChanges(source)
	if err != nil {
		return err
	}
	a.changes = changes
	a.options = options
	return a.err
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

type recordingDocumentReplacerWithOptions struct {
	calls   int
	spec    meta.TypeSpec
	key     storage.RecordKey
	doc     map[string]any
	options storage.ApplyOptions
}

type recordingRecordLocker struct {
	calls   int
	spec    meta.TypeSpec
	key     storage.RecordKey
	options storage.LockOptions
	lock    storage.RecordLock
	err     error
	nilLock bool
}

type recordingRecordLock struct {
	unlocks   int
	unlockErr error
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
	if len(l.docs) > 0 {
		doc := l.docs[0]
		l.docs = l.docs[1:]
		exists := false
		if len(l.existsMany) > 0 {
			exists = l.existsMany[0]
			l.existsMany = l.existsMany[1:]
		}
		return doc, exists, nil
	}
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

func (r *recordingDocumentReplacerWithOptions) ReplaceDocumentWithOptions(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) error {
	r.calls++
	r.spec = spec
	r.key = key
	r.doc = doc
	r.options = options
	return nil
}

func (l *recordingRecordLocker) LockRecord(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, options storage.LockOptions) (storage.RecordLock, error) {
	l.calls++
	l.spec = spec
	l.key = key
	l.options = options
	if l.err != nil {
		return nil, l.err
	}
	if l.nilLock {
		return nil, nil
	}
	if l.lock != nil {
		return l.lock, nil
	}
	return &recordingRecordLock{}, nil
}

func (l *recordingRecordLock) Unlock(context.Context) error {
	l.unlocks++
	return l.unlockErr
}

type recordingRedisStore struct {
	recordingApplier
	recordingDocumentLoader
}

func TestFusionStrategyEnqueuesMongoSave(t *testing.T) {
	queue := &recordingQueue{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelaySave: meta.Duration{Duration: 600 * time.Second},
		},
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
	if redis.optionRuns != 1 {
		t.Fatalf("redis runs = %d, want 1", redis.optionRuns)
	}
	if queue.ops[0].RunAfter != 600*time.Second {
		t.Fatalf("RunAfter = %s", queue.ops[0].RunAfter)
	}
}

func TestFusionStrategyReturnsRedisTransportErrorWithoutEnqueue(t *testing.T) {
	transportErr := errors.New("redis unavailable")
	queue := &recordingQueue{}
	strategy := fusion.Strategy{RedisApplier: &recordingApplier{err: transportErr}, MongoLoader: &recordingDocumentLoader{}, MongoQueue: queue}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{})
	if !errors.Is(err, transportErr) {
		t.Fatalf("ApplyChangesWithOptions error = %v, want transport error", err)
	}
	if len(queue.ops) != 0 {
		t.Fatalf("queue ops = %#v, want no delayed task after Redis failure", queue.ops)
	}
}

func TestFusionStrategyEnqueuesDelayedRedisDeleteAfterSaveWindow(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{RedisApplier: &recordingApplier{}, MongoLoader: &recordingDocumentLoader{}, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelaySave:   meta.Duration{Duration: 10 * time.Minute},
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001), ShardKey: int64(7)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(queue.ops) != 2 {
		t.Fatalf("queued ops = %#v, want save and delayed delete", queue.ops)
	}
	if queue.ops[0].Kind != fusion.DelayedSave || queue.ops[0].RunAfter != 10*time.Minute {
		t.Fatalf("save op = %#v, want 10 minute save", queue.ops[0])
	}
	if queue.ops[1].Kind != fusion.DelayedDelete || queue.ops[1].RunAfter != 24*time.Hour+10*time.Minute {
		t.Fatalf("delete op = %#v, want delaySave + delayDel", queue.ops[1])
	}
	if queue.ops[1].Key != key {
		t.Fatalf("delete key = %#v, want %#v", queue.ops[1].Key, key)
	}
}

func TestFusionStrategyRejectsMissingWriteDependencies(t *testing.T) {
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}

	if err := (fusion.Strategy{MongoQueue: &recordingQueue{}}).ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); !errors.Is(err, fusion.ErrRedisNotConfigured) {
		t.Fatalf("missing Redis error = %v, want ErrRedisNotConfigured", err)
	}
	if err := (fusion.Strategy{RedisApplier: &recordingApplier{}}).ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); !errors.Is(err, fusion.ErrMongoQueueNotConfigured) {
		t.Fatalf("missing MongoQueue error = %v, want ErrMongoQueueNotConfigured", err)
	}
}

func TestFusionStrategyRejectsWriteWithoutMongoLoaderBeforeRedisWrite(t *testing.T) {
	redis := &recordingApplier{}
	strategy := fusion.Strategy{
		RedisApplier: redis,
		MongoQueue:   &recordingQueue{},
	}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)},
		changes,
		testFieldNameResolver{},
		testValueResolver{},
		storage.ApplyOptions{},
	)
	if !errors.Is(err, fusion.ErrStrategyDependenciesNotConfigured) {
		t.Fatalf("ApplyChangesWithOptions error = %v, want ErrStrategyDependenciesNotConfigured", err)
	}
	if redis.optionRuns != 0 {
		t.Fatalf("redis writes = %d, want 0 before incomplete fusion configuration is rejected", redis.optionRuns)
	}
}

func TestFusionStrategyRejectsTypedNilWriteDependencies(t *testing.T) {
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}

	t.Run("redis", func(t *testing.T) {
		var redis *recordingApplier
		strategy := fusion.Strategy{RedisApplier: redis, MongoQueue: &recordingQueue{}}

		err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{})
		if !errors.Is(err, fusion.ErrRedisNotConfigured) {
			t.Fatalf("ApplyChangesWithOptions error = %v, want ErrRedisNotConfigured", err)
		}
	})

	t.Run("queue", func(t *testing.T) {
		var queue *recordingQueue
		strategy := fusion.Strategy{RedisApplier: &recordingApplier{}, MongoQueue: queue}

		err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{})
		if !errors.Is(err, fusion.ErrMongoQueueNotConfigured) {
			t.Fatalf("ApplyChangesWithOptions error = %v, want ErrMongoQueueNotConfigured", err)
		}
	})
}

func TestFusionStrategyValidatesCompleteRuntimeDependencies(t *testing.T) {
	redis := &recordingRedisStore{}
	strategy := fusion.Strategy{
		RedisApplier:  redis,
		RedisLoader:   redis,
		RedisReplacer: &recordingDocumentReplacerWithOptions{},
		RedisLocker:   &recordingRecordLocker{},
		MongoLoader:   &recordingDocumentLoader{},
		MongoQueue:    &recordingQueue{},
	}
	validator, ok := any(strategy).(interface{ Validate() error })
	if !ok {
		t.Fatal("fusion strategy does not expose runtime dependency validation")
	}
	if err := validator.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestFusionStrategyDoesNotExposePhysicalDelete(t *testing.T) {
	if _, ok := any(fusion.Strategy{}).(storage.DeleteApplier); ok {
		t.Fatal("fusion strategy implements storage.DeleteApplier, want physical delete unavailable")
	}
}

func TestFusionStrategyRejectsMissingRedisLoader(t *testing.T) {
	strategy := fusion.Strategy{
		MongoLoader: &recordingDocumentLoader{doc: map[string]any{"name": "mongo"}, exists: true},
	}

	_, exists, err := strategy.LoadDocument(context.Background(), meta.TypeSpec{}, storage.RecordKey{ID: int64(1001)})
	if err == nil {
		t.Fatal("LoadDocument error = nil, want missing Redis loader error")
	}
	if exists {
		t.Fatal("LoadDocument exists = true, want no fallback read with incomplete Fusion runtime")
	}
}

func TestFusionStrategyRejectsMissingMongoLoaderAfterRedisMiss(t *testing.T) {
	strategy := fusion.Strategy{
		RedisLoader: &recordingDocumentLoader{exists: false},
	}

	_, exists, err := strategy.LoadDocument(context.Background(), meta.TypeSpec{}, storage.RecordKey{ID: int64(1001)})
	if err == nil {
		t.Fatal("LoadDocument error = nil, want missing Mongo loader error")
	}
	if exists {
		t.Fatal("LoadDocument exists = true, want no result with incomplete Fusion runtime")
	}
}

func TestFusionStrategyRejectsMissingMongoLoaderForBatchRedisMiss(t *testing.T) {
	strategy := fusion.Strategy{
		RedisLoader: &recordingDocumentLoader{existsMany: []bool{true, false}, docs: []map[string]any{{"name": "redis"}, nil}},
	}

	_, exists, err := strategy.LoadDocuments(context.Background(), meta.TypeSpec{}, []storage.RecordKey{{ID: int64(1001)}, {ID: int64(1002)}})
	if err == nil {
		t.Fatal("LoadDocuments error = nil, want missing Mongo loader error")
	}
	if exists != nil {
		t.Fatalf("LoadDocuments exists = %#v, want no partial result with incomplete Fusion runtime", exists)
	}
}

func TestFusionStrategyAppliesRedisBeforeQueue(t *testing.T) {
	queue := &recordingQueue{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); err != nil {
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

func TestFusionStrategyQueueFailureKeepsStoreChangesForRetry(t *testing.T) {
	queueErr := errors.New("delay queue unavailable")
	queue := &recordingQueue{err: queueErr}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, MongoQueue: queue}
	store := &testChangeStore{
		spec: meta.TypeSpec{
			Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
			Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
		},
		key:     storage.RecordKey{ID: int64(1001)},
		changes: recordedChanges{{Path: testStoragePath(2), Op: change.OpSet}},
	}

	if err := storage.Save(context.Background(), strategy, store); !errors.Is(err, queueErr) {
		t.Fatalf("first Save error = %v, want queue error", err)
	}
	if store.cleared != 0 || len(store.changes) != 1 {
		t.Fatalf("store after failed enqueue = cleared:%d changes:%#v, want retained change", store.cleared, store.changes)
	}
	if redis.optionRuns != 1 || len(queue.ops) != 1 {
		t.Fatalf("first save calls = redis:%d queue:%d, want 1/1", redis.optionRuns, len(queue.ops))
	}

	queue.err = nil
	if err := storage.Save(context.Background(), strategy, store); err != nil {
		t.Fatalf("retry Save returned error: %v", err)
	}
	if store.cleared != 1 || len(store.changes) != 0 {
		t.Fatalf("store after retry = cleared:%d changes:%#v, want cleared", store.cleared, store.changes)
	}
	if redis.optionRuns != 2 || len(queue.ops) != 2 {
		t.Fatalf("retry calls = redis:%d queue:%d, want 2/2", redis.optionRuns, len(queue.ops))
	}
}

func TestFusionStrategyPassesOptionsOnlyToRedis(t *testing.T) {
	queue := &recordingQueue{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
	}
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001), ShardKey: int64(7)}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{Upsert: true, TTLSeconds: 600})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.optionRuns != 1 {
		t.Fatalf("redis option calls = %d, want 1", redis.optionRuns)
	}
	if redis.options.TTLSeconds != 600 || !redis.options.Upsert {
		t.Fatalf("redis options = %#v, want TTL 600 and upsert", redis.options)
	}
	if redis.key != key {
		t.Fatalf("redis key = %#v, want %#v", redis.key, key)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v, want one save op", queue.ops)
	}
	if queue.ops[0].Key != key || queue.ops[0].RunAfter != time.Second {
		t.Fatalf("queued op = %#v, want record key and delay", queue.ops[0])
	}
}

func TestFusionStrategySkipsEmptyChanges(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{MongoQueue: queue}
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), meta.TypeSpec{}, key, recordedChanges{}, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(queue.ops) != 0 {
		t.Fatalf("queued ops = %#v, want empty", queue.ops)
	}
}

func TestFusionStrategyRejectsNilChangeSource(t *testing.T) {
	strategy := fusion.Strategy{}
	for _, source := range []change.Source{nil, (*typedNilSource)(nil)} {
		err := strategy.ApplyChangesWithOptions(
			context.Background(),
			meta.TypeSpec{},
			storage.RecordKey{},
			source,
			nil,
			nil,
			storage.ApplyOptions{},
		)
		if !errors.Is(err, change.ErrNilSource) {
			t.Fatalf("ApplyChangesWithOptions(%T) error = %v, want ErrNilSource", source, err)
		}
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
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}}
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

func TestFusionStrategyLoadDocumentRejectsInconsistentRedisLoaderResult(t *testing.T) {
	strategy := fusion.Strategy{
		RedisLoader: &recordingDocumentLoader{doc: map[string]any{"name": "unexpected"}},
		MongoLoader: &recordingDocumentLoader{},
	}

	_, _, err := strategy.LoadDocument(context.Background(), meta.TypeSpec{}, storage.RecordKey{ID: int64(1001)})
	if err == nil || !strings.Contains(err.Error(), "missing document with data") {
		t.Fatalf("LoadDocument error = %v, want inconsistent Redis loader result", err)
	}
}

func TestFusionStrategyLoadDocumentRejectsInconsistentMongoLoaderResult(t *testing.T) {
	strategy := fusion.Strategy{
		RedisLoader: &recordingDocumentLoader{},
		MongoLoader: &recordingDocumentLoader{doc: map[string]any{"name": "unexpected"}},
	}

	_, _, err := strategy.LoadDocumentWithOptions(
		context.Background(),
		meta.TypeSpec{},
		storage.RecordKey{ID: int64(1001)},
		fusion.LoadOptions{SkipRedisBackfill: true},
	)
	if err == nil || !strings.Contains(err.Error(), "missing document with data") {
		t.Fatalf("LoadDocumentWithOptions error = %v, want inconsistent Mongo loader result", err)
	}
}

func TestFusionStrategyRejectsRedisMissWithoutBackfillDependencies(t *testing.T) {
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	strategy := fusion.Strategy{RedisLoader: redis, MongoLoader: mongo}

	_, exists, err := strategy.LoadDocument(context.Background(), meta.TypeSpec{}, storage.RecordKey{ID: int64(1001)})
	if !errors.Is(err, fusion.ErrStrategyDependenciesNotConfigured) {
		t.Fatalf("LoadDocument error = %v, want ErrStrategyDependenciesNotConfigured", err)
	}
	if exists {
		t.Fatal("LoadDocument exists = true, want false when Redis backfill is unavailable")
	}
	if mongo.calls != 0 {
		t.Fatalf("mongo calls = %d, want 0 before backfill dependencies are checked", mongo.calls)
	}
}

func TestFusionStrategyLoadDocumentsRejectsInconsistentRedisBatchResult(t *testing.T) {
	strategy := fusion.Strategy{
		RedisLoader: &recordingDocumentLoader{
			docs:       []map[string]any{{"name": "unexpected"}},
			existsMany: []bool{false},
		},
		MongoLoader: &recordingDocumentLoader{},
	}

	_, _, err := strategy.LoadDocuments(context.Background(), meta.TypeSpec{}, []storage.RecordKey{{ID: int64(1001)}})
	if err == nil || !strings.Contains(err.Error(), "missing document with data") {
		t.Fatalf("LoadDocuments error = %v, want inconsistent Redis batch result", err)
	}
}

func TestFusionStrategyLoadDocumentBackfillsRedisFromFullMongoFallback(t *testing.T) {
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"roleId": int64(1001), "name": "cold", "power": int64(9)}, exists: true}
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "cold" {
		t.Fatalf("LoadDocument = %#v, %v; want Mongo document", doc, exists)
	}
	if redis.calls != 2 || mongo.calls != 1 {
		t.Fatalf("loader calls redis=%d mongo=%d, want redis double-check and mongo fallback", redis.calls, mongo.calls)
	}
	if locker.calls != 1 || locker.key.ID != key.ID {
		t.Fatalf("lock calls/key = %d/%#v, want one lock for key", locker.calls, locker.key)
	}
	if len(mongo.fields) != 0 {
		t.Fatalf("mongo fallback fields = %#v, want full document load", mongo.fields)
	}
	if replacer.calls != 1 || replacer.doc["power"] != int64(9) || replacer.options.TTLSeconds != int64(time.Hour/time.Second) {
		t.Fatalf("redis backfill calls/doc/options = %d/%#v/%#v", replacer.calls, replacer.doc, replacer.options)
	}
}

func TestFusionStrategyLoadDocumentReturnsRecordUnlockError(t *testing.T) {
	unlockErr := errors.New("unlock failed")
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	locker := &recordingRecordLocker{lock: &recordingRecordLock{unlockErr: unlockErr}}
	strategy := fusion.Strategy{
		RedisLoader:   redis,
		RedisReplacer: &recordingDocumentReplacerWithOptions{},
		RedisLocker:   locker,
		MongoLoader:   mongo,
	}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	_, _, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if !errors.Is(err, unlockErr) {
		t.Fatalf("LoadDocument error = %v, want unlock error", err)
	}
}

func TestFusionStrategyLoadDocumentRejectsNilRecordLock(t *testing.T) {
	strategy := fusion.Strategy{
		RedisLoader:   &recordingDocumentLoader{},
		RedisReplacer: &recordingDocumentReplacerWithOptions{},
		RedisLocker:   &recordingRecordLocker{nilLock: true},
		MongoLoader:   &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true},
	}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	_, _, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if err == nil || !strings.Contains(err.Error(), "nil record lock") {
		t.Fatalf("LoadDocument error = %v, want nil record lock error", err)
	}
}

func TestFusionStrategyProjectsCanonicalDocumentWithJSONFieldName(t *testing.T) {
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"roleId": int64(1001), "name": "cold", "power": int64(9)}, exists: true}
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type: meta.Type{
			Ref: meta.TypeRef{Name: "GDRole"},
			Fields: []meta.Field{
				{Name: "RoleId", JSONName: "roleId"},
				{Name: "Name", JSONName: "name"},
				{Name: "Power", JSONName: "power"},
			},
		},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "Name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || len(doc) != 1 || doc["name"] != "cold" {
		t.Fatalf("LoadDocument = %#v, want canonical JSON projection {name:cold}", doc)
	}
	if _, exists := doc["Name"]; exists {
		t.Fatalf("LoadDocument must not return Go field name: %#v", doc)
	}
}

func TestFusionStrategyLoadDocumentDoubleChecksRedisUnderLockBeforeMongoBackfill(t *testing.T) {
	redis := &recordingDocumentLoader{
		docs:       []map[string]any{nil, {"name": "hot-after-lock", "power": int64(8)}},
		existsMany: []bool{false, true},
	}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "hot-after-lock" {
		t.Fatalf("LoadDocument = %#v exists=%v, want Redis row from lock double-check", doc, exists)
	}
	if redis.calls != 2 {
		t.Fatalf("redis calls = %d, want initial read and lock double-check", redis.calls)
	}
	if mongo.calls != 0 || replacer.calls != 0 {
		t.Fatalf("mongo/replacer calls = %d/%d, want no Mongo load or backfill", mongo.calls, replacer.calls)
	}
	if locker.calls != 1 {
		t.Fatalf("lock calls = %d, want one", locker.calls)
	}
}

func TestFusionStrategyLoadDocumentWithNoBackfillOptionDoesNotBackfillRedis(t *testing.T) {
	redis := &recordingRedisStore{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	replacer := &recordingDocumentReplacerWithOptions{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisReplacer: replacer, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocumentWithOptions(context.Background(), spec, key, fusion.LoadOptions{SkipRedisBackfill: true}, "name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "cold" {
		t.Fatalf("LoadDocument = %#v, %v; want Mongo document", doc, exists)
	}
	if redis.recordingApplier.optionRuns != 0 {
		t.Fatalf("redis backfill ApplyChangesWithOptions calls = %d, want 0", redis.recordingApplier.optionRuns)
	}
	if replacer.calls != 0 {
		t.Fatalf("redis backfill ReplaceDocumentWithOptions calls = %d, want 0", replacer.calls)
	}
	if len(mongo.fields) != 1 || mongo.fields[0] != "name" {
		t.Fatalf("mongo no-backfill fields = %#v, want projection", mongo.fields)
	}
}

func TestFusionStrategyLoadDocumentRequiresDelayDeleteForRedisBackfill(t *testing.T) {
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}

	_, exists, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "name")
	if err == nil {
		t.Fatal("LoadDocument returned nil error, want missing delayDelTime error")
	}
	if exists {
		t.Fatal("LoadDocument exists = true, want false on missing delayDelTime")
	}
	if replacer.calls != 0 {
		t.Fatalf("redis backfill calls = %d, want 0", replacer.calls)
	}
}

func TestFusionStrategyRejectsBatchRedisMissWithoutBackfillDependencies(t *testing.T) {
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

	_, _, err := strategy.LoadDocuments(context.Background(), spec, keys, "name")
	if !errors.Is(err, fusion.ErrStrategyDependenciesNotConfigured) {
		t.Fatalf("LoadDocuments error = %v, want ErrStrategyDependenciesNotConfigured", err)
	}
	if redis.manyCalls != 1 {
		t.Fatalf("redis batch calls = %d, want 1", redis.manyCalls)
	}
	if mongo.manyCalls != 0 {
		t.Fatalf("mongo batch calls = %d, want 0 before backfill dependencies are checked", mongo.manyCalls)
	}
}

func TestFusionStrategyLoadDocumentsReturnsRecordUnlockError(t *testing.T) {
	unlockErr := errors.New("unlock failed")
	redis := &recordingDocumentLoader{docs: []map[string]any{nil}, existsMany: []bool{false}}
	mongo := &recordingDocumentLoader{docs: []map[string]any{{"name": "cold"}}, existsMany: []bool{true}}
	locker := &recordingRecordLocker{lock: &recordingRecordLock{unlockErr: unlockErr}}
	strategy := fusion.Strategy{
		RedisLoader:   redis,
		RedisReplacer: &recordingDocumentReplacerWithOptions{},
		RedisLocker:   locker,
		MongoLoader:   mongo,
	}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	_, _, err := strategy.LoadDocuments(context.Background(), spec, []storage.RecordKey{{ID: int64(1001)}})
	if !errors.Is(err, unlockErr) {
		t.Fatalf("LoadDocuments error = %v, want unlock error", err)
	}
}

func TestFusionStrategyLoadDocumentsBackfillsRedisFromFullMongoFallback(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}, {ID: int64(1002)}}
	redis := &recordingRedisStore{
		recordingDocumentLoader: recordingDocumentLoader{
			docs:       []map[string]any{nil, nil},
			existsMany: []bool{false, false},
		},
	}
	mongo := &recordingSingleDocumentLoader{
		docs: map[any]map[string]any{
			int64(1001): {"name": "cold", "power": int64(2)},
			int64(1002): {"name": "cold", "power": int64(2)},
		},
	}
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "cold" || !exists[1] || docs[1]["name"] != "cold" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want Mongo fallback rows", docs, exists)
	}
	if redis.recordingApplier.optionRuns != 0 {
		t.Fatalf("redis backfill ApplyChangesWithOptions calls = %d, want 0", redis.recordingApplier.optionRuns)
	}
	if mongo.calls != 2 {
		t.Fatalf("mongo single calls = %d, want protected single-record fallback", mongo.calls)
	}
	if len(mongo.fields) != 0 {
		t.Fatalf("mongo fallback fields = %#v, want full document load", mongo.fields)
	}
	if locker.calls != 2 || replacer.calls != 2 || replacer.options.TTLSeconds != int64(time.Hour/time.Second) {
		t.Fatalf("lock/backfill calls/options = %d/%d/%#v", locker.calls, replacer.calls, replacer.options)
	}
}

func TestFusionStrategyLoadDocumentsBatchBackfillsFromMongo(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}, {ID: int64(1002)}}
	redis := &recordingRedisStore{
		recordingDocumentLoader: recordingDocumentLoader{
			docs:       []map[string]any{nil, nil},
			existsMany: []bool{false, false},
		},
	}
	mongo := &recordingDocumentLoader{
		doc:        map[string]any{"name": "single-fallback"},
		exists:     true,
		docs:       []map[string]any{{"name": "cold-1", "power": int64(1)}, {"name": "cold-2", "power": int64(2)}},
		existsMany: []bool{true, true},
	}
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "cold-1" || !exists[1] || docs[1]["name"] != "cold-2" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want batch Mongo fallback rows", docs, exists)
	}
	if mongo.manyCalls != 1 || mongo.calls != 0 {
		t.Fatalf("mongo calls batch/single = %d/%d, want one full batch fallback", mongo.manyCalls, mongo.calls)
	}
	if len(mongo.fields) != 0 {
		t.Fatalf("mongo batch fallback fields = %#v, want full document load", mongo.fields)
	}
	if locker.calls != 2 || replacer.calls != 2 || replacer.options.TTLSeconds != int64(time.Hour/time.Second) {
		t.Fatalf("lock/backfill calls/options = %d/%d/%#v", locker.calls, replacer.calls, replacer.options)
	}
}

func TestFusionStrategyLoadDocumentsDoubleChecksRedisUnderLockBeforeMongoBackfill(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}}
	redis := &recordingDocumentLoader{
		doc:    map[string]any{"name": "hot-after-lock", "power": int64(8)},
		exists: true,
	}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "hot-after-lock" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want Redis row from lock double-check", docs, exists)
	}
	if redis.manyCalls != 1 || redis.calls != 1 {
		t.Fatalf("redis calls batch/single = %d/%d, want initial batch read and lock double-check", redis.manyCalls, redis.calls)
	}
	if locker.calls != 1 {
		t.Fatalf("lock calls = %d, want one", locker.calls)
	}
	if mongo.calls != 0 || mongo.manyCalls != 1 || replacer.calls != 0 {
		t.Fatalf("mongo/replacer calls = %d/%d/%d, want one candidate batch load and no backfill", mongo.calls, mongo.manyCalls, replacer.calls)
	}
}

func TestFusionStrategyLoadDocumentsWithNoBackfillOptionDoesNotBackfillRedis(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}, {ID: int64(1002)}}
	redis := &recordingRedisStore{
		recordingDocumentLoader: recordingDocumentLoader{
			docs:       []map[string]any{nil, nil},
			existsMany: []bool{false, false},
		},
	}
	mongo := &recordingDocumentLoader{
		docs:       []map[string]any{{"name": "cold-1"}, {"name": "cold-2"}},
		existsMany: []bool{true, true},
	}
	replacer := &recordingDocumentReplacerWithOptions{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisReplacer: replacer, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}}}

	docs, exists, err := strategy.LoadDocumentsWithOptions(context.Background(), spec, keys, fusion.LoadOptions{SkipRedisBackfill: true}, "name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "cold-1" || !exists[1] || docs[1]["name"] != "cold-2" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want Mongo fallback rows", docs, exists)
	}
	if replacer.calls != 0 {
		t.Fatalf("redis backfill ReplaceDocumentWithOptions calls = %d, want 0", replacer.calls)
	}
	if len(mongo.fields) != 1 || mongo.fields[0] != "name" {
		t.Fatalf("mongo no-backfill fields = %#v, want projection", mongo.fields)
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
	replacer := &recordingDocumentReplacerWithOptions{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisReplacer: replacer, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

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
	if redis.calls != 4 {
		t.Fatalf("redis single calls = %d, want 4 with lock double-check", redis.calls)
	}
	if len(mongo.keys) != 1 || mongo.keys[0].ID != int64(1002) {
		t.Fatalf("mongo fallback keys = %#v, want only missing key", mongo.keys)
	}
	if locker.calls != 1 || replacer.calls != 1 {
		t.Fatalf("backfill lock/replacer calls = %d/%d, want 1/1", locker.calls, replacer.calls)
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
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}}
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
