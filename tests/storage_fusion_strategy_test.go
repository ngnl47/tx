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
	ops  []fusion.DelayedSave
	err  error
	errs []error
}

type recordingDelaySubmitter struct {
	specs    []meta.TypeSpec
	keys     []storage.RecordKey
	onSubmit func()
}

func (s *recordingDelaySubmitter) Submit(spec meta.TypeSpec, key storage.RecordKey) {
	s.specs = append(s.specs, spec)
	s.keys = append(s.keys, key)
	if s.onSubmit != nil {
		s.onSubmit()
	}
}

func (q *recordingQueue) Enqueue(_ context.Context, op fusion.DelayedSave) error {
	q.ops = append(q.ops, op)
	if len(q.errs) > 0 {
		err := q.errs[0]
		q.errs = q.errs[1:]
		return err
	}
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

type recordingDocumentBackfiller struct {
	calls         int
	spec          meta.TypeSpec
	key           storage.RecordKey
	doc           map[string]any
	options       storage.ApplyOptions
	alreadyExists bool
	err           error
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

func fusionRoleType() meta.Type {
	return meta.Type{
		Ref: meta.TypeRef{Name: "GDRole"},
		Fields: []meta.Field{
			{Name: "RoleId", JSONName: "roleId"},
			{Name: "Name", JSONName: "name"},
			{Name: "Power", JSONName: "power"},
		},
	}
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

func (r *recordingDocumentBackfiller) BackfillDocumentWithOptions(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) (bool, error) {
	r.calls++
	r.spec = spec
	r.key = key
	r.doc = doc
	r.options = options
	if r.err != nil {
		return false, r.err
	}
	return !r.alreadyExists, nil
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

func TestFusionStrategyRegistersDelayedSaveAfterRedisWrite(t *testing.T) {
	submitter := &recordingDelaySubmitter{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: submitter}
	spec := meta.TypeSpec{
		Type: fusionRoleType(),
		Storage: meta.StorageSpec{
			DelaySave: meta.Duration{Duration: 600 * time.Second},
		},
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.optionRuns != 1 {
		t.Fatalf("redis runs = %d, want 1", redis.optionRuns)
	}
	if len(submitter.keys) != 1 || submitter.keys[0] != key {
		t.Fatalf("submitted keys = %#v, want %#v", submitter.keys, key)
	}
	if len(submitter.specs) != 1 || submitter.specs[0].Storage.DelaySave.Duration != 600*time.Second {
		t.Fatalf("submitted specs = %#v, want delaySave=600s", submitter.specs)
	}
}

func TestFusionStrategyUsesConfiguredSafetyTTLForDirtyRedisDocument(t *testing.T) {
	redis := &recordingApplier{}
	strategy := fusion.Strategy{
		RedisApplier:   redis,
		MongoLoader:    &recordingDocumentLoader{},
		DelaySubmitter: &recordingDelaySubmitter{},
		DirtyTTL:       7 * 24 * time.Hour,
	}
	spec := meta.TypeSpec{
		Type: fusionRoleType(),
		Storage: meta.StorageSpec{
			DelaySave:   meta.Duration{Duration: 10 * time.Minute},
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
		changes,
		testFieldNameResolver{},
		testValueResolver{},
		storage.ApplyOptions{TTL: 24 * time.Hour},
	)
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.options.TTL != 7*24*time.Hour {
		t.Fatalf("Redis TTL = %s, want seven-day safety TTL", redis.options.TTL)
	}
}

func TestFusionStrategyPreservesDirtyTTLMillisecondPrecision(t *testing.T) {
	redis := &recordingApplier{}
	strategy := fusion.Strategy{
		RedisApplier:   redis,
		MongoLoader:    &recordingDocumentLoader{},
		DelaySubmitter: &recordingDelaySubmitter{},
		DirtyTTL:       1500 * time.Millisecond,
	}
	spec := meta.TypeSpec{
		Type: fusionRoleType(),
		Storage: meta.StorageSpec{
			Mode:        meta.StorageModeFusion,
			DelaySave:   meta.Duration{Duration: time.Second},
			DelayDelete: meta.Duration{Duration: time.Second},
		},
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
		changes,
		testFieldNameResolver{},
		testValueResolver{},
		storage.ApplyOptions{},
	)
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.options.TTL != 1500*time.Millisecond {
		t.Fatalf("Redis TTL = %s, want 1500ms", redis.options.TTL)
	}
}

func TestFusionStrategyRejectsDirtyTTLBelowRedisMillisecondPrecision(t *testing.T) {
	redis := &recordingApplier{}
	strategy := fusion.Strategy{
		RedisApplier:   redis,
		MongoLoader:    &recordingDocumentLoader{},
		DelaySubmitter: &recordingDelaySubmitter{},
		DirtyTTL:       500 * time.Microsecond,
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(
		context.Background(),
		meta.TypeSpec{Type: fusionRoleType()},
		storage.RecordKey{ID: int64(1001)},
		changes,
		testFieldNameResolver{},
		testValueResolver{},
		storage.ApplyOptions{},
	)
	if err == nil || !strings.Contains(err.Error(), "at least 1ms") {
		t.Fatalf("ApplyChangesWithOptions error = %v, want dirty TTL precision error", err)
	}
	if redis.optionRuns != 0 {
		t.Fatalf("Redis calls = %d, want validation before I/O", redis.optionRuns)
	}
}

func TestFusionStrategyRejectsDirtyTTLNotGreaterThanTypeLifetime(t *testing.T) {
	redis := &recordingApplier{}
	strategy := fusion.Strategy{
		RedisApplier:   redis,
		MongoLoader:    &recordingDocumentLoader{},
		DelaySubmitter: &recordingDelaySubmitter{},
		DirtyTTL:       24 * time.Hour,
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{FullName: "data.GDRole"}},
		Storage: meta.StorageSpec{
			Mode:        meta.StorageModeFusion,
			DelaySave:   meta.Duration{Duration: 10 * time.Minute},
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}

	err := strategy.ApplyChangesWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
		changes,
		testFieldNameResolver{},
		testValueResolver{},
		storage.ApplyOptions{},
	)
	if err == nil || !strings.Contains(err.Error(), "delayDelTime") {
		t.Fatalf("ApplyChangesWithOptions error = %v, want dirty TTL lifetime error", err)
	}
	if redis.optionRuns != 0 {
		t.Fatalf("Redis calls = %d, want validation before I/O", redis.optionRuns)
	}
}

func TestFusionStrategyReturnsRedisTransportErrorWithoutEnqueue(t *testing.T) {
	transportErr := errors.New("redis unavailable")
	submitter := &recordingDelaySubmitter{}
	strategy := fusion.Strategy{RedisApplier: &recordingApplier{err: transportErr}, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: submitter}
	spec := meta.TypeSpec{Type: fusionRoleType()}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{})
	if !errors.Is(err, transportErr) {
		t.Fatalf("ApplyChangesWithOptions error = %v, want transport error", err)
	}
	if len(submitter.keys) != 0 {
		t.Fatalf("submitted keys = %#v, want no delayed task after Redis failure", submitter.keys)
	}
}

func TestFusionStrategyRejectsMissingWriteDependencies(t *testing.T) {
	spec := meta.TypeSpec{Type: fusionRoleType()}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{ID: int64(1001)}

	if err := (fusion.Strategy{DelaySubmitter: &recordingDelaySubmitter{}}).ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); !errors.Is(err, fusion.ErrRedisNotConfigured) {
		t.Fatalf("missing Redis error = %v, want ErrRedisNotConfigured", err)
	}
	if err := (fusion.Strategy{RedisApplier: &recordingApplier{}, MongoLoader: &recordingDocumentLoader{}}).ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); !errors.Is(err, fusion.ErrDelayTaskSubmitterNotConfigured) {
		t.Fatalf("missing DelaySubmitter error = %v, want ErrDelayTaskSubmitterNotConfigured", err)
	}
}

func TestFusionStrategyRejectsWriteWithoutMongoLoaderBeforeRedisWrite(t *testing.T) {
	redis := &recordingApplier{}
	strategy := fusion.Strategy{
		RedisApplier:   redis,
		DelaySubmitter: &recordingDelaySubmitter{},
	}
	spec := meta.TypeSpec{Type: fusionRoleType()}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
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

func TestFusionStrategyRejectsWriteWithoutDelaySubmitterBeforeRedisWrite(t *testing.T) {
	redis := &recordingApplier{}
	strategy := fusion.Strategy{
		RedisApplier: redis,
		MongoLoader:  &recordingDocumentLoader{},
	}
	spec := meta.TypeSpec{Type: fusionRoleType()}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(
		context.Background(),
		spec,
		storage.RecordKey{ID: int64(1001)},
		changes,
		testFieldNameResolver{},
		testValueResolver{},
		storage.ApplyOptions{},
	)
	if !errors.Is(err, fusion.ErrDelayTaskSubmitterNotConfigured) {
		t.Fatalf("ApplyChangesWithOptions error = %v, want ErrDelayTaskSubmitterNotConfigured", err)
	}
	if redis.optionRuns != 0 {
		t.Fatalf("redis writes = %d, want 0 before incomplete delay dispatcher configuration is rejected", redis.optionRuns)
	}
}

func TestFusionStrategyRejectsTypedNilWriteDependencies(t *testing.T) {
	spec := meta.TypeSpec{Type: fusionRoleType()}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{ID: int64(1001)}

	t.Run("redis", func(t *testing.T) {
		var redis *recordingApplier
		strategy := fusion.Strategy{RedisApplier: redis, DelaySubmitter: &recordingDelaySubmitter{}}

		err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{})
		if !errors.Is(err, fusion.ErrRedisNotConfigured) {
			t.Fatalf("ApplyChangesWithOptions error = %v, want ErrRedisNotConfigured", err)
		}
	})

	t.Run("submitter", func(t *testing.T) {
		var submitter *recordingDelaySubmitter
		strategy := fusion.Strategy{RedisApplier: &recordingApplier{}, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: submitter}

		err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{})
		if !errors.Is(err, fusion.ErrDelayTaskSubmitterNotConfigured) {
			t.Fatalf("ApplyChangesWithOptions error = %v, want ErrDelayTaskSubmitterNotConfigured", err)
		}
	})
}

func TestFusionStrategyValidatesCompleteRuntimeDependencies(t *testing.T) {
	redis := &recordingRedisStore{}
	strategy := fusion.Strategy{
		RedisApplier:    redis,
		RedisLoader:     redis,
		RedisBackfiller: &recordingDocumentBackfiller{},
		RedisLocker:     &recordingRecordLocker{},
		MongoLoader:     &recordingDocumentLoader{},
		DelaySubmitter:  &recordingDelaySubmitter{},
	}
	validator, ok := any(strategy).(interface{ Validate() error })
	if !ok {
		t.Fatal("fusion strategy does not expose runtime dependency validation")
	}
	if err := validator.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
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

func TestFusionStrategyAppliesRedisBeforeDelayRegistration(t *testing.T) {
	redis := &recordingApplier{}
	submitter := &recordingDelaySubmitter{onSubmit: func() {
		if redis.optionRuns != 1 {
			t.Fatalf("redis option calls at Submit = %d, want 1", redis.optionRuns)
		}
	}}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: submitter}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	key := storage.RecordKey{ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.optionRuns != 1 {
		t.Fatalf("redis option calls = %d, want 1", redis.optionRuns)
	}
	if redis.key.ID != int64(1001) {
		t.Fatalf("redis key = %#v", redis.key)
	}
	if len(submitter.keys) != 1 {
		t.Fatalf("submitted keys = %#v", submitter.keys)
	}
}

func TestFusionStrategySuccessfulHotWriteClearsStoreAfterLocalRegistration(t *testing.T) {
	submitter := &recordingDelaySubmitter{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: submitter}
	store := &testChangeStore{
		spec: meta.TypeSpec{
			Type:    fusionRoleType(),
			Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
		},
		key:     storage.RecordKey{ID: int64(1001)},
		changes: recordedChanges{{Path: testStoragePath(2), Op: change.OpSet}},
	}

	if err := storage.Save(context.Background(), strategy, store); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if store.cleared != 1 || len(store.changes) != 0 {
		t.Fatalf("store after Save = cleared:%d changes:%#v, want cleared", store.cleared, store.changes)
	}
	if redis.optionRuns != 1 || len(submitter.keys) != 1 {
		t.Fatalf("save calls = redis:%d submit:%d, want 1/1", redis.optionRuns, len(submitter.keys))
	}
}

func TestFusionStrategyScopesWriteOptionsToRedisAndOverridesBusinessTTL(t *testing.T) {
	submitter := &recordingDelaySubmitter{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: submitter, DirtyTTL: 30 * 24 * time.Hour}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
	}
	key := storage.RecordKey{ID: int64(1001), ShardKey: int64(7)}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}

	err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, changes, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{Upsert: true, TTL: 10 * time.Minute})
	if err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if redis.optionRuns != 1 {
		t.Fatalf("redis option calls = %d, want 1", redis.optionRuns)
	}
	if redis.options.TTL != 30*24*time.Hour || !redis.options.Upsert {
		t.Fatalf("redis options = %#v, want configured dirty TTL and upsert", redis.options)
	}
	if redis.key != key {
		t.Fatalf("redis key = %#v, want %#v", redis.key, key)
	}
	if len(submitter.keys) != 1 || submitter.keys[0] != key {
		t.Fatalf("submitted keys = %#v, want %#v", submitter.keys, key)
	}
}

func TestFusionStrategySkipsEmptyChanges(t *testing.T) {
	submitter := &recordingDelaySubmitter{}
	strategy := fusion.Strategy{DelaySubmitter: submitter}
	key := storage.RecordKey{ID: int64(1001)}

	if err := strategy.ApplyChangesWithOptions(context.Background(), meta.TypeSpec{}, key, recordedChanges{}, testFieldNameResolver{}, testValueResolver{}, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(submitter.keys) != 0 {
		t.Fatalf("submitted keys = %#v, want empty", submitter.keys)
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
	spec := meta.TypeSpec{Type: fusionRoleType()}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "Name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "hot" {
		t.Fatalf("LoadDocument = %#v, %v; want Redis document", doc, exists)
	}
	if redis.calls != 1 || mongo.calls != 0 {
		t.Fatalf("loader calls redis=%d mongo=%d, want redis only", redis.calls, mongo.calls)
	}
	if len(redis.fields) != 1 || redis.fields[0] != "Name" {
		t.Fatalf("redis fields = %#v, want Name", redis.fields)
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
	spec := meta.TypeSpec{Type: fusionRoleType()}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "Name")
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
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "Name")
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
	if backfiller.calls != 1 || backfiller.doc["power"] != int64(9) || backfiller.options.TTL != time.Hour {
		t.Fatalf("redis backfill calls/doc/options = %d/%#v/%#v", backfiller.calls, backfiller.doc, backfiller.options)
	}
}

func TestFusionStrategyLoadDocumentReturnsConcurrentRedisWriteWhenBackfillLosesRace(t *testing.T) {
	redis := &recordingDocumentLoader{
		docs: []map[string]any{
			nil,
			nil,
			{"name": "hot"},
		},
		existsMany: []bool{false, false, true},
	}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold", "power": int64(9)}, exists: true}
	backfiller := &recordingDocumentBackfiller{alreadyExists: true}
	strategy := fusion.Strategy{
		RedisLoader:     redis,
		RedisBackfiller: backfiller,
		RedisLocker:     &recordingRecordLocker{},
		MongoLoader:     mongo,
	}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "Name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || len(doc) != 1 || doc["name"] != "hot" {
		t.Fatalf("LoadDocument = %#v exists=%v, want concurrent Redis document", doc, exists)
	}
	if redis.calls != 3 || mongo.calls != 1 || backfiller.calls != 1 {
		t.Fatalf("redis/mongo/backfill calls = %d/%d/%d, want 3/1/1", redis.calls, mongo.calls, backfiller.calls)
	}
}

func TestFusionStrategyLoadDocumentReturnsRecordUnlockError(t *testing.T) {
	unlockErr := errors.New("unlock failed")
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	locker := &recordingRecordLocker{lock: &recordingRecordLock{unlockErr: unlockErr}}
	strategy := fusion.Strategy{
		RedisLoader:     redis,
		RedisBackfiller: &recordingDocumentBackfiller{},
		RedisLocker:     locker,
		MongoLoader:     mongo,
	}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	_, _, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)})
	if !errors.Is(err, unlockErr) {
		t.Fatalf("LoadDocument error = %v, want unlock error", err)
	}
}

func TestFusionStrategyLoadDocumentRejectsNilRecordLock(t *testing.T) {
	strategy := fusion.Strategy{
		RedisLoader:     &recordingDocumentLoader{},
		RedisBackfiller: &recordingDocumentBackfiller{},
		RedisLocker:     &recordingRecordLocker{nilLock: true},
		MongoLoader:     &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true},
	}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
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
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
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

func TestFusionStrategyRejectsUnknownProjectionBeforeLoad(t *testing.T) {
	redis := &recordingDocumentLoader{doc: map[string]any{"power": int64(9)}, exists: true}
	mongo := &recordingDocumentLoader{}
	strategy := fusion.Strategy{RedisLoader: redis, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: meta.Type{
		Ref:    meta.TypeRef{Name: "GDRole"},
		Fields: []meta.Field{{Name: "RoleId"}, {Name: "Power"}},
	}}

	_, _, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "Pwoer")
	if !errors.Is(err, storage.ErrUnknownField) {
		t.Fatalf("LoadDocument error = %v, want ErrUnknownField", err)
	}
	if redis.calls != 0 || mongo.calls != 0 {
		t.Fatalf("redis/mongo calls = %d/%d, want 0/0", redis.calls, mongo.calls)
	}
}

func TestFusionStrategyLoadDocumentDoubleChecksRedisUnderLockBeforeMongoBackfill(t *testing.T) {
	redis := &recordingDocumentLoader{
		docs:       []map[string]any{nil, {"name": "hot-after-lock", "power": int64(8)}},
		existsMany: []bool{false, true},
	}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, key, "Name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "hot-after-lock" {
		t.Fatalf("LoadDocument = %#v exists=%v, want Redis row from lock double-check", doc, exists)
	}
	if redis.calls != 2 {
		t.Fatalf("redis calls = %d, want initial read and lock double-check", redis.calls)
	}
	if mongo.calls != 0 || backfiller.calls != 0 {
		t.Fatalf("mongo/backfiller calls = %d/%d, want no Mongo load or backfill", mongo.calls, backfiller.calls)
	}
	if locker.calls != 1 {
		t.Fatalf("lock calls = %d, want one", locker.calls)
	}
}

func TestFusionStrategyLoadDocumentWithNoBackfillOptionDoesNotBackfillRedis(t *testing.T) {
	redis := &recordingRedisStore{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	backfiller := &recordingDocumentBackfiller{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisBackfiller: backfiller, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: fusionRoleType()}
	key := storage.RecordKey{ID: int64(1001)}

	doc, exists, err := strategy.LoadDocumentWithOptions(context.Background(), spec, key, fusion.LoadOptions{SkipRedisBackfill: true}, "Name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "cold" {
		t.Fatalf("LoadDocument = %#v, %v; want Mongo document", doc, exists)
	}
	if redis.recordingApplier.optionRuns != 0 {
		t.Fatalf("redis backfill ApplyChangesWithOptions calls = %d, want 0", redis.recordingApplier.optionRuns)
	}
	if backfiller.calls != 0 {
		t.Fatalf("redis backfill calls = %d, want 0", backfiller.calls)
	}
	if len(mongo.fields) != 1 || mongo.fields[0] != "Name" {
		t.Fatalf("mongo no-backfill fields = %#v, want projection", mongo.fields)
	}
}

func TestFusionStrategyLoadDocumentUsesDirtyTTLWhenDelayDeleteIsNotConfigured(t *testing.T) {
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: fusionRoleType()}

	doc, exists, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "Name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists || doc["name"] != "cold" {
		t.Fatalf("LoadDocument = %#v exists=%v, want Mongo document", doc, exists)
	}
	if backfiller.calls != 1 || backfiller.options.TTL != fusion.DefaultDirtyTTL {
		t.Fatalf("redis backfill calls/options = %d/%#v, want default dirty TTL", backfiller.calls, backfiller.options)
	}
}

func TestFusionStrategyPreservesBackfillTTLMillisecondPrecision(t *testing.T) {
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type: fusionRoleType(),
		Storage: meta.StorageSpec{
			DelayDelete: meta.Duration{Duration: 1500 * time.Millisecond},
		},
	}

	_, exists, err := strategy.LoadDocument(context.Background(), spec, storage.RecordKey{ID: int64(1001)}, "Name")
	if err != nil {
		t.Fatalf("LoadDocument returned error: %v", err)
	}
	if !exists {
		t.Fatal("LoadDocument exists = false, want Mongo document")
	}
	if backfiller.calls != 1 || backfiller.options.TTL != 1500*time.Millisecond {
		t.Fatalf("redis backfill calls/options = %d/%#v, want 1500ms", backfiller.calls, backfiller.options)
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
	spec := meta.TypeSpec{Type: fusionRoleType()}

	_, _, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
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
		RedisLoader:     redis,
		RedisBackfiller: &recordingDocumentBackfiller{},
		RedisLocker:     locker,
		MongoLoader:     mongo,
	}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
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
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
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
	if locker.calls != 2 || backfiller.calls != 2 || backfiller.options.TTL != time.Hour {
		t.Fatalf("lock/backfill calls/options = %d/%d/%#v", locker.calls, backfiller.calls, backfiller.options)
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
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "cold-1" || !exists[1] || docs[1]["name"] != "cold-2" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want batch Mongo fallback rows", docs, exists)
	}
	if mongo.manyCalls != 0 || mongo.calls != 2 {
		t.Fatalf("mongo calls batch/single = %d/%d, want protected per-record reads", mongo.manyCalls, mongo.calls)
	}
	if len(mongo.fields) != 0 {
		t.Fatalf("mongo batch fallback fields = %#v, want full document load", mongo.fields)
	}
	if locker.calls != 2 || backfiller.calls != 2 || backfiller.options.TTL != time.Hour {
		t.Fatalf("lock/backfill calls/options = %d/%d/%#v", locker.calls, backfiller.calls, backfiller.options)
	}
}

func TestFusionStrategyLoadDocumentsReturnsConcurrentRedisWriteWhenBackfillLosesRace(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}}
	redis := &recordingRedisStore{
		recordingDocumentLoader: recordingDocumentLoader{
			docs:       []map[string]any{nil, {"name": "hot"}},
			existsMany: []bool{false, true},
		},
	}
	mongo := &recordingDocumentLoader{
		docs:       []map[string]any{{"name": "cold"}},
		existsMany: []bool{true},
	}
	backfiller := &recordingDocumentBackfiller{alreadyExists: true}
	strategy := fusion.Strategy{
		RedisApplier:    redis,
		RedisBackfiller: backfiller,
		RedisLocker:     &recordingRecordLocker{},
		MongoLoader:     mongo,
	}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "hot" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want concurrent Redis document", docs, exists)
	}
	if redis.recordingDocumentLoader.calls != 2 || backfiller.calls != 1 {
		t.Fatalf("redis single/backfill calls = %d/%d, want 2/1", redis.recordingDocumentLoader.calls, backfiller.calls)
	}
}

func TestFusionStrategyLoadDocumentsRechecksRedisAfterMongoMiss(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}}
	redis := &recordingRedisStore{
		recordingDocumentLoader: recordingDocumentLoader{
			docs:       []map[string]any{nil, {"name": "hot"}},
			existsMany: []bool{false, true},
		},
	}
	mongo := &recordingDocumentLoader{
		docs:       []map[string]any{nil},
		existsMany: []bool{false},
	}
	strategy := fusion.Strategy{
		RedisApplier:    redis,
		RedisBackfiller: &recordingDocumentBackfiller{},
		RedisLocker:     &recordingRecordLocker{},
		MongoLoader:     mongo,
	}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "hot" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want Redis document written during Mongo miss", docs, exists)
	}
}

func TestFusionStrategyLoadDocumentsDoubleChecksRedisUnderLockBeforeMongoBackfill(t *testing.T) {
	keys := []storage.RecordKey{{ID: int64(1001)}}
	redis := &recordingDocumentLoader{
		doc:    map[string]any{"name": "hot-after-lock", "power": int64(8)},
		exists: true,
	}
	mongo := &recordingDocumentLoader{doc: map[string]any{"name": "cold"}, exists: true}
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
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
	if mongo.calls != 0 || mongo.manyCalls != 0 || backfiller.calls != 0 {
		t.Fatalf("mongo/backfiller calls = %d/%d/%d, want no cold read after a concurrent hot write", mongo.calls, mongo.manyCalls, backfiller.calls)
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
	backfiller := &recordingDocumentBackfiller{}
	strategy := fusion.Strategy{RedisApplier: redis, RedisBackfiller: backfiller, MongoLoader: mongo}
	spec := meta.TypeSpec{Type: fusionRoleType()}

	docs, exists, err := strategy.LoadDocumentsWithOptions(context.Background(), spec, keys, fusion.LoadOptions{SkipRedisBackfill: true}, "Name")
	if err != nil {
		t.Fatalf("LoadDocuments returned error: %v", err)
	}
	if !exists[0] || docs[0]["name"] != "cold-1" || !exists[1] || docs[1]["name"] != "cold-2" {
		t.Fatalf("LoadDocuments = %#v exists=%#v, want Mongo fallback rows", docs, exists)
	}
	if backfiller.calls != 0 {
		t.Fatalf("redis backfill calls = %d, want 0", backfiller.calls)
	}
	if len(mongo.fields) != 1 || mongo.fields[0] != "Name" {
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
	backfiller := &recordingDocumentBackfiller{}
	locker := &recordingRecordLocker{}
	strategy := fusion.Strategy{RedisLoader: redis, RedisBackfiller: backfiller, RedisLocker: locker, MongoLoader: mongo}
	spec := meta.TypeSpec{
		Type:    fusionRoleType(),
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
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
	if mongo.calls != 1 || mongo.key.ID != int64(1002) {
		t.Fatalf("mongo fallback key = %#v, want only missing key", mongo.key)
	}
	if locker.calls != 1 || backfiller.calls != 1 {
		t.Fatalf("backfill lock/writer calls = %d/%d, want 1/1", locker.calls, backfiller.calls)
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
	spec := meta.TypeSpec{Type: fusionRoleType()}

	docs, exists, err := strategy.LoadDocuments(context.Background(), spec, keys, "Name")
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

func TestFusionCreationNeverOverwritesExistingData(t *testing.T) {
	for _, where := range []string{"missing", "mongo", "redis"} {
		t.Run(where, func(t *testing.T) {
			cold := &recordingDocumentLoader{}
			if where == "mongo" {
				cold.exists = true
				cold.doc = map[string]any{"roleId": int64(9)}
			}
			hot := &recordingDocumentBackfiller{alreadyExists: where == "redis"}
			lock := &recordingRecordLock{}
			submit := &recordingDelaySubmitter{}
			strategy := fusion.Strategy{RedisApplier: &recordingApplier{}, RedisLoader: &recordingDocumentLoader{}, RedisBackfiller: hot, RedisLocker: &recordingRecordLocker{lock: lock}, MongoLoader: cold, DelaySubmitter: submit}
			spec := meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{Mode: meta.StorageModeFusion}}
			inserted, err := strategy.BackfillDocumentWithOptions(t.Context(), spec, storage.RecordKey{ID: int64(9)}, map[string]any{"roleId": int64(9), "name": "created"}, storage.ApplyOptions{})
			if err != nil || inserted != (where == "missing") {
				t.Fatalf("inserted=%v err=%v", inserted, err)
			}
			if lock.unlocks != 1 {
				t.Fatal("record lock leaked")
			}
			if where == "mongo" && hot.calls != 0 {
				t.Fatal("cold record overwritten in Redis")
			}
			if (len(submit.keys) == 1) != (where == "missing") {
				t.Fatal("wrong delayed write registration")
			}
		})
	}
}

type cancellationAwareRecordLock struct{ unlocks int }

func (l *cancellationAwareRecordLock) Unlock(ctx context.Context) error {
	l.unlocks++
	return ctx.Err()
}
func TestFusionCreationConfirmedInsertSurvivesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lock := &cancellationAwareRecordLock{}
	strategy := fusion.Strategy{RedisApplier: &recordingApplier{}, RedisLoader: &recordingDocumentLoader{}, RedisBackfiller: &recordingDocumentBackfiller{}, RedisLocker: &recordingRecordLocker{lock: lock}, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: &recordingDelaySubmitter{onSubmit: cancel}}
	inserted, err := strategy.BackfillDocumentWithOptions(ctx, meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{Mode: meta.StorageModeFusion}}, storage.RecordKey{ID: int64(1)}, map[string]any{"roleId": int64(1)}, storage.ApplyOptions{})
	if err != nil || !inserted || lock.unlocks != 1 {
		t.Fatalf("confirmed insert became failed: inserted=%v err=%v unlocks=%d", inserted, err, lock.unlocks)
	}
}

func TestFusionCreationConfirmedInsertSurvivesUnlockFailure(t *testing.T) {
	lock := &recordingRecordLock{unlockErr: errors.New("unlock transport failed")}
	strategy := fusion.Strategy{RedisApplier: &recordingApplier{}, RedisLoader: &recordingDocumentLoader{}, RedisBackfiller: &recordingDocumentBackfiller{}, RedisLocker: &recordingRecordLocker{lock: lock}, MongoLoader: &recordingDocumentLoader{}, DelaySubmitter: &recordingDelaySubmitter{}}
	inserted, err := strategy.BackfillDocumentWithOptions(t.Context(), meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{Mode: meta.StorageModeFusion}}, storage.RecordKey{ID: int64(1)}, map[string]any{"roleId": int64(1)}, storage.ApplyOptions{})
	if err != nil || !inserted || lock.unlocks != 1 {
		t.Fatalf("cleanup changed confirmed insert: inserted=%v err=%v", inserted, err)
	}
}
