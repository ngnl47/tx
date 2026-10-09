package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/txn"
	txstorage "github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/task"
)

type recordingLoader struct{ calls int }

func (loader *recordingLoader) LoadDocument(context.Context, meta.TypeSpec, txstorage.RecordKey, ...string) (map[string]any, bool, error) {
	loader.calls++
	return map[string]any{"name": "cached"}, true, nil
}

type recordingApplier struct{ calls int }

// Mongo 更新必须走版本条件入口，不能退回无条件写入；创建仍走独立路径。
func TestMongoWriterUsesVersionedUpdatesAndKeepsCreationSeparate(t *testing.T) {
	backend := &versionedRecordingApplier{}
	runtime := &Runtime{mongo: &txmongo.Cluster{}}
	writer := storageChangeWriter{storage: runtime, mongo: backend}
	spec := meta.TypeSpec{Storage: meta.StorageSpec{Mode: meta.StorageModeMongo}}
	ctx := context.Background()
	if err := writer.ApplyChangesWithOptions(ctx, spec, txstorage.RecordKey{ID: int64(1)}, staticSource{}, nil, nil, txstorage.ApplyOptions{}); err != nil {
		t.Fatal(err)
	}
	if backend.versioned != 1 || backend.calls != 0 {
		t.Fatalf("update: versioned=%d plain=%d", backend.versioned, backend.calls)
	}
	if err := writer.ApplyChangesWithOptions(ctx, spec, txstorage.RecordKey{ID: int64(1)}, staticSource{}, nil, nil, txstorage.ApplyOptions{Upsert: true}); err != nil {
		t.Fatal(err)
	}
	if backend.versioned != 1 || backend.calls != 1 {
		t.Fatalf("create: versioned=%d plain=%d", backend.versioned, backend.calls)
	}
	runtime.markBackendUnavailable(txstorage.BackendMongo, errors.New("offline"))
	if err := writer.ApplyChangesWithOptions(ctx, spec, txstorage.RecordKey{ID: int64(1)}, staticSource{}, nil, nil, txstorage.ApplyOptions{}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("gate = %v", err)
	}
	if backend.versioned != 1 {
		t.Fatal("closed gate reached Mongo")
	}
}

type versionedRecordingApplier struct {
	recordingApplier
	versioned int
}

func (applier *versionedRecordingApplier) ApplyVersionedChanges(context.Context, meta.TypeSpec, txstorage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver) error {
	applier.versioned++
	return nil
}

func (applier *recordingApplier) ApplyChangesWithOptions(context.Context, meta.TypeSpec, txstorage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, txstorage.ApplyOptions) error {
	applier.calls++
	return nil
}

type staticSource struct{}

func (staticSource) VisitChanges(visitor change.Visitor) error {
	path := change.Path{Depth: 1}
	path.Nodes[0].Field = 1
	return visitor.VisitChange(path, change.OpSet)
}

func (staticSource) HasChanges() bool { return true }

type admissionStore struct {
	txstorage.ChangeStore
	mode meta.StorageMode
}

func (*admissionStore) SetUndoRecorder(change.UndoRecorder) {}

func (store *admissionStore) TypeSpec() meta.TypeSpec {
	return meta.TypeSpec{Storage: meta.StorageSpec{Mode: store.mode}}
}

func admissionTarget(scope string, mode meta.StorageMode) txn.LockTarget {
	return txn.NewLockTarget(txn.NewLockKey(scope, 1001), &admissionStore{mode: mode})
}

func TestStorageWriteAdmissionChecksTargetStorageModes(t *testing.T) {
	storage := &Runtime{redis: &txredis.Stores{}, mongo: &txmongo.Cluster{}}
	storage.markBackendUnavailable(txstorage.BackendMongo, errors.New("mongo unavailable"))
	admission := WriteAdmission{storage: storage}

	if err := admission.RequireWritable([]txn.LockTarget{
		admissionTarget("redis-role", meta.StorageModeRedis),
	}); err != nil {
		t.Fatalf("Redis-only target rejected Mongo outage: %v", err)
	}
	if err := admission.RequireWritable([]txn.LockTarget{
		admissionTarget("mongo-world", meta.StorageModeMongo),
	}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Mongo target error = %v, want ErrStorageUnavailable", err)
	}
	if err := admission.RequireWritable([]txn.LockTarget{
		admissionTarget("fusion-role", meta.StorageModeFusion),
	}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Fusion target error = %v, want ErrStorageUnavailable", err)
	}
}

func TestStorageWriteAdmissionDoesNotRequireUnrelatedBackend(t *testing.T) {
	redisOnly := WriteAdmission{storage: &Runtime{redis: &txredis.Stores{}}}
	if err := redisOnly.RequireWritable([]txn.LockTarget{
		admissionTarget("redis-role", meta.StorageModeRedis),
	}); err != nil {
		t.Fatalf("Redis-only target required Mongo: %v", err)
	}

	mongoOnly := WriteAdmission{storage: &Runtime{mongo: &txmongo.Cluster{}}}
	if err := mongoOnly.RequireWritable([]txn.LockTarget{
		admissionTarget("mongo-world", meta.StorageModeMongo),
	}); err != nil {
		t.Fatalf("Mongo-only target required Redis: %v", err)
	}
}

func TestStorageWriteAdmissionRejectsMissingTargetBackend(t *testing.T) {
	admission := WriteAdmission{storage: &Runtime{}}
	if err := admission.RequireWritable([]txn.LockTarget{
		admissionTarget("redis-role", meta.StorageModeRedis),
	}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("missing Redis target error = %v, want ErrStorageUnavailable", err)
	}
}

func TestFusionHealthGateRejectsBeforeRedisReadWhenMongoIsUnavailable(t *testing.T) {
	store := &Runtime{}
	store.markBackendUnavailable(txstorage.BackendMongo, errors.New("mongo unavailable"))
	loader := &recordingLoader{}
	_, _, err := store.guardReader(meta.StorageModeFusion, loader).LoadDocument(context.Background(), meta.TypeSpec{}, txstorage.RecordKey{ID: int64(1001)})
	if !errors.Is(err, ErrStorageUnavailable) || loader.calls != 0 {
		t.Fatalf("fusion read error/calls = %v/%d, want unavailable/0", err, loader.calls)
	}
}

func TestMongoAtomicCapabilitiesUseMongoHealthGate(t *testing.T) {
	store := &Runtime{mongo: &txmongo.Cluster{}}
	backfiller := store.MongoBackfiller()
	sequences := store.MongoCounters()
	if backfiller == nil || sequences == nil {
		t.Fatal("Mongo atomic capabilities are nil for configured Mongo storage")
	}
	store.markBackendUnavailable(txstorage.BackendMongo, errors.New("mongo unavailable"))

	if _, _, err := store.FindDocument(context.Background(), meta.TypeSpec{}, int64(1), txmongo.Filter{Field: "platform", Value: "test"}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Mongo query error = %v, want ErrStorageUnavailable", err)
	}
	_, err := backfiller.BackfillDocumentWithOptions(
		context.Background(),
		meta.TypeSpec{},
		txstorage.RecordKey{ID: int64(1001)},
		map[string]any{},
		txstorage.ApplyOptions{},
	)
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Mongo backfill error = %v, want ErrStorageUnavailable", err)
	}
	if _, err := sequences.Reserve(context.Background(), "global", int64(1), "custom_counters", "example", 100); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Mongo sequence error = %v, want ErrStorageUnavailable", err)
	}
}

func TestStorageHealthGateIsModeAware(t *testing.T) {
	store := &Runtime{}
	store.markBackendUnavailable(txstorage.BackendMongo, errors.New("mongo unavailable"))
	loader := &recordingLoader{}
	if _, _, err := store.guardReader(meta.StorageModeRedis, loader).LoadDocument(context.Background(), meta.TypeSpec{}, txstorage.RecordKey{ID: int64(1001)}); err != nil {
		t.Fatalf("redis reader rejected mongo outage: %v", err)
	}
	redisWriter := &recordingApplier{}
	writer := storageChangeWriter{storage: store, redis: redisWriter}
	if err := writer.ApplyChangesWithOptions(context.Background(), meta.TypeSpec{Storage: meta.StorageSpec{Mode: meta.StorageModeRedis}}, txstorage.RecordKey{}, staticSource{}, nil, nil, txstorage.ApplyOptions{}); err != nil {
		t.Fatalf("redis writer rejected mongo outage: %v", err)
	}
	if redisWriter.calls != 1 {
		t.Fatalf("redis writer calls = %d, want 1", redisWriter.calls)
	}
}

func TestOlderProbeCannotClearNewBackendFailure(t *testing.T) {
	store := &Runtime{}
	generation := store.backendHealthGeneration(txstorage.BackendRedis)
	store.markBackendUnavailable(txstorage.BackendRedis, errors.New("new redis failure"))
	store.recoverBackendHealth(txstorage.BackendRedis, generation)
	if err := store.requireStorageMode(meta.StorageModeRedis); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("stale recovery reopened Redis gate: %v", err)
	}
}

func TestOnlyBackendErrorsCloseStorageHealthGate(t *testing.T) {
	store := &Runtime{}
	store.observeStorageError(context.Canceled)
	store.observeStorageError(txstorage.ErrNoEffectiveChanges)
	if err := store.requireStorageMode(meta.StorageModeFusion); err != nil {
		t.Fatalf("request error closed health gate: %v", err)
	}
	store.observeStorageError(txstorage.NewBackendError(txstorage.BackendRedis, "eval", errors.New("connection refused")))
	if err := store.requireStorageMode(meta.StorageModeFusion); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("backend error did not close health gate: %v", err)
	}
}

func TestDelayHandlerReportsBackendFailureForRetry(t *testing.T) {
	store := &Runtime{}
	handler := healthAwareDelayHandler{
		storage: store,
		next: fusion.DelayTaskHandlerFunc(func(context.Context, task.DelayTask) error {
			return txstorage.NewBackendError(txstorage.BackendMongo, "replace", errors.New("connection refused"))
		}),
	}
	if err := handler.HandleDelayTask(context.Background(), task.DelayTask{Type: "role", ID: int64(1001), Action: "save"}); err == nil {
		t.Fatal("HandleDelayTask error = nil, want retryable backend error")
	}
	if err := store.requireStorageMode(meta.StorageModeFusion); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("backend error did not close Fusion gate: %v", err)
	}
}
