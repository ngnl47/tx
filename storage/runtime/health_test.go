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
	"github.com/ngnl47/tx/wal"
)

type recordingLoader struct{ calls int }

func (loader *recordingLoader) LoadDocument(context.Context, meta.TypeSpec, txstorage.RecordKey, ...string) (map[string]any, bool, error) {
	loader.calls++
	return map[string]any{"name": "cached"}, true, nil
}

type recordingApplier struct{ calls int }

func (applier *recordingApplier) ApplyChangesWithOptions(context.Context, meta.TypeSpec, txstorage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, txstorage.ApplyOptions) error {
	applier.calls++
	return nil
}

type recordingFencedApplier struct {
	recordingApplier
	fence txmongo.Fence
}

func (applier *recordingFencedApplier) ApplyChangesWithFence(_ context.Context, _ meta.TypeSpec, _ txstorage.RecordKey, _ change.Source, _ change.FieldNameResolver, _ change.ValueResolver, fence txmongo.Fence, _ txstorage.ApplyOptions) error {
	applier.calls++
	applier.fence = fence
	return nil
}

type recordingWAL struct {
	calls int
	err   error
}

func (store *recordingWAL) Append(context.Context, wal.Record) error {
	store.calls++
	return store.err
}

func (store *recordingWAL) Read(context.Context, wal.Stream, uint64, int) ([]wal.Record, error) {
	store.calls++
	return nil, store.err
}

func (store *recordingWAL) Replay(context.Context, wal.Stream, uint64, func(context.Context, wal.Record) error) (uint64, error) {
	store.calls++
	return 0, store.err
}

func (store *recordingWAL) Checkpoint(context.Context, wal.Stream, uint64, func(context.Context) error) error {
	store.calls++
	return store.err
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
	storage := &Runtime{redis: &txredis.Cluster{}, mongo: &txmongo.Cluster{}}
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
	redisOnly := WriteAdmission{storage: &Runtime{redis: &txredis.Cluster{}}}
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

func TestStorageChangeWriterForwardsMongoFence(t *testing.T) {
	store := &Runtime{}
	mongoWriter := &recordingFencedApplier{}
	writer := storageChangeWriter{storage: store, mongo: mongoWriter}
	path := change.Path{Depth: 1}
	path.Nodes[0].Field = 1
	fence := txmongo.Fence{Path: path, Expected: uint64(3)}

	err := writer.ApplyChangesWithFence(
		context.Background(),
		meta.TypeSpec{Storage: meta.StorageSpec{Mode: meta.StorageModeMongo}},
		txstorage.RecordKey{ID: "world-1/7"},
		staticSource{},
		nil,
		nil,
		fence,
		txstorage.ApplyOptions{},
	)
	if err != nil {
		t.Fatalf("ApplyChangesWithFence returned error: %v", err)
	}
	if mongoWriter.calls != 1 || mongoWriter.fence.Expected != uint64(3) {
		t.Fatalf("mongo fence calls/value = %d/%#v, want 1/3", mongoWriter.calls, mongoWriter.fence.Expected)
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

func TestWALUsesMongoHealthGateAndReportsBackendFailure(t *testing.T) {
	storage := &Runtime{}
	storage.markBackendUnavailable(txstorage.BackendMongo, errors.New("mongo unavailable"))
	next := &recordingWAL{}
	guarded := healthAwareWAL{storage: storage, next: next}
	if err := guarded.Checkpoint(context.Background(), wal.Stream{Namespace: "world", StreamKey: "47", Generation: 1}, 1, func(context.Context) error { return nil }); !errors.Is(err, ErrStorageUnavailable) || next.calls != 0 {
		t.Fatalf("unhealthy WAL checkpoint error/calls = %v/%d, want unavailable/0", err, next.calls)
	}
	if _, err := guarded.Replay(context.Background(), wal.Stream{Namespace: "world", StreamKey: "47", Generation: 1}, 0, func(context.Context, wal.Record) error { return nil }); !errors.Is(err, ErrStorageUnavailable) || next.calls != 0 {
		t.Fatalf("unhealthy WAL replay error/calls = %v/%d, want unavailable/0", err, next.calls)
	}

	storage.recoverBackendHealth(txstorage.BackendMongo, storage.backendHealthGeneration(txstorage.BackendMongo))
	next.err = txstorage.NewBackendError(txstorage.BackendMongo, "append WAL", errors.New("connection refused"))
	record, err := wal.NewRecord(wal.Stream{Namespace: "world", StreamKey: "47", Generation: 1}, 1, 1, []byte("frame"))
	if err != nil {
		t.Fatalf("NewRecord returned error: %v", err)
	}
	if err := guarded.Append(context.Background(), record); err == nil {
		t.Fatal("Append error = nil, want backend failure")
	}
	if err := storage.requireStorageMode(meta.StorageModeMongo); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("WAL backend error did not close Mongo gate: %v", err)
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
