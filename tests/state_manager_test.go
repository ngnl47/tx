package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/state"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

type stateTestStore struct {
	recorder   change.UndoRecorder
	deltas     change.DeltaSet
	deltaReads int
}

func (store *stateTestStore) SetUndoRecorder(recorder change.UndoRecorder) {
	store.recorder = recorder
}

func (store *stateTestStore) Set() {
	previous := store.deltas.Clone()
	if store.recorder != nil {
		store.recorder.RecordUndo(func() { store.deltas = previous })
	}
	store.deltas = append(store.deltas, change.DeltaEntry{
		Path: testStorageDeltaPath(1),
		Kind: change.DeltaKindSet,
	})
}

func (store *stateTestStore) TypeSpec() meta.TypeSpec {
	return meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "StateTest"}}}
}

func (store *stateTestStore) RecordKey() storage.RecordKey {
	return storage.RecordKey{ID: int64(1001)}
}

func (store *stateTestStore) Deltas() change.DeltaSet {
	store.deltaReads++
	return store.deltas
}

func (store *stateTestStore) FieldNameResolver() change.FieldNameResolver {
	return change.FieldNameResolverFunc(func(change.FieldID, change.NameBackend) string { return "field" })
}

func (store *stateTestStore) DeltaValueResolver() change.DeltaValueResolver {
	return change.DeltaValueResolverFunc(func(change.DeltaPath) (any, bool) { return "value", true })
}

func (store *stateTestStore) DefaultApplyOptions() storage.ApplyOptions {
	return storage.ApplyOptions{}
}

func (store *stateTestStore) AllTouch() {}

func (store *stateTestStore) ClearDeltas() {
	store.deltas = nil
}

type stateTestProcedure struct {
	lock  txn.LockTarget
	store *stateTestStore
}

func (procedure stateTestProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure stateTestProcedure) Process(*txn.Tx) error {
	procedure.store.Set()
	return nil
}

func TestStateManagerDefersTransactionStoreUntilFlush(t *testing.T) {
	store := &stateTestStore{}
	writer := &recordingDeltaStoreApplier{}
	manager := state.NewManager(writer)
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)

	if err := manager.Call(context.Background(), stateTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if writer.hasOptions {
		t.Fatal("transaction commit wrote storage before manager flush")
	}
	if manager.Pending() != 1 {
		t.Fatalf("Pending = %d, want 1", manager.Pending())
	}
	if len(store.Deltas()) != 1 {
		t.Fatalf("store deltas = %#v, want pending change", store.Deltas())
	}
	store.deltaReads = 0

	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if !writer.hasOptions || len(writer.deltas) != 1 {
		t.Fatalf("writer state = %#v, want one persisted delta", writer)
	}
	if store.deltaReads != 1 {
		t.Fatalf("Flush Deltas calls = %d, want 1", store.deltaReads)
	}
	if manager.Pending() != 0 || len(store.Deltas()) != 0 {
		t.Fatalf("flush left pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}

func TestStateManagerAllowsAccumulatedDeltasWhenCleanTargetCheckIsEnabled(t *testing.T) {
	previous := txn.CurrentConfig()
	t.Cleanup(func() { txn.Configure(previous) })
	txn.Configure(txn.Config{Debug: txn.DebugConfig{CheckCleanLockTargets: true}})

	store := &stateTestStore{}
	store.Set()
	manager := state.NewManager(&recordingDeltaStoreApplier{})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	manager.MarkDirty(lock)

	if err := manager.Call(context.Background(), stateTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error with accumulated deltas: %v", err)
	}
	if manager.Pending() != 1 || len(store.Deltas()) != 2 {
		t.Fatalf("state manager lost accumulated deltas: pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}

func TestStateManagerRetainsDirtyStoreWhenFlushFails(t *testing.T) {
	store := &stateTestStore{}
	store.Set()
	writer := &recordingDeltaStoreApplier{err: errors.New("storage unavailable")}
	manager := state.NewManager(writer)
	manager.MarkDirty(txn.NewLockTarget(txn.NewLockKey("role", 1001), store))

	err := manager.Flush(context.Background())
	if !errors.Is(err, writer.err) {
		t.Fatalf("Flush error = %v, want storage error", err)
	}
	if manager.Pending() != 1 || len(store.Deltas()) != 1 {
		t.Fatalf("failed flush lost state: pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}

	writer.err = nil
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush returned error: %v", err)
	}
	if manager.Pending() != 0 || len(store.Deltas()) != 0 {
		t.Fatalf("successful retry left pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}

type blockingStateWriter struct {
	started chan struct{}
	release chan struct{}
}

type scheduledStateWriter struct {
	called chan struct{}
}

type retryingStateWriter struct {
	attempts int
}

func (writer *scheduledStateWriter) ApplyDeltasWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.DeltaSet, change.FieldNameResolver, change.DeltaValueResolver, storage.ApplyOptions) error {
	select {
	case writer.called <- struct{}{}:
	default:
	}
	return nil
}

func (writer *retryingStateWriter) ApplyDeltasWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.DeltaSet, change.FieldNameResolver, change.DeltaValueResolver, storage.ApplyOptions) error {
	writer.attempts++
	if writer.attempts == 1 {
		return errors.New("temporary storage failure")
	}
	return nil
}

func (writer *blockingStateWriter) ApplyDeltasWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.DeltaSet, change.FieldNameResolver, change.DeltaValueResolver, storage.ApplyOptions) error {
	close(writer.started)
	<-writer.release
	return nil
}

func TestStateManagerFlushHoldsTransactionLockUntilStorageWriteFinishes(t *testing.T) {
	store := &stateTestStore{}
	store.Set()
	writer := &blockingStateWriter{started: make(chan struct{}), release: make(chan struct{})}
	manager := state.NewManager(writer)
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	manager.MarkDirty(lock)

	flushDone := make(chan error, 1)
	go func() { flushDone <- manager.Flush(context.Background()) }()
	<-writer.started

	transactionEntered := make(chan struct{})
	transactionDone := make(chan error, 1)
	go func() {
		transactionDone <- txn.Run(context.Background(), []txn.LockTarget{lock}, func(*txn.Tx) error {
			close(transactionEntered)
			return nil
		})
	}()

	select {
	case <-transactionEntered:
		t.Fatal("transaction entered while state flush held the same lock")
	case <-time.After(20 * time.Millisecond):
	}

	close(writer.release)
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	select {
	case err := <-transactionDone:
		if err != nil {
			t.Fatalf("transaction returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("transaction did not continue after flush released lock")
	}
}

func TestStateManagerRunFlushesPendingStoresOnInterval(t *testing.T) {
	store := &stateTestStore{}
	store.Set()
	writer := &scheduledStateWriter{called: make(chan struct{}, 1)}
	manager := state.NewManager(writer)
	manager.MarkDirty(txn.NewLockTarget(txn.NewLockKey("role", 1001), store))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx, time.Millisecond) }()

	select {
	case <-writer.called:
	case <-time.After(time.Second):
		t.Fatal("manager run did not flush pending store")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if manager.Pending() != 0 || len(store.Deltas()) != 0 {
		t.Fatalf("scheduled flush left pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}

func TestStateManagerShutdownRetriesUntilPendingStoreIsFlushed(t *testing.T) {
	store := &stateTestStore{}
	store.Set()
	writer := &retryingStateWriter{}
	manager := state.NewManager(writer)
	manager.MarkDirty(txn.NewLockTarget(txn.NewLockKey("role", 1001), store))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx, time.Millisecond); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	if writer.attempts != 2 {
		t.Fatalf("storage attempts = %d, want 2", writer.attempts)
	}
	if manager.Pending() != 0 || len(store.Deltas()) != 0 {
		t.Fatalf("shutdown left pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}
