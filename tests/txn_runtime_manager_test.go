package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

type runtimeTestStore struct {
	recorder   change.UndoRecorder
	deltas     change.DeltaSet
	deltaReads int
	collapsed  int
}

func (store *runtimeTestStore) SetUndoRecorder(recorder change.UndoRecorder) {
	store.recorder = recorder
}

func (store *runtimeTestStore) Set() {
	store.SetField(1)
}

func (store *runtimeTestStore) SetField(field change.FieldID) {
	previous := store.deltas.Clone()
	if store.recorder != nil {
		store.recorder.RecordUndo(func() { store.deltas = previous })
	}
	store.deltas = append(store.deltas, change.DeltaEntry{
		Path: testStorageDeltaPath(field),
		Kind: change.DeltaKindSet,
	})
}

func (store *runtimeTestStore) TypeSpec() meta.TypeSpec {
	return meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "RuntimeTest"}}}
}

func (store *runtimeTestStore) RecordKey() storage.RecordKey {
	return storage.RecordKey{ID: int64(1001)}
}

func (store *runtimeTestStore) Deltas() change.DeltaSet {
	store.deltaReads++
	return store.deltas
}

func (store *runtimeTestStore) FieldNameResolver() change.FieldNameResolver {
	return change.FieldNameResolverFunc(func(change.FieldID, change.NameBackend) string { return "field" })
}

func (store *runtimeTestStore) DeltaValueResolver() change.DeltaValueResolver {
	return change.DeltaValueResolverFunc(func(change.DeltaPath) (any, bool) { return "value", true })
}

func (store *runtimeTestStore) DefaultApplyOptions() storage.ApplyOptions {
	return storage.ApplyOptions{}
}

func (store *runtimeTestStore) AllTouch() {}

func (store *runtimeTestStore) ClearDeltas() {
	store.deltas = nil
}

func (store *runtimeTestStore) CollapseDeltas() {
	store.collapsed++
	store.deltas = change.DeltaSet{{
		Path: testStorageDeltaPath(1),
		Kind: change.DeltaKindReplace,
	}}
}

type runtimeTestProcedure struct {
	lock  txn.LockTarget
	store *runtimeTestStore
}

func (procedure runtimeTestProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure runtimeTestProcedure) Process(*txn.Tx) error {
	procedure.store.Set()
	return nil
}

type observingRuntimeTestProcedure struct {
	runtimeTestProcedure
	called *bool
}

type multiFieldRuntimeTestProcedure struct {
	runtimeTestProcedure
}

func (procedure multiFieldRuntimeTestProcedure) Process(*txn.Tx) error {
	procedure.store.SetField(1)
	procedure.store.SetField(2)
	return nil
}

func (procedure observingRuntimeTestProcedure) Process(tx *txn.Tx) error {
	*procedure.called = true
	return procedure.runtimeTestProcedure.Process(tx)
}

func TestTxnRuntimeDefersTransactionStoreUntilFlush(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingDeltaStoreApplier{}
	manager := txn.NewRuntime(writer, txn.RuntimeConfig{
		FlushInterval:        time.Millisecond,
		ShutdownFlushTimeout: time.Second,
	})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)

	if err := manager.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
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

func TestTxnRuntimeAllowsAccumulatedDeltasWhenCleanTargetCheckIsEnabled(t *testing.T) {
	previous := txn.CurrentConfig()
	t.Cleanup(func() { txn.Configure(previous) })
	txn.Configure(txn.Config{Debug: txn.DebugConfig{CheckCleanLockTargets: true}})

	store := &runtimeTestStore{}
	store.Set()
	manager := txn.NewRuntime(&recordingDeltaStoreApplier{})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	manager.MarkDirty(lock)

	if err := manager.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error with accumulated deltas: %v", err)
	}
	if manager.Pending() != 1 || len(store.Deltas()) != 2 {
		t.Fatalf("txn runtime lost accumulated deltas: pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}

func TestTxnRuntimeAllowsPendingRootWhenCapacityIsFull(t *testing.T) {
	store := &runtimeTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	manager := txn.NewRuntime(&recordingDeltaStoreApplier{}, txn.RuntimeConfig{MaxPendingRoots: 1})
	if err := manager.MarkDirty(lock); err != nil {
		t.Fatalf("MarkDirty returned error: %v", err)
	}

	if err := manager.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call for pending root returned error: %v", err)
	}
	if manager.Pending() != 1 || len(store.Deltas()) != 1 {
		t.Fatalf("pending root state = pending:%d deltas:%#v, want one accumulated delta", manager.Pending(), store.Deltas())
	}
}

func TestTxnRuntimeRejectsNewRootBeforeProcedureWhenCapacityIsFull(t *testing.T) {
	first := &runtimeTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	manager := txn.NewRuntime(&recordingDeltaStoreApplier{}, txn.RuntimeConfig{MaxPendingRoots: 1})
	if err := manager.MarkDirty(firstLock); err != nil {
		t.Fatalf("MarkDirty returned error: %v", err)
	}

	second := &runtimeTestStore{}
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	called := false
	err := manager.Call(context.Background(), observingRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: secondLock, store: second},
		called:               &called,
	})
	if !errors.Is(err, txn.ErrPendingRootCapacity) {
		t.Fatalf("Call error = %v, want ErrPendingRootCapacity", err)
	}
	if called || len(second.Deltas()) != 0 {
		t.Fatalf("rejected procedure ran: called=%t deltas=%#v", called, second.Deltas())
	}
	if manager.Pending() != 1 {
		t.Fatalf("Pending = %d, want original root only", manager.Pending())
	}
}

func TestTxnRuntimeCollapsesOverBudgetRootDeltasBeforeFlush(t *testing.T) {
	store := &runtimeTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	manager := txn.NewRuntime(&recordingDeltaStoreApplier{}, txn.RuntimeConfig{
		MaxRootDeltaEntries: 1,
	})

	if err := manager.Call(context.Background(), multiFieldRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: lock, store: store},
	}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if store.collapsed != 0 {
		t.Fatalf("CollapseDeltas calls before Flush = %d, want 0", store.collapsed)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if store.collapsed != 1 {
		t.Fatalf("CollapseDeltas calls = %d, want 1", store.collapsed)
	}
	if manager.Pending() != 0 {
		t.Fatalf("Pending = %d, want 0 after successful Flush", manager.Pending())
	}
}

func TestTxnRuntimeMemorySoftLimitWarnsButAcceptsNewPendingRoots(t *testing.T) {
	var heapBytes uint64
	manager := txn.NewRuntime(&recordingDeltaStoreApplier{}, txn.RuntimeConfig{
		PendingMemorySoftLimit: 1,
		HeapBytes:              func() uint64 { return heapBytes },
	})

	first := &runtimeTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	if err := manager.MarkDirty(firstLock); err != nil {
		t.Fatalf("MarkDirty returned error: %v", err)
	}
	heapBytes = 2

	if err := manager.Call(context.Background(), runtimeTestProcedure{lock: firstLock, store: first}); err != nil {
		t.Fatalf("Call for existing pending root returned error: %v", err)
	}

	second := &runtimeTestStore{}
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	called := new(bool)
	if err := manager.Call(context.Background(), observingRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: secondLock, store: second},
		called:               called,
	}); err != nil {
		t.Fatalf("Call for new pending root returned error: %v", err)
	}
	if !*called {
		t.Fatal("new pending root procedure was not called")
	}
	if manager.Pending() != 2 {
		t.Fatalf("Pending = %d, want both roots", manager.Pending())
	}
}

func TestTxnRuntimeRetainsDirtyStoreWhenFlushFails(t *testing.T) {
	store := &runtimeTestStore{}
	store.Set()
	writer := &recordingDeltaStoreApplier{err: errors.New("storage unavailable")}
	manager := txn.NewRuntime(writer)
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

func TestTxnRuntimeRetainsDirtyStoreWhenWriterIsTypedNil(t *testing.T) {
	var writer *recordingDeltaStoreApplier
	store := &runtimeTestStore{}
	store.Set()
	manager := txn.NewRuntime(writer)
	manager.MarkDirty(txn.NewLockTarget(txn.NewLockKey("role", 1001), store))

	err := manager.Flush(context.Background())
	if !errors.Is(err, storage.ErrNilDeltaApplier) {
		t.Fatalf("Flush error = %v, want ErrNilDeltaApplier", err)
	}
	if manager.Pending() != 1 || len(store.Deltas()) != 1 {
		t.Fatalf("typed nil writer lost state: pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}

type blockingRuntimeWriter struct {
	started chan struct{}
	release chan struct{}
}

type scheduledRuntimeWriter struct {
	called chan struct{}
}

type retryingRuntimeWriter struct {
	attempts int
}

func (writer *scheduledRuntimeWriter) ApplyDeltasWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.DeltaSet, change.FieldNameResolver, change.DeltaValueResolver, storage.ApplyOptions) error {
	select {
	case writer.called <- struct{}{}:
	default:
	}
	return nil
}

func (writer *retryingRuntimeWriter) ApplyDeltasWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.DeltaSet, change.FieldNameResolver, change.DeltaValueResolver, storage.ApplyOptions) error {
	writer.attempts++
	if writer.attempts == 1 {
		return errors.New("temporary storage failure")
	}
	return nil
}

func (writer *blockingRuntimeWriter) ApplyDeltasWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.DeltaSet, change.FieldNameResolver, change.DeltaValueResolver, storage.ApplyOptions) error {
	close(writer.started)
	<-writer.release
	return nil
}

func TestTxnRuntimeFlushHoldsTransactionLockUntilStorageWriteFinishes(t *testing.T) {
	store := &runtimeTestStore{}
	store.Set()
	writer := &blockingRuntimeWriter{started: make(chan struct{}), release: make(chan struct{})}
	manager := txn.NewRuntime(writer)
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
		t.Fatal("transaction entered while txn runtime flush held the same lock")
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

func TestTxnRuntimeRunFlushesPendingStoresOnInterval(t *testing.T) {
	store := &runtimeTestStore{}
	store.Set()
	writer := &scheduledRuntimeWriter{called: make(chan struct{}, 1)}
	manager := txn.NewRuntime(writer)
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

func TestTxnRuntimeShutdownRetriesUntilPendingStoreIsFlushed(t *testing.T) {
	store := &runtimeTestStore{}
	store.Set()
	writer := &retryingRuntimeWriter{}
	manager := txn.NewRuntime(writer, txn.RuntimeConfig{
		FlushInterval:        time.Millisecond,
		ShutdownFlushTimeout: time.Second,
	})
	manager.MarkDirty(txn.NewLockTarget(txn.NewLockKey("role", 1001), store))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	if writer.attempts != 2 {
		t.Fatalf("storage attempts = %d, want 2", writer.attempts)
	}
	if manager.Pending() != 0 || len(store.Deltas()) != 0 {
		t.Fatalf("shutdown left pending=%d deltas=%#v", manager.Pending(), store.Deltas())
	}
}
