package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	cachemodule "github.com/ngnl47/tx/modules/cache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

type runtimeTestStore struct {
	recorder   change.UndoRecorder
	changes    recordedChanges
	visitCalls int
	collapsed  int
}

type runtimeNonCollapsibleStore struct {
	store runtimeTestStore
}

type runtimeMemoryStore struct {
	runtimeTestStore
}

func (*runtimeMemoryStore) TypeSpec() meta.TypeSpec {
	return meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "RuntimeMemoryTest"}}}
}

func (store *runtimeNonCollapsibleStore) SetUndoRecorder(recorder change.UndoRecorder) {
	store.store.SetUndoRecorder(recorder)
}

func (store *runtimeNonCollapsibleStore) TypeSpec() meta.TypeSpec {
	return store.store.TypeSpec()
}

func (store *runtimeNonCollapsibleStore) RecordKey() storage.RecordKey {
	return store.store.RecordKey()
}

func (store *runtimeNonCollapsibleStore) VisitChanges(visitor change.Visitor) error {
	return store.store.VisitChanges(visitor)
}

func (store *runtimeNonCollapsibleStore) HasChanges() bool {
	return store.store.HasChanges()
}

func (store *runtimeNonCollapsibleStore) FieldNameResolver() change.FieldNameResolver {
	return store.store.FieldNameResolver()
}

func (store *runtimeNonCollapsibleStore) ResolveValue(path change.Path) (any, bool) {
	return store.store.ResolveValue(path)
}

func (store *runtimeNonCollapsibleStore) DefaultApplyOptions() storage.ApplyOptions {
	return store.store.DefaultApplyOptions()
}

func (store *runtimeNonCollapsibleStore) AllTouch() {
	store.store.AllTouch()
}

func (store *runtimeNonCollapsibleStore) ClearChanges() {
	store.store.ClearChanges()
}

func (store *runtimeTestStore) SetUndoRecorder(recorder change.UndoRecorder) {
	store.recorder = recorder
}

func (store *runtimeTestStore) Set() {
	store.SetField(1)
}

func (store *runtimeTestStore) SetField(field change.FieldID) {
	previous := store.changes.clone()
	if store.recorder != nil {
		store.recorder.RecordUndo(func() { store.changes = previous })
	}
	store.changes = append(store.changes, recordedChange{
		Path: testStoragePath(field),
		Op:   change.OpSet,
	})
}

func (store *runtimeTestStore) TypeSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{FullName: "RuntimeTest"}},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
	}
}

func (store *runtimeTestStore) RecordKey() storage.RecordKey {
	return storage.RecordKey{ID: int64(1001)}
}

func (store *runtimeTestStore) VisitChanges(visitor change.Visitor) error {
	store.visitCalls++
	return store.changes.VisitChanges(visitor)
}

func (store *runtimeTestStore) HasChanges() bool {
	return len(store.changes) > 0
}

func (store *runtimeTestStore) FieldNameResolver() change.FieldNameResolver {
	return change.FieldNameResolverFunc(func(change.FieldID, change.FieldNameKind) string { return "field" })
}

func (store *runtimeTestStore) ResolveValue(change.Path) (any, bool) { return "value", true }

func (store *runtimeTestStore) DefaultApplyOptions() storage.ApplyOptions {
	return storage.ApplyOptions{}
}

func (store *runtimeTestStore) AllTouch() {}

func (store *runtimeTestStore) ClearChanges() {
	store.changes = nil
}

func (store *runtimeTestStore) CollapseChanges() {
	store.collapsed++
	store.changes = recordedChanges{{
		Path: testStoragePath(1),
		Op:   change.OpReplace,
	}}
}

type runtimeTestProcedure struct {
	lock  txn.LockTarget
	store *runtimeTestStore
}

type runtimeDynamicChildProcedure struct {
	lock   txn.LockTarget
	store  *runtimeTestStore
	called *bool
}

func (procedure runtimeDynamicChildProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure runtimeDynamicChildProcedure) Process(*txn.Tx) error {
	if procedure.called != nil {
		*procedure.called = true
	}
	procedure.store.Set()
	return nil
}

type runtimeDynamicParentProcedure struct {
	lock  txn.LockTarget
	store *runtimeTestStore
	child txn.Procedure
}

func (procedure runtimeDynamicParentProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure runtimeDynamicParentProcedure) Process(tx *txn.Tx) error {
	procedure.store.Set()
	return tx.Call(procedure.child)
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

type runtimeNonCollapsibleProcedure struct {
	lock   txn.LockTarget
	called *bool
}

func (procedure runtimeNonCollapsibleProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure runtimeNonCollapsibleProcedure) Process(*txn.Tx) error {
	*procedure.called = true
	return nil
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

func newCacheRuntime(t *testing.T, writer storage.ChangeApplier, config cachemodule.Config) (*txn.Runtime, *cachemodule.Manager) {
	t.Helper()
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: writer}, config)
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	return runtime, manager
}

func TestCacheManagerDefersTransactionStoreUntilFlush(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{}
	runtime, manager := newCacheRuntime(t, writer, cachemodule.Config{
		FlushInterval:        time.Millisecond,
		ShutdownFlushTimeout: time.Second,
	})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)

	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if writer.hasOptions {
		t.Fatal("transaction commit wrote storage before manager flush")
	}
	if manager.Pending() != 1 {
		t.Fatalf("Pending = %d, want 1", manager.Pending())
	}
	if len(store.changes) != 1 {
		t.Fatalf("store changes = %#v, want pending change", store.changes)
	}
	store.visitCalls = 0

	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if !writer.hasOptions || len(writer.changes) != 1 {
		t.Fatalf("writer state = %#v, want one persisted change", writer)
	}
	if store.visitCalls != 1 {
		t.Fatalf("Flush VisitChanges calls = %d, want 1", store.visitCalls)
	}
	if manager.Pending() != 0 || len(store.changes) != 0 {
		t.Fatalf("flush left pending=%d changes=%#v", manager.Pending(), store.changes)
	}
}

func TestCacheManagerBudgetedFlushVisitsOnceWhenWithinBudget(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{}
	runtime, manager := newCacheRuntime(t, writer, cachemodule.Config{MaxRootChanges: 2})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)

	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	store.visitCalls = 0

	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if store.visitCalls != 1 {
		t.Fatalf("Flush VisitChanges calls = %d, want 1", store.visitCalls)
	}
}

func TestCacheManagerAllowsAccumulatedChangesWhenCleanTargetCheckIsEnabled(t *testing.T) {
	previous := txn.CurrentConfig()
	t.Cleanup(func() { txn.Configure(previous) })
	txn.Configure(txn.Config{Debug: txn.DebugConfig{CheckCleanLockTargets: true}})

	store := &runtimeTestStore{}
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("first Call returned error: %v", err)
	}
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error with accumulated changes: %v", err)
	}
	if manager.Pending() != 1 || len(store.changes) != 2 {
		t.Fatalf("txn runtime lost accumulated changes: pending=%d changes=%#v", manager.Pending(), store.changes)
	}
}

func TestCacheManagerAllowsPendingRootWhenCapacityIsFull(t *testing.T) {
	store := &runtimeTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{MaxPendingRoots: 1})
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("first Call returned error: %v", err)
	}

	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call for pending root returned error: %v", err)
	}
	if manager.Pending() != 1 || len(store.changes) != 2 {
		t.Fatalf("pending root state = pending:%d changes:%#v, want two accumulated changes", manager.Pending(), store.changes)
	}
}

func TestCacheManagerRejectsNewRootBeforeProcedureWhenCapacityIsFull(t *testing.T) {
	first := &runtimeTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{MaxPendingRoots: 1})
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: firstLock, store: first}); err != nil {
		t.Fatalf("first Call returned error: %v", err)
	}

	second := &runtimeTestStore{}
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	called := false
	err := runtime.Call(context.Background(), observingRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: secondLock, store: second},
		called:               &called,
	})
	if !errors.Is(err, cachemodule.ErrPendingRootCapacity) {
		t.Fatalf("Call error = %v, want ErrPendingRootCapacity", err)
	}
	if called || len(second.changes) != 0 {
		t.Fatalf("rejected procedure ran: called=%t changes=%#v", called, second.changes)
	}
	if manager.Pending() != 1 {
		t.Fatalf("Pending = %d, want original root only", manager.Pending())
	}
}

func TestCacheManagerReservesDynamicChildTargetBeforeChildProcess(t *testing.T) {
	first := &runtimeTestStore{}
	second := &runtimeTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	childCalled := false
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{MaxPendingRoots: 1})

	err := runtime.Call(context.Background(), runtimeDynamicParentProcedure{
		lock:  firstLock,
		store: first,
		child: runtimeDynamicChildProcedure{lock: secondLock, store: second, called: &childCalled},
	})
	if !errors.Is(err, cachemodule.ErrPendingRootCapacity) {
		t.Fatalf("Call error = %v, want ErrPendingRootCapacity", err)
	}
	if childCalled {
		t.Fatal("dynamic child Process ran after target reservation was rejected")
	}
	if len(first.changes) != 0 || len(second.changes) != 0 || manager.Pending() != 0 {
		t.Fatalf("rejected dynamic target left changes or pending roots: first=%#v second=%#v pending=%d", first.changes, second.changes, manager.Pending())
	}
}

func TestCacheManagerCommitsDynamicChildTarget(t *testing.T) {
	first := &runtimeTestStore{}
	second := &runtimeTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{MaxPendingRoots: 2})

	if err := runtime.Call(context.Background(), runtimeDynamicParentProcedure{
		lock:  firstLock,
		store: first,
		child: runtimeDynamicChildProcedure{lock: secondLock, store: second},
	}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if manager.Pending() != 2 {
		t.Fatalf("Pending = %d, want root and dynamic child", manager.Pending())
	}
}

func TestCacheManagerCollapsesOverBudgetRootChangesBeforeFlush(t *testing.T) {
	store := &runtimeTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{
		MaxRootChanges: 1,
	})

	if err := runtime.Call(context.Background(), multiFieldRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: lock, store: store},
	}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if store.collapsed != 0 {
		t.Fatalf("CollapseChanges calls before Flush = %d, want 0", store.collapsed)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if store.collapsed != 1 {
		t.Fatalf("CollapseChanges calls = %d, want 1", store.collapsed)
	}
	if manager.Pending() != 0 {
		t.Fatalf("Pending = %d, want 0 after successful Flush", manager.Pending())
	}
}

func TestCacheManagerRejectsNonCollapsiblePersistentTargetBeforeProcedure(t *testing.T) {
	store := &runtimeNonCollapsibleStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{MaxRootChanges: 1})
	called := false

	err := runtime.Call(context.Background(), runtimeNonCollapsibleProcedure{
		lock:   lock,
		called: &called,
	})
	if !errors.Is(err, cachemodule.ErrChangeCollapseUnsupported) {
		t.Fatalf("Call error = %v, want ErrChangeCollapseUnsupported", err)
	}
	if called || manager.Pending() != 0 {
		t.Fatalf("unsupported target reached procedure: called=%t pending=%d", called, manager.Pending())
	}
}

func TestCacheManagerMemorySoftLimitWarnsButAcceptsNewPendingRoots(t *testing.T) {
	var heapBytes uint64
	runtime, manager := newCacheRuntime(t, &recordingChangeStoreApplier{}, cachemodule.Config{
		HeapSoftLimitBytes: 1,
		ReadHeapBytes:      func() uint64 { return heapBytes },
	})

	first := &runtimeTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: firstLock, store: first}); err != nil {
		t.Fatalf("first Call returned error: %v", err)
	}
	heapBytes = 2

	second := &runtimeTestStore{}
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	called := new(bool)
	if err := runtime.Call(context.Background(), observingRuntimeTestProcedure{
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

func (writer *scheduledRuntimeWriter) ApplyChangesWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, storage.ApplyOptions) error {
	select {
	case writer.called <- struct{}{}:
	default:
	}
	return nil
}

func (writer *retryingRuntimeWriter) ApplyChangesWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, storage.ApplyOptions) error {
	writer.attempts++
	if writer.attempts == 1 {
		return errors.New("temporary storage failure")
	}
	return nil
}

func (writer *blockingRuntimeWriter) ApplyChangesWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, storage.ApplyOptions) error {
	close(writer.started)
	<-writer.release
	return nil
}

func TestCacheManagerFlushHoldsTransactionLockUntilStorageWriteFinishes(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &blockingRuntimeWriter{started: make(chan struct{}), release: make(chan struct{})}
	runtime, manager := newCacheRuntime(t, writer, cachemodule.Config{})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}

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

func TestCacheManagerRunFlushesPendingStoresOnInterval(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &scheduledRuntimeWriter{called: make(chan struct{}, 1)}
	runtime, manager := newCacheRuntime(t, writer, cachemodule.Config{})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}

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
	if manager.Pending() != 0 || len(store.changes) != 0 {
		t.Fatalf("scheduled flush left pending=%d changes=%#v", manager.Pending(), store.changes)
	}
}

func TestCacheManagerShutdownRetriesUntilPendingStoreIsFlushed(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &retryingRuntimeWriter{}
	runtime, manager := newCacheRuntime(t, writer, cachemodule.Config{
		FlushInterval:        time.Millisecond,
		ShutdownFlushTimeout: time.Second,
	})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	if writer.attempts != 2 {
		t.Fatalf("storage attempts = %d, want 2", writer.attempts)
	}
	if manager.Pending() != 0 || len(store.changes) != 0 {
		t.Fatalf("shutdown left pending=%d changes=%#v", manager.Pending(), store.changes)
	}
}
