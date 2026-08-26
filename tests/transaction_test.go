package tests

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/modules/txn"
)

type transactionTestStore struct {
	recorder change.UndoRecorder
	value    int
}

type transactionDirtyStore struct {
	transactionTestStore
	changes recordedChanges
}

func (store *transactionDirtyStore) HasChanges() bool {
	return len(store.changes) > 0
}

func (store *transactionDirtyStore) VisitChanges(visitor change.Visitor) error {
	return store.changes.VisitChanges(visitor)
}

func (store *transactionDirtyStore) Set(value int) {
	previousValue := store.value
	previousChanges := store.changes.clone()
	if store.recorder != nil {
		store.recorder.RecordUndo(func() {
			store.value = previousValue
			store.changes = previousChanges
		})
	}
	store.value = value
	store.changes = append(store.changes, recordedChange{
		Path: change.Path{Nodes: [change.MaxPathDepth]change.PathNode{{Field: 1}}, Depth: 1},
		Op:   change.OpSet,
	})
}

type transactionTestLockable struct {
	lock txn.LockTarget
}

func (value transactionTestLockable) LockTarget() txn.LockTarget {
	return value.lock
}

func (store *transactionTestStore) SetUndoRecorder(recorder change.UndoRecorder) {
	store.recorder = recorder
}

func (store *transactionTestStore) Set(value int) {
	previous := store.value
	if store.recorder != nil {
		store.recorder.RecordUndo(func() { store.value = previous })
	}
	store.value = value
}

func TestLocksAndLockSetCollectLockables(t *testing.T) {
	firstStore := &transactionTestStore{}
	secondStore := &transactionTestStore{}
	first := transactionTestLockable{lock: txn.NewOrderedLockTarget(10, txn.NewLockKey("guild", 7), firstStore)}
	second := transactionTestLockable{lock: txn.NewOrderedLockTarget(20, txn.NewLockKey("role", 1001), secondStore)}
	want := []txn.LockTarget{first.lock, second.lock}

	if got := txn.Locks(first, second); !reflect.DeepEqual(got, want) {
		t.Fatalf("Locks = %#v, want %#v", got, want)
	}
	if got := txn.NewLockSet().Add(first).Add(second).Build(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LockSet.Build = %#v, want %#v", got, want)
	}
}

func TestTransactionRunValidatesSingleLockTarget(t *testing.T) {
	store := &transactionTestStore{}
	tests := []struct {
		name  string
		locks []txn.LockTarget
		want  string
	}{
		{name: "NoLocks", want: "no locks"},
		{
			name:  "InvalidKey",
			locks: []txn.LockTarget{txn.NewLockTarget(txn.LockKey{}, store)},
			want:  "invalid lock key",
		},
		{
			name: "NegativeOrder",
			locks: []txn.LockTarget{txn.NewOrderedLockTarget(
				-1,
				txn.NewLockKey("role", 1001),
				store,
			)},
			want: "negative lock order",
		},
		{
			name:  "NilStore",
			locks: []txn.LockTarget{txn.NewLockTarget(txn.NewLockKey("role", 1001), nil)},
			want:  "nil Store",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := txn.Run(context.Background(), test.locks, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestTransactionRunRollsBackAndUnbindsStore(t *testing.T) {
	store := &transactionTestStore{value: 1}
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(tx *txn.Tx) error {
		store.Set(2)
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("Run returned nil error")
	}
	if store.value != 1 {
		t.Fatalf("store value = %d, want 1", store.value)
	}
	if store.recorder != nil {
		t.Fatal("store recorder remains bound after rollback")
	}
}

func TestTransactionDebugCheckRejectsPendingChangesAtEntry(t *testing.T) {
	previousConfig := txn.CurrentConfig()
	t.Cleanup(func() { txn.Configure(previousConfig) })
	txn.Configure(txn.Config{Debug: txn.DebugConfig{CheckCleanLockTargets: true}})

	store := &transactionDirtyStore{changes: recordedChanges{{
		Path: change.Path{Nodes: [change.MaxPathDepth]change.PathNode{{Field: 1}}, Depth: 1},
		Op:   change.OpSet,
	}}}
	called := false
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(*txn.Tx) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "role:1001") || !strings.Contains(err.Error(), "pending changes") {
		t.Fatalf("Run error = %v, want pending change validation error", err)
	}
	if called {
		t.Fatal("transaction callback ran with pending changes")
	}
	if store.recorder != nil {
		t.Fatal("store recorder bound after pending change validation failed")
	}
}

func TestTransactionDebugCheckIsDisabledByDefault(t *testing.T) {
	previousConfig := txn.CurrentConfig()
	t.Cleanup(func() { txn.Configure(previousConfig) })
	txn.Configure(txn.Config{})

	store := &transactionDirtyStore{changes: recordedChanges{{
		Path: change.Path{Nodes: [change.MaxPathDepth]change.PathNode{{Field: 1}}, Depth: 1},
		Op:   change.OpSet,
	}}}
	called := false
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(*txn.Tx) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Run returned error with debug check disabled: %v", err)
	}
	if !called {
		t.Fatal("transaction callback did not run with debug check disabled")
	}
}

func TestTransactionDirtyTargetsIncludesOnlyPendingModifiedStores(t *testing.T) {
	first := &transactionDirtyStore{}
	second := &transactionDirtyStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	var transaction *txn.Tx

	err := txn.Run(context.Background(), []txn.LockTarget{secondLock, firstLock}, func(tx *txn.Tx) error {
		transaction = tx
		second.Set(2)
		if err := tx.Do(func(*txn.Tx) error {
			first.Set(1)
			return errors.New("rollback first")
		}); err == nil {
			t.Fatal("Do returned nil error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if transaction == nil {
		t.Fatal("transaction was not captured")
	}
	if got := transaction.DirtyTargets(); !reflect.DeepEqual(got, []txn.LockTarget{secondLock}) {
		t.Fatalf("DirtyTargets = %#v, want %#v", got, []txn.LockTarget{secondLock})
	}
}

func TestTransactionDoRollsBackOnlyNestedSavepoint(t *testing.T) {
	store := &transactionTestStore{value: 1}
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(tx *txn.Tx) error {
		store.Set(2)
		if err := tx.Do(func(tx *txn.Tx) error {
			store.Set(3)
			return errors.New("nested rollback")
		}); err == nil {
			t.Fatal("Do returned nil error")
		}
		if store.value != 2 {
			t.Fatalf("value after nested rollback = %d, want 2", store.value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if store.value != 2 {
		t.Fatalf("committed value = %d, want 2", store.value)
	}
}

func TestTransactionRunsCallbacksAtExpectedBoundary(t *testing.T) {
	store := &transactionTestStore{}
	var calls []string
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(tx *txn.Tx) error {
		tx.AfterCommit(func(context.Context) error {
			if store.recorder != nil {
				t.Fatal("store recorder remains bound while running after-commit callback")
			}
			calls = append(calls, "commit")
			return nil
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"commit"}) {
		t.Fatalf("commit calls = %#v", calls)
	}

	calls = nil
	err = txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(tx *txn.Tx) error {
		tx.AfterRollback(func(context.Context) {
			if store.recorder != nil {
				t.Fatal("store recorder remains bound while running after-rollback callback")
			}
			calls = append(calls, "rollback")
		})
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("Run returned nil error")
	}
	if !reflect.DeepEqual(calls, []string{"rollback"}) {
		t.Fatalf("rollback calls = %#v", calls)
	}
}

func TestTransactionStopsAfterCommitCallbacksAfterFailure(t *testing.T) {
	store := &transactionTestStore{}
	called := false
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(tx *txn.Tx) error {
		tx.AfterCommit(func(context.Context) error {
			return errors.New("commit failed")
		})
		tx.AfterCommit(func(context.Context) error {
			called = true
			return nil
		})
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "commit failed") {
		t.Fatalf("Run error = %v, want commit callback error", err)
	}
	if called {
		t.Fatal("after-commit callback ran after previous callback failed")
	}
}

type transactionChildProcedure struct {
	lock  txn.LockTarget
	store *transactionTestStore
}

func (procedure transactionChildProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure transactionChildProcedure) Process(tx *txn.Tx) error {
	procedure.store.Set(3)
	return errors.New("child rollback")
}

type transactionParentProcedure struct {
	lock  txn.LockTarget
	store *transactionTestStore
	child txn.Procedure
}

func (procedure transactionParentProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure transactionParentProcedure) Process(tx *txn.Tx) error {
	procedure.store.Set(2)
	if err := tx.Call(procedure.child); err == nil {
		return errors.New("child procedure returned nil error")
	}
	if procedure.store.value != 2 {
		return errors.New("child rollback changed parent state")
	}
	return nil
}

func TestTransactionCallReusesRootTargetsAndRollsBackChildSavepoint(t *testing.T) {
	store := &transactionTestStore{value: 1}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	procedure := transactionParentProcedure{
		lock:  lock,
		store: store,
		child: transactionChildProcedure{lock: lock, store: store},
	}
	if err := txn.Call(context.Background(), procedure); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if store.value != 2 {
		t.Fatalf("store value = %d, want 2", store.value)
	}
}

type transactionNewLockProcedure struct {
	lock  txn.LockTarget
	store *transactionTestStore
	value int
}

func (procedure transactionNewLockProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure transactionNewLockProcedure) Process(*txn.Tx) error {
	if procedure.store != nil {
		procedure.store.Set(procedure.value)
	}
	return nil
}

type transactionCallsNewLockProcedure struct {
	lock  txn.LockTarget
	child txn.Procedure
}

func (procedure transactionCallsNewLockProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure transactionCallsNewLockProcedure) Process(tx *txn.Tx) error {
	return tx.Call(procedure.child)
}

func TestTransactionCallAcquiresChildProcedureDirectLock(t *testing.T) {
	first := &transactionTestStore{}
	second := &transactionTestStore{value: 1}
	parent := transactionCallsNewLockProcedure{
		lock: txn.NewLockTarget(txn.NewLockKey("role", 1001), first),
		child: transactionNewLockProcedure{
			lock:  txn.NewLockTarget(txn.NewLockKey("role", 1002), second),
			store: second,
			value: 2,
		},
	}
	if err := txn.Call(context.Background(), parent); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if second.value != 2 {
		t.Fatalf("child store value = %d, want 2", second.value)
	}
	if second.recorder != nil {
		t.Fatal("dynamic child Store remains bound after root commit")
	}
}

func TestTransactionCallRollsBackDynamicChildSavepoint(t *testing.T) {
	first := &transactionTestStore{value: 1}
	second := &transactionTestStore{value: 10}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	parent := transactionParentProcedure{
		lock:  firstLock,
		store: first,
		child: transactionChildProcedure{lock: secondLock, store: second},
	}

	if err := txn.Call(context.Background(), parent); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if first.value != 2 {
		t.Fatalf("parent value = %d, want 2", first.value)
	}
	if second.value != 10 {
		t.Fatalf("dynamic child value = %d, want rollback to 10", second.value)
	}
	if second.recorder != nil {
		t.Fatal("dynamic child Store remains bound after root commit")
	}
}

func TestTransactionCallRejectsChildProcedureWithDifferentStoreForHeldKey(t *testing.T) {
	rootStore := &transactionTestStore{}
	childStore := &transactionTestStore{}
	key := txn.NewLockKey("role", 1001)
	parent := transactionCallsNewLockProcedure{
		lock: txn.NewLockTarget(key, rootStore),
		child: transactionNewLockProcedure{
			lock: txn.NewLockTarget(key, childStore),
		},
	}
	err := txn.Call(context.Background(), parent)
	if err == nil || !strings.Contains(err.Error(), "different Store") || !strings.Contains(err.Error(), "role:1001") {
		t.Fatalf("Call error = %v, want different Store error", err)
	}
}

type transactionDeadlockFirstProcedure struct {
	lock      txn.LockTarget
	store     *transactionTestStore
	child     txn.Procedure
	ready     chan struct{}
	peerReady chan struct{}
	once      sync.Once
	attempts  atomic.Int32
	commits   atomic.Int32
}

func (procedure *transactionDeadlockFirstProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure *transactionDeadlockFirstProcedure) Process(tx *txn.Tx) error {
	procedure.attempts.Add(1)
	procedure.store.Set(procedure.store.value + 1)
	tx.AfterCommit(func(context.Context) error {
		procedure.commits.Add(1)
		return nil
	})
	procedure.once.Do(func() { close(procedure.ready) })
	<-procedure.peerReady
	return tx.Call(procedure.child)
}

type transactionDeadlockSecondProcedure struct {
	lock      txn.LockTarget
	store     *transactionTestStore
	child     txn.Procedure
	ready     chan struct{}
	peerReady chan struct{}
	once      sync.Once
	attempts  atomic.Int32
	commits   atomic.Int32
}

func (procedure *transactionDeadlockSecondProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure *transactionDeadlockSecondProcedure) Process(tx *txn.Tx) error {
	procedure.attempts.Add(1)
	procedure.store.Set(procedure.store.value + 1)
	tx.AfterCommit(func(context.Context) error {
		procedure.commits.Add(1)
		return nil
	})
	procedure.once.Do(func() { close(procedure.ready) })
	<-procedure.peerReady
	return tx.Call(procedure.child)
}

func newTransactionDeadlockProcedures() (*transactionDeadlockFirstProcedure, *transactionDeadlockSecondProcedure, *transactionTestStore, *transactionTestStore) {
	firstStore := &transactionTestStore{}
	secondStore := &transactionTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), firstStore)
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), secondStore)
	firstReady := make(chan struct{})
	secondReady := make(chan struct{})
	first := &transactionDeadlockFirstProcedure{
		lock:      firstLock,
		store:     firstStore,
		child:     transactionNewLockProcedure{lock: secondLock},
		ready:     firstReady,
		peerReady: secondReady,
	}
	second := &transactionDeadlockSecondProcedure{
		lock:      secondLock,
		store:     secondStore,
		child:     transactionNewLockProcedure{lock: firstLock},
		ready:     secondReady,
		peerReady: firstReady,
	}
	return first, second, firstStore, secondStore
}

func TestTransactionDeadlockReportsCompleteWaitingAndProcedureChain(t *testing.T) {
	first, second, _, _ := newTransactionDeadlockProcedures()
	results := make(chan error, 2)
	go func() { results <- txn.CallWithOptions(context.Background(), first, txn.CallOptions{}) }()
	go func() { results <- txn.CallWithOptions(context.Background(), second, txn.CallOptions{}) }()

	var deadlock *txn.DeadlockError
	for range 2 {
		err := <-results
		if err == nil {
			continue
		}
		if !errors.As(err, &deadlock) {
			t.Fatalf("concurrent Call error = %v, want DeadlockError", err)
		}
	}
	if deadlock == nil {
		t.Fatal("concurrent Calls returned without a deadlock victim")
	}
	if len(deadlock.Cycle) != 2 {
		t.Fatalf("deadlock cycle length = %d, want 2", len(deadlock.Cycle))
	}
	joined := deadlock.Error()
	for _, part := range []string{
		"transactionDeadlockFirstProcedure -> transactionNewLockProcedure",
		"transactionDeadlockSecondProcedure -> transactionNewLockProcedure",
		"role:1001",
		"role:1002",
	} {
		if !strings.Contains(joined, part) {
			t.Fatalf("deadlock error %q does not contain %q", joined, part)
		}
	}
}

func TestTransactionCallRetriesDeadlockVictimFromCleanRootState(t *testing.T) {
	first, second, firstStore, secondStore := newTransactionDeadlockProcedures()
	results := make(chan error, 2)
	go func() { results <- txn.Call(context.Background(), first) }()
	go func() { results <- txn.Call(context.Background(), second) }()

	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("Call returned error after bounded retry: %v", err)
		}
	}
	if firstStore.value != 1 || secondStore.value != 1 {
		t.Fatalf("committed values = (%d, %d), want (1, 1)", firstStore.value, secondStore.value)
	}
	if first.attempts.Load()+second.attempts.Load() < 3 {
		t.Fatalf("total attempts = %d, want at least one deadlock retry", first.attempts.Load()+second.attempts.Load())
	}
	if first.commits.Load()+second.commits.Load() != 2 {
		t.Fatalf("after-commit calls = %d, want one for each committed root", first.commits.Load()+second.commits.Load())
	}
}

func TestTransactionRunRejectsNestedRun(t *testing.T) {
	store := &transactionTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	err := txn.Run(context.Background(), []txn.LockTarget{lock}, func(tx *txn.Tx) error {
		return txn.Run(tx.Context(), []txn.LockTarget{lock}, func(*txn.Tx) error {
			return nil
		})
	})
	if err == nil || !strings.Contains(err.Error(), "nested Run is not allowed") {
		t.Fatalf("nested Run error = %v, want rejected error", err)
	}
}

func TestTransactionRunReturnsContextDeadlineBeforeCallback(t *testing.T) {
	store := &transactionTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- txn.Run(context.Background(), []txn.LockTarget{lock}, func(*txn.Tx) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	called := false
	err := txn.Run(ctx, []txn.LockTarget{lock}, func(*txn.Tx) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want context deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), "role:1001") {
		t.Fatalf("Run error = %v, want blocked lock key", err)
	}
	if called {
		t.Fatal("transaction callback ran after lock wait timed out")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("holder Run returned error: %v", err)
	}
}

func TestTransactionRunTimeoutReleasesPreviouslyAcquiredTargets(t *testing.T) {
	first := &transactionTestStore{}
	second := &transactionTestStore{}
	firstLock := txn.NewLockTarget(txn.NewLockKey("role", 1001), first)
	secondLock := txn.NewLockTarget(txn.NewLockKey("role", 1002), second)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- txn.Run(context.Background(), []txn.LockTarget{secondLock}, func(*txn.Tx) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := txn.Run(ctx, []txn.LockTarget{firstLock, secondLock}, func(*txn.Tx) error {
		t.Fatal("transaction callback ran before every lock was acquired")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want context deadline exceeded", err)
	}

	available := make(chan error, 1)
	go func() {
		available <- txn.Run(context.Background(), []txn.LockTarget{firstLock}, func(*txn.Tx) error {
			return nil
		})
	}()
	select {
	case err := <-available:
		if err != nil {
			t.Fatalf("Run on released target returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first target remained locked after later target timed out")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("holder Run returned error: %v", err)
	}
}

func TestTransactionRunRejectsDuplicateKeyWithDifferentOrder(t *testing.T) {
	store := &transactionTestStore{}
	key := txn.NewLockKey("role", 1001)
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewOrderedLockTarget(10, key, store),
		txn.NewOrderedLockTarget(20, key, store),
	}, func(*txn.Tx) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "different lock orders") {
		t.Fatalf("Run error = %v, want duplicate lock order error", err)
	}
}
