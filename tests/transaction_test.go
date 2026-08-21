package tests

import (
	"context"
	"errors"
	"reflect"
	"strings"
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
	deltas change.DeltaSet
}

func (store *transactionDirtyStore) Deltas() change.DeltaSet {
	return store.deltas
}

func (store *transactionDirtyStore) Set(value int) {
	previousValue := store.value
	previousDeltas := store.deltas.Clone()
	if store.recorder != nil {
		store.recorder.RecordUndo(func() {
			store.value = previousValue
			store.deltas = previousDeltas
		})
	}
	store.value = value
	store.deltas = append(store.deltas, change.DeltaEntry{
		Path: change.DeltaPath{Nodes: [change.MaxDeltaDepth]change.PathNode{{Field: 1}}, Depth: 1},
		Kind: change.DeltaKindSet,
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

func TestTransactionDebugCheckRejectsPendingDeltasAtEntry(t *testing.T) {
	previousConfig := txn.CurrentConfig()
	t.Cleanup(func() { txn.Configure(previousConfig) })
	txn.Configure(txn.Config{Debug: txn.DebugConfig{CheckCleanLockTargets: true}})

	store := &transactionDirtyStore{deltas: change.DeltaSet{{
		Path: change.DeltaPath{Nodes: [change.MaxDeltaDepth]change.PathNode{{Field: 1}}, Depth: 1},
		Kind: change.DeltaKindSet,
	}}}
	called := false
	err := txn.Run(context.Background(), []txn.LockTarget{
		txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
	}, func(*txn.Tx) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "role:1001") || !strings.Contains(err.Error(), "pending deltas") {
		t.Fatalf("Run error = %v, want pending delta validation error", err)
	}
	if called {
		t.Fatal("transaction callback ran with pending deltas")
	}
	if store.recorder != nil {
		t.Fatal("store recorder bound after pending delta validation failed")
	}
}

func TestTransactionDebugCheckIsDisabledByDefault(t *testing.T) {
	previousConfig := txn.CurrentConfig()
	t.Cleanup(func() { txn.Configure(previousConfig) })
	txn.Configure(txn.Config{})

	store := &transactionDirtyStore{deltas: change.DeltaSet{{
		Path: change.DeltaPath{Nodes: [change.MaxDeltaDepth]change.PathNode{{Field: 1}}, Depth: 1},
		Kind: change.DeltaKindSet,
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
	lock txn.LockTarget
}

func (procedure transactionNewLockProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (transactionNewLockProcedure) Process(*txn.Tx) error {
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

func TestTransactionCallRejectsChildProcedureWithNewLock(t *testing.T) {
	first := &transactionTestStore{}
	second := &transactionTestStore{}
	parent := transactionCallsNewLockProcedure{
		lock: txn.NewLockTarget(txn.NewLockKey("role", 1001), first),
		child: transactionNewLockProcedure{
			lock: txn.NewLockTarget(txn.NewLockKey("role", 1002), second),
		},
	}
	err := txn.Call(context.Background(), parent)
	if err == nil || !strings.Contains(err.Error(), "transactionCallsNewLockProcedure -> transactionNewLockProcedure") || !strings.Contains(err.Error(), "role:1002") {
		t.Fatalf("Call error = %v, want procedure trace and new lock", err)
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
