package tests

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/modules/transaction"
)

type transactionTestStore struct {
	recorder change.UndoRecorder
	value    int
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

func TestTransactionRunRollsBackAndUnbindsStore(t *testing.T) {
	store := &transactionTestStore{value: 1}
	err := transaction.Run(context.Background(), transaction.Targets(
		transaction.NewTarget(transaction.NewKey("role", 1001), store),
	), func(tx *transaction.Tx) error {
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

func TestTransactionDoRollsBackOnlyNestedSavepoint(t *testing.T) {
	store := &transactionTestStore{value: 1}
	err := transaction.Run(context.Background(), transaction.Targets(
		transaction.NewTarget(transaction.NewKey("role", 1001), store),
	), func(tx *transaction.Tx) error {
		store.Set(2)
		if err := tx.Do(func(tx *transaction.Tx) error {
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
	err := transaction.Run(context.Background(), transaction.Targets(
		transaction.NewTarget(transaction.NewKey("role", 1001), store),
	), func(tx *transaction.Tx) error {
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
	err = transaction.Run(context.Background(), transaction.Targets(
		transaction.NewTarget(transaction.NewKey("role", 1001), store),
	), func(tx *transaction.Tx) error {
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

type transactionChildProcedure struct {
	target transaction.Target
	store  *transactionTestStore
}

func (procedure transactionChildProcedure) Targets(context.Context) ([]transaction.Target, error) {
	return transaction.Targets(procedure.target), nil
}

func (procedure transactionChildProcedure) Process(tx *transaction.Tx) error {
	procedure.store.Set(3)
	return errors.New("child rollback")
}

type transactionParentProcedure struct {
	target transaction.Target
	store  *transactionTestStore
	child  transaction.Procedure
}

func (procedure transactionParentProcedure) Targets(context.Context) ([]transaction.Target, error) {
	return transaction.Targets(procedure.target), nil
}

func (procedure transactionParentProcedure) Process(tx *transaction.Tx) error {
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
	target := transaction.NewTarget(transaction.NewKey("role", 1001), store)
	procedure := transactionParentProcedure{
		target: target,
		store:  store,
		child:  transactionChildProcedure{target: target, store: store},
	}
	if err := transaction.Call(context.Background(), procedure); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if store.value != 2 {
		t.Fatalf("store value = %d, want 2", store.value)
	}
}

type transactionNewTargetProcedure struct {
	target transaction.Target
}

func (procedure transactionNewTargetProcedure) Targets(context.Context) ([]transaction.Target, error) {
	return transaction.Targets(procedure.target), nil
}

func (transactionNewTargetProcedure) Process(*transaction.Tx) error {
	return nil
}

type transactionCallsNewTargetProcedure struct {
	target transaction.Target
	child  transaction.Procedure
}

func (procedure transactionCallsNewTargetProcedure) Targets(context.Context) ([]transaction.Target, error) {
	return transaction.Targets(procedure.target), nil
}

func (procedure transactionCallsNewTargetProcedure) Process(tx *transaction.Tx) error {
	return tx.Call(procedure.child)
}

func TestTransactionCallRejectsChildProcedureWithNewTarget(t *testing.T) {
	first := &transactionTestStore{}
	second := &transactionTestStore{}
	parent := transactionCallsNewTargetProcedure{
		target: transaction.NewTarget(transaction.NewKey("role", 1001), first),
		child: transactionNewTargetProcedure{
			target: transaction.NewTarget(transaction.NewKey("role", 1002), second),
		},
	}
	err := transaction.Call(context.Background(), parent)
	if err == nil || !strings.Contains(err.Error(), "transactionCallsNewTargetProcedure -> transactionNewTargetProcedure") || !strings.Contains(err.Error(), "role:1002") {
		t.Fatalf("Call error = %v, want procedure trace and new target", err)
	}
}

func TestTransactionCallRejectsChildProcedureWithDifferentStoreForHeldKey(t *testing.T) {
	rootStore := &transactionTestStore{}
	childStore := &transactionTestStore{}
	key := transaction.NewKey("role", 1001)
	parent := transactionCallsNewTargetProcedure{
		target: transaction.NewTarget(key, rootStore),
		child: transactionNewTargetProcedure{
			target: transaction.NewTarget(key, childStore),
		},
	}
	err := transaction.Call(context.Background(), parent)
	if err == nil || !strings.Contains(err.Error(), "different Store") || !strings.Contains(err.Error(), "role:1001") {
		t.Fatalf("Call error = %v, want different Store error", err)
	}
}

func TestTransactionRunRejectsNestedRun(t *testing.T) {
	store := &transactionTestStore{}
	target := transaction.NewTarget(transaction.NewKey("role", 1001), store)
	err := transaction.Run(context.Background(), transaction.Targets(target), func(tx *transaction.Tx) error {
		return transaction.Run(tx.Context(), transaction.Targets(target), func(*transaction.Tx) error {
			return nil
		})
	})
	if err == nil || !strings.Contains(err.Error(), "nested Run is not allowed") {
		t.Fatalf("nested Run error = %v, want rejected error", err)
	}
}
