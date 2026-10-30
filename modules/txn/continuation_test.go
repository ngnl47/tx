package txn

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type continuationProcedure struct {
	locks   Lockeys
	process func(*Tx) error
}

func (p continuationProcedure) Locks(context.Context) (Lockeys, error) { return p.locks, nil }
func (p continuationProcedure) Process(tx *Tx) error                   { return p.process(tx) }

func TestAfterCommitPanicDoesNotRunRollback(t *testing.T) {
	rolledBack := false
	func() {
		defer func() {
			if got := recover(); got != "notification failed" {
				t.Fatalf("panic = %v", got)
			}
		}()
		_ = NewRuntime().Call(t.Context(), continuationProcedure{process: func(tx *Tx) error {
			tx.AfterRollback(func(context.Context) { rolledBack = true })
			tx.AfterCommit(func(context.Context) error { panic("notification failed") })
			return nil
		}})
	}()
	if rolledBack {
		t.Fatal("已提交事务的通知 panic 不应触发回滚回调")
	}
}

func TestContinuationRunsAfterUnlockWithIndependentTransaction(t *testing.T) {
	runtime := NewRuntime()
	key := NewLockey("role", 1, 10)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var rootID TransactionID
	var events []string
	failure := errors.New("follow-up failed")
	first := continuationProcedure{locks: Lockeys{key}, process: func(tx *Tx) error {
		if tx.id == rootID {
			t.Fatal("后续过程复用了已提交事务")
		}
		events = append(events, "follow-up")
		return failure
	}}
	err := runtime.Call(ctx, continuationProcedure{locks: Lockeys{key}, process: func(tx *Tx) error {
		rootID = tx.id
		events = append(events, "root")
		if err := tx.CallAfterCommit(first); err != nil {
			return err
		}
		return tx.CallAfterCommit(continuationProcedure{process: func(*Tx) error {
			events = append(events, "unexpected")
			return nil
		}})
	}})
	var committed *AfterCommitError
	if !errors.As(err, &committed) || !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}
	if !reflect.DeepEqual(events, []string{"root", "follow-up"}) {
		t.Fatalf("events = %v", events)
	}
}

func TestContinuationRegistrationFollowsSavepointAndRootRollback(t *testing.T) {
	for _, failRoot := range []bool{false, true} {
		runs := 0
		next := continuationProcedure{process: func(*Tx) error { runs++; return nil }}
		failure := errors.New("rollback")
		err := NewRuntime().Call(t.Context(), continuationProcedure{process: func(tx *Tx) error {
			if err := tx.Call(continuationProcedure{process: func(child *Tx) error {
				if err := child.CallAfterCommit(next); err != nil {
					return err
				}
				return failure
			}}); !errors.Is(err, failure) {
				t.Fatalf("child error = %v", err)
			}
			if failRoot {
				if err := tx.CallAfterCommit(next); err != nil {
					return err
				}
				return failure
			}
			return nil
		}})
		if failRoot != errors.Is(err, failure) || runs != 0 {
			t.Fatalf("error=%v runs=%d", err, runs)
		}
	}
}

func TestContinuationIsDiscardedOnDeadlockRetry(t *testing.T) {
	attempts, continuations := 0, 0
	err := NewRuntime().Call(t.Context(), continuationProcedure{process: func(tx *Tx) error {
		attempts++
		if err := tx.CallAfterCommit(continuationProcedure{process: func(*Tx) error {
			continuations++
			return nil
		}}); err != nil {
			return err
		}
		if attempts == 1 {
			tx.deadlock = ErrDeadlock
		}
		return nil
	}})
	if err != nil || attempts != 2 || continuations != 1 {
		t.Fatalf("err=%v attempts=%d continuations=%d", err, attempts, continuations)
	}
}

func TestRuntimeDrainWaitsForAcceptedContinuation(t *testing.T) {
	runtime := NewRuntime()
	entered, proceed, following, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runtime.Call(t.Context(), continuationProcedure{process: func(tx *Tx) error {
			close(entered)
			<-proceed
			return tx.CallAfterCommit(continuationProcedure{process: func(*Tx) error {
				close(following)
				<-finish
				return nil
			}})
		}})
	}()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runtime.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain = %v", err)
	}
	if err := runtime.Call(t.Context(), continuationProcedure{process: func(*Tx) error {
		t.Fatal("排水后不应接受新根事务")
		return nil
	}}); err == nil {
		t.Fatal("新根事务未被拒绝")
	}
	close(proceed)
	select {
	case <-following:
	case err := <-done:
		t.Fatalf("后续调用提前结束：%v", err)
	case <-time.After(time.Second):
		t.Fatal("后续调用未执行")
	}
	if err := runtime.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("后续调用未完成时 Drain = %v", err)
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := runtime.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
}
