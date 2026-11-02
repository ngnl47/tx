package txn

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeAfterRollbackRunsOutsideRootTransaction(t *testing.T) {
	for _, mode := range []string{"error", "panic", "child_panic"} {
		t.Run(mode, func(t *testing.T) {
			runtime := NewRuntime()
			key := NewLockey("rollback", 1, mode)
			store := &lockeyTestStore{}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			failure := errors.New("root failed")
			callbacks := 0
			var callErr error
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				callErr = runtime.Call(ctx, continuationProcedure{locks: Lockeys{key}, process: func(tx *Tx) error {
					if err := tx.Enlist(NewStoreTarget(key, store)); err != nil {
						return err
					}
					store.set(1)
					callback := func(callbackCtx context.Context) {
						callbacks++
						if callbackCtx != ctx {
							t.Error("根回滚回调未收到原操作上下文")
						}
						if _, active := FromContext(tx.Context()); active {
							t.Error("根回滚回调执行时事务仍有效")
						}
						if store.value != 0 || store.recorder != nil {
							t.Errorf("根回滚回调执行前未完成恢复和解绑：value=%d recorder=%T", store.value, store.recorder)
						}
						if err := runtime.Call(ctx, continuationProcedure{locks: Lockeys{key}, process: func(*Tx) error { return nil }}); err != nil {
							t.Errorf("根回滚回调无法重新取得同一锁：%v", err)
						}
					}
					tx.AfterRollback(callback)
					if mode == "child_panic" {
						return tx.Call(continuationProcedure{process: func(child *Tx) error {
							child.AfterRollback(callback)
							store.set(2)
							panic(failure)
						}})
					}
					if mode == "panic" {
						panic(failure)
					}
					return failure
				}})
			}()
			if mode == "error" {
				if !errors.Is(callErr, failure) || panicked != nil {
					t.Fatalf("Call error=%v panic=%v", callErr, panicked)
				}
			} else if panicked != failure {
				t.Fatalf("panic=%v，预期原始异常", panicked)
			}
			wantCallbacks := 1
			if mode == "child_panic" {
				wantCallbacks = 2
			}
			if callbacks != wantCallbacks {
				t.Fatalf("回滚回调次数=%d，预期=%d", callbacks, wantCallbacks)
			}
		})
	}
}

func TestAfterRollbackForChildSavepointKeepsRootActive(t *testing.T) {
	for _, failRoot := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[failRoot], func(t *testing.T) {
			key := NewLockey("rollback-child", 1, 1)
			store := &lockeyTestStore{}
			failure := errors.New("savepoint failed")
			rootCallbacks, childCallbacks := 0, 0
			err := NewRuntime().Call(t.Context(), continuationProcedure{locks: Lockeys{key}, process: func(tx *Tx) error {
				if err := tx.Enlist(NewStoreTarget(key, store)); err != nil {
					return err
				}
				store.set(1)
				tx.AfterRollback(func(context.Context) { rootCallbacks++ })
				err := tx.Call(continuationProcedure{locks: Lockeys{key}, process: func(child *Tx) error {
					store.set(2)
					child.AfterRollback(func(ctx context.Context) {
						childCallbacks++
						if current, active := FromContext(ctx); !active || current != tx {
							t.Error("子 savepoint 回滚回调丢失活跃根事务")
						}
						if store.value != 1 || store.recorder == nil {
							t.Errorf("子回滚破坏根修改或解绑 Store：value=%d recorder=%T", store.value, store.recorder)
						}
						store.set(3)
					})
					return failure
				}})
				if !errors.Is(err, failure) || childCallbacks != 1 || rootCallbacks != 0 || store.value != 3 {
					t.Errorf("子回滚未在返回前独立完成：err=%v child=%d root=%d value=%d", err, childCallbacks, rootCallbacks, store.value)
				}
				if failRoot {
					return failure
				}
				return nil
			}})
			wantValue, wantRootCallbacks := 3, 0
			if failRoot {
				wantValue, wantRootCallbacks = 0, 1
			}
			if errors.Is(err, failure) != failRoot || store.value != wantValue || rootCallbacks != wantRootCallbacks || childCallbacks != 1 {
				t.Fatalf("根收尾错误：err=%v value=%d root=%d child=%d", err, store.value, rootCallbacks, childCallbacks)
			}
		})
	}
}

func TestChildAfterRollbackPanicRunsOnceAndFinishesRootRollback(t *testing.T) {
	runtime := NewRuntime()
	key := NewLockey("rollback-callback", 1, 1)
	store := &lockeyTestStore{}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	failure := errors.New("rollback callback failed")
	rootCallbacks, childCallbacks := 0, 0
	func() {
		defer func() {
			if got := recover(); got != failure {
				t.Errorf("panic=%v，预期原始回调异常", got)
			}
		}()
		_ = runtime.Call(ctx, continuationProcedure{locks: Lockeys{key}, process: func(tx *Tx) error {
			if err := tx.Enlist(NewStoreTarget(key, store)); err != nil {
				return err
			}
			store.set(1)
			tx.AfterRollback(func(callbackCtx context.Context) {
				rootCallbacks++
				if _, active := FromContext(tx.Context()); active || callbackCtx != ctx || store.value != 0 || store.recorder != nil {
					t.Error("子回滚回调 panic 后根事务未清理完成")
				}
				if err := runtime.Call(ctx, continuationProcedure{locks: Lockeys{key}, process: func(*Tx) error { return nil }}); err != nil {
					t.Errorf("子回滚回调 panic 后根锁未释放：%v", err)
				}
			})
			return tx.Call(continuationProcedure{process: func(child *Tx) error {
				store.set(2)
				child.AfterRollback(func(context.Context) {
					childCallbacks++
					if childCallbacks == 1 {
						store.set(3)
					}
					panic(failure)
				})
				return errors.New("child failed")
			}})
		}})
	}()
	if childCallbacks != 1 || rootCallbacks != 1 {
		t.Fatalf("回滚回调次数 child=%d root=%d，预期各一次", childCallbacks, rootCallbacks)
	}
}

func TestRootAfterRollbackPanicKeepsNextTransactionRecorder(t *testing.T) {
	runtime := NewRuntime()
	key := NewLockey("rollback-reentry", 1, 1)
	store := &lockeyTestStore{}
	setValue := func(value int) {
		old := store.value
		if store.recorder != nil {
			store.recorder.RecordUndo(func() { store.value = old })
		}
		store.value = value
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	failure := errors.New("transaction failed")
	callbackPanic := errors.New("root rollback callback failed")
	ready, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	finished := false
	func() {
		defer close(release)
		defer func() {
			if got := recover(); got != callbackPanic {
				t.Errorf("panic=%v，预期原始回调异常", got)
			}
		}()
		_ = runtime.Call(ctx, continuationProcedure{locks: Lockeys{key}, process: func(tx *Tx) error {
			if err := tx.Enlist(NewStoreTarget(key, store)); err != nil {
				return err
			}
			setValue(1)
			tx.OnFinish(func() { finished = true })
			tx.AfterRollback(func(callbackCtx context.Context) {
				go func() {
					done <- runtime.Call(callbackCtx, continuationProcedure{locks: Lockeys{key}, process: func(next *Tx) error {
						if err := next.Enlist(NewStoreTarget(key, store)); err != nil {
							return err
						}
						close(ready)
						<-release
						setValue(2)
						return failure
					}})
				}()
				select {
				case <-ready:
				case <-ctx.Done():
					t.Error("后续根事务未能取得同一对象")
				}
				panic(callbackPanic)
			})
			return failure
		}})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, failure) {
			t.Fatalf("后续事务错误=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("后续事务未结束")
	}
	if !finished {
		t.Error("回滚回调 panic 后未执行 OnFinish")
	}
	if store.value != 0 {
		t.Fatalf("前一回调 panic 破坏后续事务回滚，value=%d，预期=0", store.value)
	}
}
