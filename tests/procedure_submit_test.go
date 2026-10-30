package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	txruntime "github.com/ngnl47/tx/runtime"
	"github.com/ngnl47/tx/storage"
)

type submitProvider struct {
	txruntime.Provider
	executor *txn.Runtime
}

func (p submitProvider) Procedure() txn.Executor { return p.executor }

func TestSubmitDetachesRequestScopeAndKeepsRuntime(t *testing.T) {
	executor := txn.NewRuntime()
	cache, err := entitycache.NewRuntime(entitycache.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := &entitycache.Descriptor{Store: func(v any) storage.ChangeStore { return v.(*managedRoot).Store() }}
	descriptor.Load = func(context.Context, storage.RecordKey) (any, bool, error) {
		return &managedRoot{descriptor: descriptor}, true, nil
	}
	parent, cancel := context.WithCancel(t.Context())
	ctx, end := entitycache.Begin(entitycache.Bind(txruntime.Bind(parent, submitProvider{executor: executor}), cache))
	proceed := make(chan struct{})
	f, err := txruntime.Submit(ctx, entityProcedure(func(tx *txn.Tx) error {
		<-proceed
		if txruntime.Procedure(tx.Context()) != executor || entitycache.FromContext(tx.Context()) != cache {
			return fmt.Errorf("运行时能力未保留")
		}
		if err := tx.Context().Err(); err != nil {
			return err
		}
		_, exists, err := cache.Find(tx.Context(), descriptor, storage.RecordKey{ID: int64(1001)})
		if err == nil && !exists {
			return fmt.Errorf("未取得实体")
		}
		return err
	}))
	if err != nil {
		t.Fatal(err)
	}
	end()
	cancel()
	close(proceed)
	if err := f.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := executor.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitWriteBackCommitAndRollback(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint(rollback), func(t *testing.T) {
			store := &runtimeTestStore{typeName: "GDRole"}
			writer := &recordingChangeStoreApplier{}
			executor, manager := newWriteBackRuntime(t, writer, writeback.Config{})
			if err := executor.ConfigureAsync(txn.AsyncConfig{MaxConcurrent: 1, MaxPending: 2}); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			blocker, err := executor.Submit(t.Context(), entityProcedure(func(*txn.Tx) error { close(entered); <-release; return nil }))
			if err != nil {
				t.Fatal(err)
			}
			<-entered
			f, err := executor.Submit(t.Context(), sharedStoreProcedure{targets: []txn.StoreTarget{txn.NewStoreTarget(txn.NewLockey("RoleLock", 1, 1001), store)}, stores: []*runtimeTestStore{store}, fail: rollback})
			if err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := blocker.Await(t.Context()); err != nil {
				t.Fatal(err)
			}
			err = f.Await(t.Context())
			if rollback {
				if err == nil || manager.Pending() != 0 || store.HasChanges() {
					t.Fatalf("rollback: %v pending=%d", err, manager.Pending())
				}
			} else {
				if err != nil || manager.Pending() != 1 {
					t.Fatalf("commit: %v pending=%d", err, manager.Pending())
				}
				writer.err = errors.New("存储暂不可用")
				if err := manager.Flush(t.Context()); err == nil || manager.Pending() != 1 || !store.HasChanges() {
					t.Fatalf("失败未保留写回: %v", err)
				}
				if err := f.Await(t.Context()); err != nil {
					t.Fatal("落盘失败不能改变已提交结果", err)
				}
				writer.err = nil
				if err := manager.Flush(t.Context()); err != nil {
					t.Fatal(err)
				}
				if store.HasChanges() || manager.Pending() != 0 {
					t.Fatal("写回未完成")
				}
			}
		})
	}
}
