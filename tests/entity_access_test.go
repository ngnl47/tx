package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	txruntime "github.com/ngnl47/tx/runtime"
	"github.com/ngnl47/tx/storage"
)

type managedRoot struct {
	runtimeTestStore
	descriptor *entitycache.Descriptor
}

func (v *managedRoot) EntityDescriptor() *entitycache.Descriptor { return v.descriptor }
func (v *managedRoot) Store() *runtimeTestStore                  { return &v.runtimeTestStore }
func (v *managedRoot) LockTarget() txn.LockTarget {
	return txn.NewLockTarget(txn.NewLockKey("managed", 1001), v.Store())
}

type managedWriter struct {
	createWriter
	deleted   int
	deleteErr error
}

func (w *managedWriter) ApplyDelete(context.Context, meta.TypeSpec, storage.RecordKey) error {
	if w.deleteErr != nil {
		return w.deleteErr
	}
	w.deleted++
	w.exists = false
	return nil
}

type managedProvider struct {
	txruntime.Provider
	writer  *managedWriter
	manager *writeback.Manager
}

func (p managedProvider) Writer() storage.ChangeApplier { return p.writer }

type entityProcedure func(*txn.Tx) error

func (p entityProcedure) Locks(context.Context) ([]txn.LockTarget, error) { return nil, nil }
func (p entityProcedure) Process(tx *txn.Tx) error                        { return p(tx) }

func TestManagedEntityCreateFindRollbackRemoveAndRecreate(t *testing.T) {
	writer := &managedWriter{}
	manager := writeback.NewManager(writeback.Dependencies{Writer: writer}, writeback.Config{})
	cache, err := entitycache.NewRuntime(entitycache.Config{MaximumSize: 16}, func(ctx context.Context, value any) (bool, error) {
		return manager.TryRelease(ctx, value.(*managedRoot).LockTarget())
	})
	if err != nil {
		t.Fatal(err)
	}
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	if err = cache.Start(tasks); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tasks.Cancel(); tasks.Wait() })
	if err = manager.BindCreationResolver(cache); err != nil {
		t.Fatal(err)
	}
	executor := txn.NewRuntime()
	if err = executor.BindCommitParticipant(manager); err != nil {
		t.Fatal(err)
	}
	descriptor := &entitycache.Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) { return nil, false, nil }, Store: func(v any) storage.ChangeStore { return v.(*managedRoot).Store() }, Clone: func(v any) (any, error) { return &managedRoot{descriptor: v.(*managedRoot).descriptor}, nil }}
	ctx, end := entitycache.Begin(entitycache.Bind(txruntime.Bind(t.Context(), managedProvider{writer: writer, manager: manager}), cache))
	defer end()
	key := storage.RecordKey{ID: int64(1001)}
	root := &managedRoot{descriptor: descriptor}
	fail := errors.New("rollback")
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error {
		if err := txruntime.Create(tx.Context(), root); err != nil {
			return err
		}
		found, ok, err := cache.Find(tx.Context(), descriptor, key)
		if err != nil || !ok || found != root {
			t.Fatalf("读己写: %v %v", ok, err)
		}
		copy, ok, err := cache.Snapshot(tx.Context(), descriptor, key)
		if err != nil || !ok || copy == root {
			t.Fatal("创建快照读己写", err)
		}
		return fail
	})); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if _, ok, err := cache.Snapshot(ctx, descriptor, key); ok || err != nil {
		t.Fatalf("回滚后仍可见: %v %v", ok, err)
	}
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error { return txruntime.Create(tx.Context(), root) })); err != nil {
		t.Fatal(err)
	}
	// 提交后尚未落盘，快照已经可见；但不能作为可写缓存实体提交。
	copy, ok, err := cache.Snapshot(ctx, descriptor, key)
	if err != nil || !ok || copy == root {
		t.Fatal("创建快照", err)
	}
	if err = txruntime.Save(ctx, copy.(*managedRoot)); !errors.Is(err, entitycache.ErrProcedureRequired) {
		t.Fatal("快照允许保存", err)
	}
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error { return txruntime.Remove(tx.Context(), copy.(*managedRoot)) })); err == nil {
		t.Fatal("快照允许删除原对象")
	}
	if err = txruntime.Create(ctx, &managedRoot{descriptor: descriptor}); !errors.Is(err, entitycache.ErrProcedureRequired) {
		t.Fatal("无事务创建共享对象", err)
	}
	if err = txruntime.Remove(ctx, root); !errors.Is(err, entitycache.ErrProcedureRequired) {
		t.Fatal("无事务删除共享对象", err)
	}
	if err = manager.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if !writer.exists {
		t.Fatal("创建未落盘")
	}
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error {
		found, ok, err := cache.Find(tx.Context(), descriptor, key)
		if err != nil || !ok || found != root {
			return errors.New("未复用对象")
		}
		if err := txruntime.Remove(tx.Context(), root); err != nil {
			return err
		}
		if _, ok, err := cache.Find(tx.Context(), descriptor, key); ok || err != nil {
			t.Fatalf("事务删除后仍可见 %v %v", ok, err)
		}
		return fail
	})); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if found, ok, err := cache.Snapshot(ctx, descriptor, key); err != nil || !ok || found == root {
		t.Fatal("删除回滚失去对象", err)
	}
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error { return txruntime.Remove(tx.Context(), root) })); err != nil {
		t.Fatal(err)
	}
	writer.deleteErr = errors.New("backend down")
	if err = manager.Flush(ctx); err == nil {
		t.Fatal("删除错误被吞")
	}
	if err = txruntime.Save(ctx, root); !errors.Is(err, entitycache.ErrProcedureRequired) {
		t.Fatal("旧对象允许保存", err)
	}
	if _, ok, err := cache.Snapshot(ctx, descriptor, key); ok || err != nil {
		t.Fatal("删除未隐藏", err)
	}
	writer.deleteErr = nil
	if err = manager.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	replacement := &managedRoot{descriptor: descriptor}
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error { return txruntime.Create(tx.Context(), replacement) })); err != nil {
		t.Fatal("同 ID 重建", err)
	}
	if found, ok, err := cache.Snapshot(ctx, descriptor, key); err != nil || !ok || found == replacement {
		t.Fatal("重建身份", err)
	}
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error { return txruntime.Remove(tx.Context(), replacement) })); err != nil {
		t.Fatal(err)
	}
	if err = manager.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if writer.exists {
		t.Fatal("删除未完成")
	}

	fresh := &managedRoot{descriptor: descriptor}
	if err = executor.Call(ctx, entityProcedure(func(tx *txn.Tx) error {
		if err := txruntime.Create(tx.Context(), fresh); err != nil {
			return err
		}
		return txruntime.Remove(tx.Context(), fresh)
	})); err != nil {
		t.Fatal(err)
	}
	if err = manager.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if writer.exists {
		t.Fatal("同事务创建删除后仍落盘")
	}
}

func BenchmarkManagedEntityFind(b *testing.B) {
	manager := writeback.NewManager(writeback.Dependencies{Writer: &managedWriter{}}, writeback.Config{})
	root := &managedRoot{}
	descriptor := &entitycache.Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) { return root, true, nil }, Store: func(v any) storage.ChangeStore { return v.(*managedRoot).Store() }, Clone: func(v any) (any, error) { return &managedRoot{descriptor: v.(*managedRoot).descriptor}, nil }}
	root.descriptor = descriptor
	cache, err := entitycache.NewRuntime(entitycache.Config{MaximumSize: 16}, func(ctx context.Context, v any) (bool, error) {
		return manager.TryRelease(ctx, v.(*managedRoot).LockTarget())
	})
	if err != nil {
		b.Fatal(err)
	}
	tasks := lifecycle.NewWaitGroupContext(b.Context())
	if err = cache.Start(tasks); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { tasks.Cancel(); tasks.Wait() })
	if err = manager.BindCreationResolver(cache); err != nil {
		b.Fatal(err)
	}
	executor := txn.NewRuntime()
	if err = executor.BindCommitParticipant(manager); err != nil {
		b.Fatal(err)
	}
	key := storage.RecordKey{ID: int64(1001)}
	warm, end := entitycache.Begin(b.Context())
	if _, _, err = cache.Snapshot(warm, descriptor, key); err != nil {
		b.Fatal(err)
	}
	end()
	b.Run("Snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ctx, end := entitycache.Begin(b.Context())
			_, _, err := cache.Snapshot(ctx, descriptor, key)
			end()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Procedure", func(b *testing.B) {
		b.ReportAllocs()
		p := entityProcedure(func(tx *txn.Tx) error { _, _, err := cache.Find(tx.Context(), descriptor, key); return err })
		for b.Loop() {
			if err := executor.Call(b.Context(), p); err != nil {
				b.Fatal(err)
			}
		}
	})
}
