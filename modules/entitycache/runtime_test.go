package entitycache

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/modules/txn"
	"testing"

	"github.com/maypok86/otter/v2"

	"github.com/ngnl47/tx/storage"
)

func TestRuntimeSeparatesTypesAndRecordRoutes(t *testing.T) {
	r, err := NewRuntime(Config{MaximumSize: 16}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.cache.policy.StopAllGoroutines()
	loads := 0
	d := &Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) {
		loads++
		return &runtimeValue{id: int64(loads)}, true, nil
	}}
	other := &Descriptor{Load: d.Load}
	ctx, end := Begin(t.Context())
	defer end()
	err = txn.NewRuntime().Call(ctx, scopeProcedure(func(tx *txn.Tx) error {
		ctx := tx.Context()
		a, _, err := r.Find(ctx, d, storage.RecordKey{ID: int64(1), ShardKey: int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		b, _, _ := r.Find(ctx, d, storage.RecordKey{ID: int64(1), ShardKey: int64(1)})
		c, _, _ := r.Find(ctx, d, storage.RecordKey{ID: int64(1), ShardKey: int64(2)})
		e, _, _ := r.Find(ctx, other, storage.RecordKey{ID: int64(1), ShardKey: int64(1)})
		if a != b || a == c || a == e || loads != 3 {
			t.Fatalf("cache identity: loads=%d", loads)
		}
		r2, _ := NewRuntime(Config{MaximumSize: 16}, nil)
		defer r2.cache.policy.StopAllGoroutines()
		f, _, _ := r2.Find(ctx, d, storage.RecordKey{ID: int64(1), ShardKey: int64(1)})
		if a == f {
			t.Fatal("独立运行时错误共享对象")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
}

// 删除后同 ID 重建，旧 policy token 和延期候选不能滞留或覆盖新一代对象。
func TestRemovalRetiresPolicyAndDeferredEntry(t *testing.T) {
	r, err := NewRuntime(Config{MaximumSize: 16}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.cache.policy.StopAllGoroutines()
	d := &Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) { return &runtimeValue{id: 1}, true, nil }}
	ctx, end := Begin(t.Context())
	defer end()
	if _, err = r.cache.Use(ctx, identity{d, storage.RecordKey{ID: int64(1)}}, func(*entity) error { return nil }); err != nil {
		t.Fatal(err)
	}
	id := identity{d, storage.RecordKey{ID: int64(1)}}
	entry := r.cache.values[id]
	event := otter.DeletionEvent[identity, *cacheEntry[*entity]]{Key: id, Value: entry, Cause: otter.CauseOverflow}
	r.cache.onAtomicDeletion(event)
	(&removal{runtime: r, entry: entry.value}).Complete()
	if _, exists := r.cache.policy.GetIfPresent(id); exists {
		t.Fatal("删除后仍保留旧 policy token")
	}
	r.cache.onAtomicDeletion(event)
	if len(r.cache.deferred) != 0 {
		t.Fatal("已删除对象重新进入延期回收")
	}
}

type runtimeValue struct{ id int64 }

func (v *runtimeValue) SetUndoRecorder(change.UndoRecorder) {}
func (v *runtimeValue) LockTarget() txn.LockTarget {
	return txn.NewLockTarget(txn.NewLockKey("runtime-value", v.id), v)
}

// 取消只结束等待意愿，不代表仍在执行的 Procedure 已归还对象。
func TestRuntimeProcedureRetainsAfterCancellation(t *testing.T) {
	r, _ := NewRuntime(Config{MaximumSize: 16}, nil)
	defer r.cache.policy.StopAllGoroutines()
	d := &Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) { return &runtimeValue{id: 1}, true, nil }}
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	var escaped context.Context
	id := identity{d, storage.RecordKey{ID: int64(1)}}
	err := txn.NewRuntime().Call(parent, scopeProcedure(func(tx *txn.Tx) error {
		escaped = tx.Context()
		first, _, err := r.Find(tx.Context(), d, id.record)
		if err != nil {
			return err
		}
		again, _, err := r.Find(tx.Context(), d, id.record)
		if err != nil || first != again {
			t.Fatal("事务重复查询失去身份", err)
		}
		cancel()
		entry := r.cache.values[id]
		r.cache.tryRelease(t.Context(), deferredEntry[identity, *entity]{key: id, entry: entry, generation: entry.generation.Load()})
		if r.cache.values[id] != entry || entry.users == 0 {
			t.Fatal("取消提前归还对象")
		}
		return nil
	}))
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if r.cache.values[id].users != 0 {
		t.Fatal("事务结束未释放")
	}
	if _, _, err = r.Find(escaped, d, id.record); !errors.Is(err, txn.ErrInactive) {
		t.Fatal("失效事务仍可读取", err)
	}
}

func TestSnapshotCopyFailureReleasesUsage(t *testing.T) {
	for _, panicCopy := range []bool{false, true} {
		r, _ := NewRuntime(Config{MaximumSize: 16}, nil)
		func() {
			defer r.cache.policy.StopAllGoroutines()
			failure := errors.New("copy failed")
			d := &Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) { return &runtimeValue{id: 1}, true, nil }, Clone: func(any) (any, error) {
				if panicCopy {
					panic(failure)
				}
				return nil, failure
			}}
			func() {
				defer func() {
					if value := recover(); (value != nil) != panicCopy {
						t.Errorf("panic=%v", value)
					}
				}()
				if _, _, err := r.Snapshot(t.Context(), d, storage.RecordKey{ID: int64(1)}); !errors.Is(err, failure) {
					t.Fatal(err)
				}
			}()
			entry := r.cache.values[identity{d, storage.RecordKey{ID: int64(1)}}]
			if entry.users != 0 {
				t.Fatal("复制失败泄漏使用权")
			}
			// WithLocks 的 defer 同时释放记录锁，后续事务不能被复制错误阻断。
			if err := txn.NewRuntime().Call(t.Context(), scopeProcedure(func(tx *txn.Tx) error {
				_, _, err := r.Find(tx.Context(), d, storage.RecordKey{ID: int64(1)})
				return err
			})); err != nil {
				t.Fatal(err)
			}
		}()
	}
}

// 冷加载对象发布给并发读者前，必须完成惰性 Store/锁目标初始化。
func TestRuntimePreparesLockBeforePublishing(t *testing.T) {
	r, err := NewRuntime(Config{MaximumSize: 16}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.cache.policy.StopAllGoroutines()
	d := &Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) {
		return &lazyLockValue{runtimeValue: runtimeValue{id: 1}}, true, nil
	}}
	_, err = r.cache.Use(t.Context(), identity{d, storage.RecordKey{ID: int64(1)}}, func(entry *entity) error {
		if !entry.value.(*lazyLockValue).prepared {
			t.Fatal("共享缓存发布了未初始化锁目标的对象")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type lazyLockValue struct {
	runtimeValue
	prepared bool
}

func (v *lazyLockValue) LockTarget() txn.LockTarget {
	if !v.prepared {
		v.prepared = true
	}
	return v.runtimeValue.LockTarget()
}
