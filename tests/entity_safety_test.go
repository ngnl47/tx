package tests

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/storage"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedEntityFindRejectsNonProcedureBeforeLoading(t *testing.T) {
	loads := 0
	root := &managedRoot{}
	d := &entitycache.Descriptor{Load: func(context.Context, storage.RecordKey) (any, bool, error) { loads++; return root, true, nil }, Store: func(v any) storage.ChangeStore { return v.(*managedRoot).Store() }}
	root.descriptor = d
	cache, err := entitycache.NewRuntime(entitycache.Config{MaximumSize: 16}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	if err = cache.Start(tasks); err != nil {
		t.Fatal(err)
	}
	defer func() { tasks.Cancel(); tasks.Wait() }()
	ctx, end := entitycache.Begin(t.Context())
	defer end()
	if _, _, err = cache.Find(ctx, d, storage.RecordKey{ID: int64(1001)}); err == nil {
		t.Fatal("共享可变 GD 在无 Procedure 时仍然返回")
	}
	if loads != 0 {
		t.Fatal("拒绝应发生在存储加载之前")
	}
}

// mapEntity 用最小 Store 夹具验证记录锁覆盖真实 map 复制和 WriteBack 读取。
type mapEntity struct {
	runtimeTestStore
	id         int64
	values     map[string]int64
	descriptor *entitycache.Descriptor
}

func (v *mapEntity) EntityDescriptor() *entitycache.Descriptor { return v.descriptor }
func (v *mapEntity) Store() *mapEntity                         { return v }
func (v *mapEntity) RecordKey() storage.RecordKey              { return storage.RecordKey{ID: v.id} }
func (v *mapEntity) StoreTarget() txn.StoreTarget {
	return txn.NewStoreTarget(txn.NewLockey("map-entity", 0, v.id), v)
}
func (v *mapEntity) ResolveValue(change.Path) (any, bool) { return v.values, true }
func (v *mapEntity) increment() {
	previous := maps.Clone(v.values)
	if v.recorder != nil {
		v.recorder.RecordUndo(func() { v.values = previous })
	}
	v.Set()
	v.values["counter"]++
	v.values["large"] = 9007199254740993
}

type mapWriter struct {
	mu     sync.Mutex
	rows   map[int64]map[string]int64
	failed atomic.Bool
	calls  atomic.Int64
}

func (w *mapWriter) ApplyChangesWithOptions(_ context.Context, _ meta.TypeSpec, key storage.RecordKey, _ change.Source, _ change.FieldNameResolver, resolver change.ValueResolver, _ storage.ApplyOptions) error {
	w.calls.Add(1)
	if w.failed.Load() {
		return errors.New("storage unavailable")
	}
	value, _ := resolver.ResolveValue(change.Path{})
	copy := maps.Clone(value.(map[string]int64))
	w.mu.Lock()
	w.rows[key.ID.(int64)] = copy
	w.mu.Unlock()
	return nil
}

func TestSharedEntitySnapshotWritebackAndEviction(t *testing.T) {
	writer := &mapWriter{rows: make(map[int64]map[string]int64)}
	manager := writeback.NewManager(writeback.Dependencies{Writer: writer}, writeback.Config{})
	cache, err := entitycache.NewRuntime(entitycache.Config{MaximumSize: 1, IdleTimeout: time.Millisecond}, func(ctx context.Context, value any) (bool, error) {
		return manager.TryRelease(ctx, value.(*mapEntity).StoreTarget())
	})
	if err != nil {
		t.Fatal(err)
	}
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	if err = cache.Start(tasks); err != nil {
		t.Fatal(err)
	}
	defer func() { tasks.Cancel(); tasks.Wait() }()
	var d *entitycache.Descriptor
	d = &entitycache.Descriptor{
		Load: func(_ context.Context, key storage.RecordKey) (any, bool, error) {
			writer.mu.Lock()
			values := maps.Clone(writer.rows[key.ID.(int64)])
			writer.mu.Unlock()
			if values == nil {
				values = map[string]int64{}
			}
			return &mapEntity{id: key.ID.(int64), values: values, descriptor: d}, true, nil
		},
		Store: func(v any) storage.ChangeStore { return v.(*mapEntity) },
		Clone: func(v any) (any, error) {
			root := v.(*mapEntity)
			return &mapEntity{id: root.id, values: maps.Clone(root.values), descriptor: d}, nil
		},
	}
	executor := txn.NewRuntime()
	if err = executor.BindCommitParticipant(manager); err != nil {
		t.Fatal(err)
	}
	key := storage.RecordKey{ID: int64(1)}
	modify := entityProcedure(func(tx *txn.Tx) error {
		value, _, err := cache.Find(tx.Context(), d, key)
		if err != nil {
			return err
		}
		value.(*mapEntity).increment()
		return nil
	})
	writer.failed.Store(true)
	if err = executor.Call(t.Context(), modify); err != nil {
		t.Fatal(err)
	}
	if err = manager.Flush(t.Context()); err == nil || manager.Pending() != 1 {
		t.Fatal("写入失败丢失 pending", err)
	}
	snapshot, _, err := cache.Snapshot(t.Context(), d, key)
	if err != nil || snapshot.(*mapEntity).values["counter"] != 1 {
		t.Fatal("失败写回丢失内存值", err)
	}
	writer.failed.Store(false)
	var committed atomic.Int64
	committed.Store(1)
	var group sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 300; i++ {
				switch worker {
				case 0:
					err := executor.Call(t.Context(), modify)
					if errors.Is(err, entitycache.ErrReleasing) {
						continue
					}
					if err != nil {
						t.Error(err)
						return
					}
					committed.Add(1)
				case 1:
					value, ok, err := cache.Snapshot(t.Context(), d, key)
					if err != nil || !ok {
						t.Error("snapshot", err)
						return
					}
					copy := value.(*mapEntity)
					if copy.values["large"] != 9007199254740993 {
						t.Error("快照精度或身份错误")
						return
					}
					copy.values["counter"] = -1
				case 2:
					if err := manager.Flush(t.Context()); err != nil {
						t.Error(err)
						return
					}
					if _, _, err := cache.Snapshot(t.Context(), d, storage.RecordKey{ID: int64(2 + i)}); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	group.Wait()
	if err = manager.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	snapshot, ok, err := cache.Snapshot(t.Context(), d, key)
	if err != nil || !ok || snapshot.(*mapEntity).values["counter"] != committed.Load() {
		t.Fatal("并发快照/回收丢失提交", committed.Load(), snapshot, err)
	}
	if manager.Pending() != 0 || writer.calls.Load() < 2 {
		t.Fatal("写回未执行或未排空")
	}
}
