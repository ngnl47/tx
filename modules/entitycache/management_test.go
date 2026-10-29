package entitycache

import (
	"context"
	"errors"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

type observedValue struct {
	runtimeValue
	fields map[int]int
}

func managementFixture(t *testing.T, release func(context.Context, any) (bool, error)) (*Runtime, *Descriptor, *int) {
	t.Helper()
	r, err := NewRuntime(Config{MaximumSize: 16}, release)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.cache.policy.StopAllGoroutines() })
	loads := 0
	d := &Descriptor{TypeName: "Observed", Load: func(_ context.Context, key storage.RecordKey) (any, bool, error) {
		loads++
		return &observedValue{runtimeValue: runtimeValue{id: key.ID.(int64)}, fields: map[int]int{1: 10}}, true, nil
	}, Clone: func(v any) (any, error) {
		old := v.(*observedValue)
		copy := &observedValue{runtimeValue: old.runtimeValue, fields: map[int]int{}}
		for k, v := range old.fields {
			copy.fields[k] = v
		}
		return copy, nil
	}}
	if _, _, err := r.Snapshot(t.Context(), d, storage.RecordKey{ID: int64(1)}); err != nil {
		t.Fatal(err)
	}
	return r, d, &loads
}

func firstRef(t *testing.T, r *Runtime) EntryRef {
	t.Helper()
	page, err := r.List(t.Context(), ListOptions{TypeName: "Observed", Limit: 1})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("列表：%+v %v", page, err)
	}
	return page.Items[0].Ref
}

func TestManagementObservationAndIncarnation(t *testing.T) {
	r, d, loads := managementFixture(t, nil)
	ref := firstRef(t, r)
	stats, err := r.Stats(t.Context())
	if err != nil || stats.Entries != 1 || stats.MaximumSize != 16 {
		t.Fatal(stats, err)
	}
	id := identity{d, storage.RecordKey{ID: int64(1)}}
	generation := r.cache.values[id].generation.Load()
	value, err := r.SnapshotCached(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	value.(*observedValue).fields[1] = 99
	value, err = r.SnapshotCached(t.Context(), ref)
	if err != nil || value.(*observedValue).fields[1] != 10 || *loads != 1 || r.cache.values[id].generation.Load() != generation {
		t.Fatal("观察改变缓存或发生加载", err)
	}
	result, err := r.TryRelease(t.Context(), ref)
	if err != nil || result != Released {
		t.Fatal(result, err)
	}
	if _, err = r.SnapshotCached(t.Context(), ref); !errors.Is(err, ErrNotCached) || *loads != 1 {
		t.Fatal("管理快照不能回源", err)
	}
	if _, _, err = r.Snapshot(t.Context(), d, id.record); err != nil {
		t.Fatal(err)
	}
	if result, err = r.TryRelease(t.Context(), ref); err != nil || result != Stale {
		t.Fatal("旧引用回收了新对象", result, err)
	}
	if _, err = r.SnapshotCached(t.Context(), ref); !errors.Is(err, ErrStaleReference) {
		t.Fatal(err)
	}
	other, _, _ := managementFixture(t, nil)
	if _, err = other.TryRelease(t.Context(), firstRef(t, r)); !errors.Is(err, ErrInvalidReference) {
		t.Fatal(err)
	}
	if _, err = r.TryRelease(t.Context(), EntryRef{}); !errors.Is(err, ErrInvalidReference) {
		t.Fatal(err)
	}
}

func TestManagementRetainsBusyAndUnsafeEntries(t *testing.T) {
	failure := errors.New("保存未完成")
	safe := false
	releaseErr := error(nil)
	r, d, _ := managementFixture(t, func(context.Context, any) (bool, error) { return safe, releaseErr })
	ref := firstRef(t, r)
	for _, wantErr := range []error{nil, failure} {
		releaseErr = wantErr
		result, err := r.TryRelease(t.Context(), ref)
		if result != Retained || !errors.Is(err, wantErr) {
			t.Fatal(result, err)
		}
	}
	releaseErr = nil
	err := txn.NewRuntime().Call(t.Context(), scopeProcedure(func(tx *txn.Tx) error {
		if _, _, err := r.Find(tx.Context(), d, storage.RecordKey{ID: int64(1)}); err != nil {
			return err
		}
		result, err := r.TryRelease(t.Context(), ref)
		if err != nil || result != Busy {
			t.Fatal(result, err)
		}
		if _, err = r.TryRelease(tx.Context(), ref); !errors.Is(err, ErrManagementInProcedure) {
			t.Fatal(err)
		}
		if _, err = r.SnapshotCached(tx.Context(), ref); !errors.Is(err, ErrManagementInProcedure) {
			t.Fatal(err)
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	safe = true
	if result, err := r.TryRelease(t.Context(), ref); err != nil || result != Released {
		t.Fatal(result, err)
	}
}

func TestManagementDrainWaitsForActualSnapshotExit(t *testing.T) {
	r, d, _ := managementFixture(t, nil)
	ref := firstRef(t, r)
	entered, leave := make(chan struct{}), make(chan struct{})
	clone := d.Clone
	d.Clone = func(v any) (any, error) { close(entered); <-leave; return clone(v) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := r.SnapshotCached(ctx, ref); done <- err }()
	<-entered
	cancel()
	timeout, stop := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer stop()
	if err := r.DrainManagement(timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("取消不能提前释放复制", err)
	}
	if _, err := r.SnapshotCached(t.Context(), ref); !errors.Is(err, ErrManagementDraining) {
		t.Fatal(err)
	}
	if _, err := r.Stats(t.Context()); err != nil {
		t.Fatal("排水仍可观察", err)
	}
	close(leave)
	<-done
	if err := r.DrainManagement(t.Context()); err != nil {
		t.Fatal(err)
	}
	// 管理排水不能阻断既有业务。
	if err := txn.NewRuntime().Call(t.Context(), scopeProcedure(func(tx *txn.Tx) error {
		_, _, err := r.Find(tx.Context(), d, storage.RecordKey{ID: int64(1)})
		return err
	})); err != nil {
		t.Fatal(err)
	}
	if err := r.CloseManagement(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Stats(t.Context()); !errors.Is(err, ErrManagementClosed) {
		t.Fatal(err)
	}
	if _, err := r.List(t.Context(), ListOptions{}); !errors.Is(err, ErrManagementClosed) {
		t.Fatal(err)
	}
}

func TestManagementListBoundedAndSnapshotFailureReleases(t *testing.T) {
	r, d, _ := managementFixture(t, nil)
	if _, _, err := r.Snapshot(t.Context(), d, storage.RecordKey{ID: int64(2)}); err != nil {
		t.Fatal(err)
	}
	page, err := r.List(t.Context(), ListOptions{Limit: 1})
	if err != nil || !page.Truncated || len(page.Items) != 1 {
		t.Fatal(page, err)
	}
	ref := page.Items[0].Ref
	d.Clone = func(any) (any, error) { panic("复制失败") }
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("未触发复制异常")
			}
		}()
		_, _ = r.SnapshotCached(t.Context(), ref)
	}()
	if result, err := r.TryRelease(t.Context(), ref); err != nil || result != Released {
		t.Fatal("复制异常泄漏使用权", result, err)
	}
	if err := r.DrainManagement(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// 管理快照与 Procedure 共用记录锁，复制时不能读到一半的 map 更新。
func TestManagementSnapshotSerializesWithProcedure(t *testing.T) {
	r, d, _ := managementFixture(t, nil)
	ref := firstRef(t, r)
	result := make(chan error, 1)
	go func() {
		executor := txn.NewRuntime()
		for i := 0; i < 100; i++ {
			err := executor.Call(t.Context(), scopeProcedure(func(tx *txn.Tx) error {
				value, _, err := r.Find(tx.Context(), d, storage.RecordKey{ID: int64(1)})
				if err != nil {
					return err
				}
				fields := value.(*observedValue).fields
				fields[1] = i
				fields[2] = 10 - i
				return nil
			}))
			if err != nil {
				result <- err
				return
			}
		}
		result <- nil
	}()
	for i := 0; i < 100; i++ {
		value, err := r.SnapshotCached(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		fields := value.(*observedValue).fields
		if fields[1]+fields[2] != 10 {
			t.Fatal("复制读到中间态", fields)
		}
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

// 首次加载与释放中的条目只展示元数据；管理请求不参与加载等待或返回半成品。
func TestManagementTransitionalEntries(t *testing.T) {
	r, err := NewRuntime(Config{MaximumSize: 16}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.cache.policy.StopAllGoroutines()
	entered, leave := make(chan struct{}), make(chan struct{})
	d := &Descriptor{TypeName: "Observed", Load: func(context.Context, storage.RecordKey) (any, bool, error) {
		close(entered)
		<-leave
		return &runtimeValue{id: 1}, true, nil
	}, Clone: func(v any) (any, error) { copy := *v.(*runtimeValue); return &copy, nil }}
	done := make(chan error, 1)
	go func() { _, _, err := r.Snapshot(t.Context(), d, storage.RecordKey{ID: int64(1)}); done <- err }()
	<-entered
	page, err := r.List(t.Context(), ListOptions{})
	if err != nil || len(page.Items) != 1 || page.Items[0].State != EntryLoading {
		t.Fatal(page, err)
	}
	ref := page.Items[0].Ref
	if _, err = r.SnapshotCached(t.Context(), ref); !errors.Is(err, ErrEntryBusy) {
		t.Fatal(err)
	}
	if result, err := r.TryRelease(t.Context(), ref); err != nil || result != Busy {
		t.Fatal(result, err)
	}
	close(leave)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err = r.SnapshotCached(t.Context(), ref); err != nil {
		t.Fatal("加载结束应沿用本次驻留引用", err)
	}
}

// 冷加载发布与手动回收并发时，Released 后不能又由加载协程将旧 GD 塞回 policy。
func TestManagementReleaseCannotBeRepublishedByLoader(t *testing.T) {
	r, _, _ := managementFixture(t, nil)
	d := &Descriptor{TypeName: "Publishing", Load: func(_ context.Context, key storage.RecordKey) (any, bool, error) {
		return &runtimeValue{id: key.ID.(int64)}, true, nil
	}}
	for i := int64(2); i < 2002; i++ {
		id := identity{d, storage.RecordKey{ID: i}}
		done := make(chan error, 1)
		go func() { _, err := r.cache.loadMissing(t.Context(), id); done <- err }()
		for {
			r.cache.mu.Lock()
			entry := r.cache.values[id]
			if entry == nil || entry.loading {
				r.cache.mu.Unlock()
				goruntime.Gosched()
				continue
			}
			ref := EntryRef{r.management, id, entry.incarnation}
			r.cache.mu.Unlock()
			result, err := r.TryRelease(t.Context(), ref)
			if err != nil || result != Released {
				t.Fatal(result, err)
			}
			break
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if _, exists := r.cache.policy.GetIfPresent(id); exists {
			t.Fatalf("回收后重新发布旧对象，记录 %d", i)
		}
	}
}
