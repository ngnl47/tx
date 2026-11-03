package discovery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

func TestSlotCacheLateFailureAndSnapshotKeepNewRoute(t *testing.T) {
	d := NewSlotDirectory()
	old := SlotAssignment{"slot", "old", 1}
	next := SlotAssignment{"slot", "new", 2}
	if err := d.ReplaceSnapshot([]SlotAssignment{old}, 10); err != nil {
		t.Fatal(err)
	}
	if !d.invalidateAssignment(old) {
		t.Fatal("旧归属应失效")
	}
	if err := d.mergeRead(next, 20); err != nil {
		t.Fatal(err)
	}
	if d.invalidateAssignment(old) {
		t.Fatal("迟到错误不能失效新归属")
	}
	if err := d.ReplaceSnapshot([]SlotAssignment{old}, 15); err != nil {
		t.Fatal(err)
	}
	got, err := d.resolve("slot")
	if err != nil || got != next {
		t.Fatalf("旧快照覆盖查询: %+v %v", got, err)
	}
	d.Invalidate()
	if err := d.mergeRead(next, 21); !errors.Is(err, ErrSlotDirectoryUnavailable) {
		t.Fatalf("查询不能重开失效目录: %v", err)
	}
}

func TestSlotCacheRejectedReadCannotOverwriteNewWatch(t *testing.T) {
	d := NewSlotDirectory()
	old := SlotAssignment{"slot", "old", 1}
	next := SlotAssignment{"slot", "new", 2}
	_ = d.ReplaceSnapshot([]SlotAssignment{next}, 30)
	_ = d.mergeRead(old, 20)
	got, err := d.resolve("slot")
	if err != nil || got != next {
		t.Fatalf("旧查询覆盖新 Watch: %+v %v", got, err)
	}
	d.invalidateAssignment(next)
	_ = d.ReplaceSnapshot([]SlotAssignment{next}, 31)
	if _, err := d.resolve("slot"); !errors.Is(err, ErrSlotRouteStale) {
		t.Fatalf("相同归属的 Watch 不能清除拒绝: %v", err)
	}
	_ = d.mergeRead(next, 32)
	if got, err := d.resolve("slot"); err != nil || got != next {
		t.Fatalf("新权威读未解除失效: %+v %v", got, err)
	}
}

type refreshStore struct {
	SlotStore

	reads atomic.Int32

	started chan struct{}
	release chan struct{}

	value SlotAssignment
}

type unavailableSlots struct {
	SlotStore

	reads atomic.Int32
}

func (s *unavailableSlots) ResolveSlotAtRevision(_ context.Context, slot string) (SlotAssignment, int64, error) {
	s.reads.Add(1)
	return SlotAssignment{slot, "old", 1}, 20, nil
}

func TestSlotRefreshDoesNotPollOnOtherQueriesPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := NewSlotDirectory()
		_ = d.ReplaceSnapshot([]SlotAssignment{{"a", "old", 1}, {"b", "old", 1}}, 10)
		s := &unavailableSlots{}
		cached, _ := NewCachedSlotStore(s, d)
		members := NewServiceDirectory()
		_ = members.Replace([]Instance{{ID: "old", Services: []string{"game"}, Endpoint: "old:1", Status: StatusDraining}})
		r, _ := NewSlotResolver(cached, members)
		tasks := lifecycle.NewWaitGroupContext(t.Context())
		defer func() { tasks.Cancel(); tasks.Wait() }()
		r.BindRefresh(lifecycle.TaskView(tasks))
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
		defer cancel()
		done := make(chan struct{}, 2)
		for _, slot := range []string{"a", "b"} {
			go func() { _, _ = r.ResolveServiceSlot(ctx, "game", slot); done <- struct{}{} }()
		}
		<-done
		<-done
		if got := s.reads.Load(); got != 2 {
			t.Fatalf("两个槽位相互唤醒造成重复权威查询: %d", got)
		}
	})
}

type slowRefreshStore struct{ *refreshStore }

func (s slowRefreshStore) ResolveSlotAtRevision(context.Context, string) (SlotAssignment, int64, error) {
	if s.reads.Add(1) == 1 {
		close(s.started)
	}
	<-s.release
	return s.value, 20, nil
}

func TestSlotRefreshRetainsActualWorkAfterCancellation(t *testing.T) {
	d := NewSlotDirectory()
	old := SlotAssignment{"slot", "old", 1}
	_ = d.ReplaceSnapshot([]SlotAssignment{old}, 10)
	d.invalidateAssignment(old)
	s := &refreshStore{started: make(chan struct{}), release: make(chan struct{}), value: SlotAssignment{"slot", "new", 2}}
	store, _ := NewCachedSlotStore(slowRefreshStore{s}, d)
	members := NewServiceDirectory()
	_ = members.Replace([]Instance{{ID: "new", Services: []string{"game"}, Endpoint: "new:1", Status: StatusRunning}})
	r, _ := NewSlotResolver(store, members)
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	r.BindRefresh(lifecycle.TaskView(tasks))
	done := make(chan error, 1)
	go func() { _, err := r.ResolveServiceSlot(t.Context(), "game", "slot"); done <- err }()
	<-s.started
	tasks.Cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r.refresh.mu.Lock()
	pending := len(r.refresh.pending)
	r.refresh.mu.Unlock()
	if pending != 1 {
		t.Fatalf("实际查询退出前登记被清除: %d", pending)
	}
	close(s.release)
	tasks.Wait()
	r.refresh.mu.Lock()
	pending = len(r.refresh.pending)
	r.refresh.mu.Unlock()
	if pending != 0 {
		t.Fatalf("查询退出后登记未清除: %d", pending)
	}
	if _, err := d.resolve("slot"); !errors.Is(err, ErrSlotRouteStale) {
		t.Fatalf("关闭后迟到查询不能发布结果: %v", err)
	}
}

func (s *refreshStore) ResolveSlotAtRevision(ctx context.Context, _ string) (SlotAssignment, int64, error) {
	if s.reads.Add(1) == 1 {
		close(s.started)
	}
	select {
	case <-s.release:
		return s.value, 20, nil
	case <-ctx.Done():
		return SlotAssignment{}, 0, ctx.Err()
	}
}

func TestSlotRefreshSharesQueryAndIsolatesWaiterCancellation(t *testing.T) {
	cache := NewSlotDirectory()
	old := SlotAssignment{"slot", "old", 1}
	_ = cache.ReplaceSnapshot([]SlotAssignment{old}, 10)
	cache.invalidateAssignment(old)
	store := &refreshStore{started: make(chan struct{}), release: make(chan struct{}), value: SlotAssignment{"slot", "new", 2}}
	cached, _ := NewCachedSlotStore(store, cache)
	members := NewServiceDirectory()
	_ = members.Replace([]Instance{{ID: "new", Services: []string{"game"}, Endpoint: "new:1", Status: StatusRunning}})
	r, _ := NewSlotResolver(cached, members)
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	t.Cleanup(func() { tasks.Cancel(); tasks.Wait() })
	r.BindRefresh(lifecycle.TaskView(tasks))
	first, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := r.ResolveServiceSlot(first, "game", "slot"); done <- err }()
	<-store.started
	second := make(chan error, 1)
	go func() {
		target, err := r.ResolveServiceSlot(t.Context(), "game", "slot")
		if err == nil && target.Assignment != store.value {
			err = errors.New("错误归属")
		}
		second <- err
	}()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(store.release)
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("共享查询未完成")
	}
	if store.reads.Load() != 1 {
		t.Fatalf("查询次数 %d", store.reads.Load())
	}
}
