package discovery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 正常 Watch 更新不应把一条变化再次展开为全目录快照。
func TestSlotWatchDoesNotRepublishFullSnapshot(t *testing.T) {
	registry, _, _ := newIdentityIntegrationRegistry(t, "slot-update", time.Second)
	putReleaseMember(t, registry, "a", StatusRunning)
	putReleaseMember(t, registry, "b", StatusRunning)
	store := registry.Slots()
	old := SlotAssignment{"slot", "a", 1}
	if err := store.CreateSlot(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	var snapshots atomic.Int32
	watcher := &watchSlots{runtimeSlotStoreFake: newRuntimeSlotStoreFake(), watch: func(ctx context.Context, d *SlotDirectory) error {
		publish := d.publish
		d.publish = func(values []SlotAssignment, revision int64) error {
			snapshots.Add(1)
			return publish(values, revision)
		}
		return store.Watch(ctx, d)
	}}
	services := &watchRegistry{watch: func(ctx context.Context, d *ServiceDirectory) error {
		if err := d.Replace(nil); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	r, err := newRuntime(services, newRuntimeRegistrationFake(), watcher)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- r.watchOnce(ctx) }()
	t.Cleanup(func() { cancel(); <-done; _ = r.Close(context.Background()) })
	if err := r.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	next := SlotAssignment{"slot", "b", 2}
	if err := store.PromoteSlot(ctx, old, next); err != nil {
		t.Fatal(err)
	}
	for {
		got, err := r.Slots().ResolveSlot(ctx, old.SlotID)
		if err == nil && got == next {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if got := snapshots.Load(); got != 1 {
		t.Fatalf("一条变更重新发布全量快照，快照次数=%d，期望仅初始化时1次", got)
	}
	if _, err := r.Slots().ResolveSlot(ctx, "missing"); !errors.Is(err, ErrSlotNotFound) {
		t.Fatal(err)
	}
}

func TestSlotChangesPreserveNewReadsAndUnchangedSlots(t *testing.T) {
	d := NewSlotDirectory()
	old, other := SlotAssignment{"slot", "a", 1}, SlotAssignment{"other", "b", 1}
	next := SlotAssignment{"slot", "c", 2}
	if err := d.ReplaceSnapshot([]SlotAssignment{old, other}, 10); err != nil {
		t.Fatal(err)
	}
	if err := d.mergeRead(next, 30); err != nil {
		t.Fatal(err)
	}
	// 落后的 Watch 删除不能撤销主动查询已经确认的新归属。
	added := SlotAssignment{"added", "b", 1}
	if err := d.applyChanges([]slotChange{{assignment: old, revision: 20, deleted: true}, {assignment: added, revision: 20}}, 20); err != nil {
		t.Fatal(err)
	}
	assertSlotValue(t, d, next)
	assertSlotValue(t, d, other)
	assertSlotValue(t, d, added)
	d.invalidateAssignment(next)
	if err := d.applyChanges([]slotChange{{assignment: next, revision: 30}}, 30); err != nil {
		t.Fatal(err)
	}
	if _, err := d.resolve(next.SlotID); !errors.Is(err, ErrSlotRouteStale) {
		t.Fatalf("相同归属的事件不能解除准入拒绝: %v", err)
	}
	free := SlotAssignment{SlotID: old.SlotID, Generation: 3}
	if err := d.applyChanges([]slotChange{{assignment: free, revision: 31}}, 31); err != nil {
		t.Fatal(err)
	}
	assertSlotValue(t, d, free)
	if err := d.applyChanges([]slotChange{{assignment: free, revision: 32, deleted: true}}, 32); err != nil {
		t.Fatal(err)
	}
	_ = d.mergeRead(next, 31)
	_ = d.applyChanges([]slotChange{{assignment: next, revision: 31}}, 31)
	_ = d.ReplaceSnapshot([]SlotAssignment{next, other}, 31)
	if _, err := d.resolve(old.SlotID); !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("删除后旧查询/事件/快照不能恢复归属: %v", err)
	}
	assertSlotValue(t, d, other)
}

func TestSlotChangesCannotOpenUnavailableDirectory(t *testing.T) {
	d := NewSlotDirectory()
	value := SlotAssignment{"slot", "a", 1}
	changes := []slotChange{{assignment: value, revision: 11}}
	if err := d.applyChanges(changes, 10); !errors.Is(err, ErrSlotDirectoryUnavailable) {
		t.Fatalf("首次快照前不得发布增量: %v", err)
	}
	if err := d.ReplaceSnapshot([]SlotAssignment{value}, 10); err != nil {
		t.Fatal(err)
	}
	d.Invalidate()
	if err := d.applyChanges(changes, 11); !errors.Is(err, ErrSlotDirectoryUnavailable) {
		t.Fatalf("断流后不得由增量恢复可用性: %v", err)
	}
	if _, err := d.resolve(value.SlotID); !errors.Is(err, ErrSlotDirectoryUnavailable) {
		t.Fatal(err)
	}
	if err := d.ReplaceSnapshot([]SlotAssignment{value}, 12); err != nil {
		t.Fatal(err)
	}
	assertSlotValue(t, d, value)
}

func TestSlotChangeBatchPreservesInvalidatedRead(t *testing.T) {
	d := NewSlotDirectory()
	old := SlotAssignment{"slot", "a", 1}
	middle := SlotAssignment{"slot", "b", 2}
	current := SlotAssignment{"slot", "c", 3}
	if err := d.ReplaceSnapshot([]SlotAssignment{old}, 10); err != nil {
		t.Fatal(err)
	}
	if err := d.mergeRead(current, 30); err != nil {
		t.Fatal(err)
	}
	d.invalidateAssignment(current)
	// Watch 追赶期间，同一批可能包含较早归属和主动读已看到的最终归属。
	if err := d.applyChanges([]slotChange{{assignment: middle, revision: 20}, {assignment: current, revision: 30}}, 30); err != nil {
		t.Fatal(err)
	}
	got, err := d.resolve(current.SlotID)
	if got != current || !errors.Is(err, ErrSlotRouteStale) {
		t.Fatalf("较早事件清除了最终归属的拒绝标记: got=%+v err=%v", got, err)
	}
}

func assertSlotValue(t *testing.T, d *SlotDirectory, want SlotAssignment) {
	t.Helper()
	got, err := d.resolve(want.SlotID)
	if err != nil || got != want {
		t.Fatalf("归属: got=%+v err=%v want=%+v", got, err, want)
	}
}
