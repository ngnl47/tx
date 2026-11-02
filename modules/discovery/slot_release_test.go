package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestReleasedSlotCanBeReassignedWithoutReusingGeneration(t *testing.T) {
	registry, _, _ := newIdentityIntegrationRegistry(t, "slot-release", time.Second)
	putReleaseMember(t, registry, "a", StatusRunning)
	putReleaseMember(t, registry, "b", StatusRunning)
	store := registry.Slots()
	ctx := t.Context()
	old := SlotAssignment{"slot", "a", 1}
	if err := store.CreateSlot(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseSlot(ctx, old); err != nil {
		t.Fatal(err)
	}
	free, err := store.ResolveSlot(ctx, old.SlotID)
	if err != nil || !free.Released() || free.Generation != 2 {
		t.Fatalf("释放记录: %+v %v", free, err)
	}
	if err := store.ReleaseSlot(ctx, old); err != nil {
		t.Fatalf("响应未知后重试: %v", err)
	}
	directory := NewServiceDirectory()
	_ = directory.Replace([]Instance{{ID: "b", Endpoint: "b:1", Services: []string{"game"}, Status: StatusRunning}})
	ownership, _ := NewSlotOwnership(store, directory)
	next, created, err := ownership.Ensure(ctx, "game", "slot", func(context.Context, string) (bool, error) { return true, nil })
	if err != nil || !created || next.Generation != 3 || next.RuntimeInstanceID != "b" {
		t.Fatalf("重新分配: %+v %v %v", next, created, err)
	}
	if err := store.ReleaseSlot(ctx, old); !errors.Is(err, ErrSlotConflict) {
		t.Fatalf("旧代次释放: %v", err)
	}
	got, _ := store.ResolveSlot(ctx, "slot")
	if got != next {
		t.Fatal(got)
	}
}

func putReleaseMember(t *testing.T, r *EtcdRegistry, id string, status Status) {
	t.Helper()
	value, err := json.Marshal(Instance{ID: id, Endpoint: id + ":1", Services: []string{"game"}, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.client.Put(t.Context(), r.instanceKey(id), value, 0); err != nil {
		t.Fatal(err)
	}
}

func TestSlotAssignmentRejectsDrainedCandidateAfterPreflight(t *testing.T) {
	r, _, _ := newIdentityIntegrationRegistry(t, "release-preflight", time.Second)
	putReleaseMember(t, r, "a", StatusRunning)
	directory := NewServiceDirectory()
	_ = directory.Replace([]Instance{{ID: "a", Endpoint: "a:1", Services: []string{"game"}, Status: StatusRunning}})
	owner, _ := NewSlotOwnership(r.Slots(), directory)
	_, _, err := owner.Ensure(t.Context(), "game", "slot", func(context.Context, string) (bool, error) {
		putReleaseMember(t, r, "a", StatusDraining)
		return true, nil
	})
	if !errors.Is(err, ErrSlotRuntimeUnavailable) {
		t.Fatalf("已排水候选仍被分配: %v", err)
	}
	if _, err = r.Slots().ResolveSlot(t.Context(), "slot"); !errors.Is(err, ErrSlotNotFound) {
		t.Fatal(err)
	}
}

func TestReleasedSlotConcurrentEnsureHasOneWinner(t *testing.T) {
	r, _, _ := newIdentityIntegrationRegistry(t, "release-race", time.Second)
	for _, id := range []string{"a", "b", "c"} {
		putReleaseMember(t, r, id, StatusRunning)
	}
	store := r.Slots()
	old := SlotAssignment{"slot", "a", 1}
	if err := store.CreateSlot(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseSlot(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered, proceed := make(chan struct{}, 2), make(chan struct{})
	type result struct {
		value   SlotAssignment
		created bool
		err     error
	}
	results := make(chan result, 2)
	for _, id := range []string{"b", "c"} {
		directory := NewServiceDirectory()
		_ = directory.Replace([]Instance{{ID: "b", Endpoint: "b:1", Services: []string{"game"}, Status: StatusRunning}, {ID: "c", Endpoint: "c:1", Services: []string{"game"}, Status: StatusRunning}})
		owner, _ := NewSlotOwnership(store, directory)
		go func() {
			v, created, err := owner.Ensure(ctx, "game", "slot", func(c context.Context, candidate string) (bool, error) {
				if candidate != id {
					return false, nil
				}
				entered <- struct{}{}
				select {
				case <-proceed:
					return true, nil
				case <-c.Done():
					return false, c.Err()
				}
			})
			results <- result{v, created, err}
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(proceed)
	one, two := <-results, <-results
	if one.err != nil || two.err != nil || one.created == two.created || one.value != two.value || one.value.Generation != 3 {
		t.Fatalf("重新分配必须有一个赢家且权威回读一致: %+v %+v", one, two)
	}
}

func TestReleaseSlotsResumesPartialCompletionAndFreesCapacity(t *testing.T) {
	r, _, _ := newIdentityIntegrationRegistry(t, "release-partial", time.Second)
	putReleaseMember(t, r, "a", StatusRunning)
	putReleaseMember(t, r, "b", StatusRunning)
	store := r.Slots()
	ctx := t.Context()
	first, second, other := SlotAssignment{"one", "a", 1}, SlotAssignment{"two", "a", 7}, SlotAssignment{"other", "b", 1}
	for _, v := range []SlotAssignment{first, second, other} {
		if err := store.CreateSlot(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟上一次仅释放首槽成功、随后连接中断；重试不能再次推进首槽版本。
	if err := store.ReleaseSlot(ctx, first); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.ReleaseSlots(ctx, "a"); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range []SlotAssignment{first, second} {
		got, err := store.ResolveSlot(ctx, v.SlotID)
		if err != nil || !got.Released() || got.Generation != v.Generation+1 {
			t.Fatalf("部分释放续跑: %+v %v", got, err)
		}
	}
	if got, err := store.ResolveSlot(ctx, other.SlotID); err != nil || got != other {
		t.Fatalf("误释放其他实例: %+v %v", got, err)
	}
	member, _ := json.Marshal(Instance{ID: "a", Endpoint: "a:1", Services: []string{"game"}, Status: StatusRunning, MaxSlotAssignments: 1})
	if err := r.client.Put(ctx, r.instanceKey("a"), member, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateSlot(ctx, SlotAssignment{"new", "a", 1}); err != nil {
		t.Fatalf("释放记录仍占用容量: %v", err)
	}
	if err := store.CreateSlot(ctx, SlotAssignment{"overflow", "a", 1}); !errors.Is(err, ErrSlotCapacityUnavailable) {
		t.Fatalf("新归属未计入容量: %v", err)
	}
}
