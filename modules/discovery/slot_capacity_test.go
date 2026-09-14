package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 并发不同世界竞争同一实例时，归属写入本身必须守住名额；晋升也不能绕过。
func TestEtcdSlotCapacityIsAtomicAndFollowsOwnership(t *testing.T) {
	registry, _, _ := newIdentityIntegrationRegistry(t, "slot-capacity", time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, id := range []string{"a", "b", "c"} {
		value, _ := json.Marshal(Instance{ID: id, Endpoint: id + ":1", Services: []string{"world"}, Status: StatusRunning, MaxSlotAssignments: 1})
		if err := registry.client.Put(ctx, registry.instanceKey(id), value, 0); err != nil {
			t.Fatal(err)
		}
	}
	store := registry.Slots()
	var admitted atomic.Int32
	winners := make(chan SlotAssignment, 16)
	var group sync.WaitGroup
	for i := range 16 {
		group.Go(func() {
			assignment := SlotAssignment{SlotID: fmt.Sprintf("world-%d", i), RuntimeInstanceID: "a", Generation: 1}
			err := store.CreateSlot(ctx, assignment)
			if err == nil {
				admitted.Add(1)
				winners <- assignment
			} else if !errors.Is(err, ErrSlotCapacityUnavailable) {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("超出归属配额: %d", admitted.Load())
	}
	first := <-winners
	second := SlotAssignment{SlotID: "season-world", RuntimeInstanceID: "b", Generation: 1}
	if err := store.CreateSlot(ctx, second); err != nil {
		t.Fatal(err)
	}
	next := first
	next.RuntimeInstanceID = "b"
	next.Generation++
	if err := store.PromoteSlot(ctx, first, next); !errors.Is(err, ErrSlotCapacityUnavailable) {
		t.Fatalf("晋升超过配额: %v", err)
	}
	directory := NewServiceDirectory()
	if err := directory.Replace([]Instance{{ID: "a", Endpoint: "a:1", Services: []string{"world"}, Status: StatusRunning}, {ID: "b", Endpoint: "b:1", Services: []string{"world"}, Status: StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	ownership, err := NewSlotOwnership(store, directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ownership.Handoff(ctx, "world", first, "b", func(context.Context, string) (bool, error) { return true, nil }, func(context.Context, string) error { t.Error("满载目标不应触发源排水"); return nil }); !errors.Is(err, ErrSlotCapacityUnavailable) {
		t.Fatalf("交接预检: %v", err)
	}
	next.RuntimeInstanceID = "c"
	if err := store.PromoteSlot(ctx, first, next); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateSlot(ctx, SlotAssignment{SlotID: "new-world", RuntimeInstanceID: "a", Generation: 1}); err != nil {
		t.Fatalf("交接后原实例名额未释放: %v", err)
	}
	if got, err := store.ResolveSlot(ctx, first.SlotID); err != nil || got != next {
		t.Fatalf("归属未更新: %+v %v", got, err)
	}
}
