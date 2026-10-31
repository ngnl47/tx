package actorrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/modules/discovery"
)

type explicitSlots struct {
	discovery.SlotStore
	assignment *discovery.SlotAssignment
}

func (s *explicitSlots) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	if s.assignment == nil {
		return discovery.SlotAssignment{}, discovery.ErrSlotNotFound
	}
	return *s.assignment, nil
}

// 不提供 CreateSlot；只读策略误入首次分配即失败。
func TestExplicitAssignmentNeverAllocates(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "world-host")
	store := &explicitSlots{}
	resolver, err := discovery.NewSlotResolver(store, directory)
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := discovery.NewSlotOwnership(store, directory)
	if err != nil {
		t.Fatal(err)
	}
	router := &EntityRouter{Resolver: resolver, Ownership: ownership,
		Service:            func(string) (string, error) { return "world", nil },
		ExplicitAssignment: func(string) bool { return true },
		SlotID:             actorhost.SlotMapping(worldSlot).Resolver()}
	id := actorhost.Identity{Type: "world", ID: "1"}
	if _, err := router.Resolve(t.Context(), id); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatal(err)
	}
	if _, _, err := router.Ensure(t.Context(), id); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatal(err)
	}
	if _, _, err := router.Retry(t.Context(), id); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatal(err)
	}
	store.assignment = &discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: "world-host", Generation: 1}
	target, err := router.Resolve(t.Context(), id)
	if err != nil || target.Instance != "world-host" {
		t.Fatalf("登记后路由: %+v %v", target, err)
	}
	store.assignment.RuntimeInstanceID = "offline"
	if _, err := router.Resolve(t.Context(), id); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatal(err)
	}
}
