package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestSlotAssignmentValidatesStableSlotAndRuntimeGeneration(t *testing.T) {
	valid := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "game-1@v42@a",
		Generation:        42,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	for _, assignment := range []discovery.SlotAssignment{
		{RuntimeInstanceID: valid.RuntimeInstanceID, Generation: valid.Generation},
		{SlotID: valid.SlotID, Generation: valid.Generation},
		{SlotID: valid.SlotID, RuntimeInstanceID: valid.RuntimeInstanceID},
		{SlotID: "slot/invalid", RuntimeInstanceID: valid.RuntimeInstanceID, Generation: valid.Generation},
	} {
		if !errors.Is(assignment.Validate(), discovery.ErrInvalidSlotAssignment) {
			t.Fatalf("Validate(%#v) must return ErrInvalidSlotAssignment", assignment)
		}
	}
}

func TestSlotRouterOnlyResolvesRunningAssignedRuntime(t *testing.T) {
	assignment := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "game-1@v42@a",
		Generation:        42,
	}
	store := &slotStoreFake{assignments: map[string]discovery.SlotAssignment{assignment.SlotID: assignment}}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       assignment.RuntimeInstanceID,
		Services: []string{"game"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	router, err := discovery.NewSlotRouter(store, directory)
	if err != nil {
		t.Fatalf("NewSlotRouter() error = %v", err)
	}

	target, err := router.ResolveSlot(context.Background(), assignment.SlotID)
	if err != nil {
		t.Fatalf("ResolveSlot() error = %v", err)
	}
	if target.Assignment != assignment || target.Instance.Endpoint != "10.0.0.1:9000" {
		t.Fatalf("target = %#v", target)
	}

	if err := directory.Replace([]discovery.Instance{{
		ID:       assignment.RuntimeInstanceID,
		Services: []string{"game"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusDraining,
	}}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if _, err := router.ResolveSlot(context.Background(), assignment.SlotID); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("ResolveSlot() on draining runtime error = %v, want ErrSlotRuntimeUnavailable", err)
	}
}

func TestSlotRouterPromotesOnlyToReadyRuntime(t *testing.T) {
	current := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "game-1@v42@a",
		Generation:        42,
	}
	next := discovery.SlotAssignment{
		SlotID:            current.SlotID,
		RuntimeInstanceID: "game-1@v43@b",
		Generation:        43,
	}
	store := &slotStoreFake{assignments: map[string]discovery.SlotAssignment{current.SlotID: current}}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       next.RuntimeInstanceID,
		Services: []string{"game"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusStarting,
	}}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	router, err := discovery.NewSlotRouter(store, directory)
	if err != nil {
		t.Fatalf("NewSlotRouter() error = %v", err)
	}
	if err := router.PromoteSlot(context.Background(), current, next); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("PromoteSlot() before ready error = %v, want ErrSlotRuntimeUnavailable", err)
	}
	if store.promotions != 0 {
		t.Fatalf("promotions = %d, want no storage write before target is ready", store.promotions)
	}

	if err := directory.Replace([]discovery.Instance{{
		ID:       next.RuntimeInstanceID,
		Services: []string{"game"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if err := router.PromoteSlot(context.Background(), current, next); err != nil {
		t.Fatalf("PromoteSlot() error = %v", err)
	}
	if store.promotions != 1 || store.assignments[current.SlotID] != next {
		t.Fatalf("store after promotion = %#v, promotions = %d", store.assignments, store.promotions)
	}
}

type slotStoreFake struct {
	assignments map[string]discovery.SlotAssignment
	promotions  int
}

func (s *slotStoreFake) ResolveSlot(_ context.Context, slotID string) (discovery.SlotAssignment, error) {
	assignment, ok := s.assignments[slotID]
	if !ok {
		return discovery.SlotAssignment{}, discovery.ErrSlotNotFound
	}
	return assignment, nil
}

func (s *slotStoreFake) CreateSlot(_ context.Context, assignment discovery.SlotAssignment) error {
	if _, exists := s.assignments[assignment.SlotID]; exists {
		return discovery.ErrSlotConflict
	}
	s.assignments[assignment.SlotID] = assignment
	return nil
}

func (s *slotStoreFake) PromoteSlot(_ context.Context, current, next discovery.SlotAssignment) error {
	if s.assignments[current.SlotID] != current {
		return discovery.ErrSlotConflict
	}
	s.assignments[current.SlotID] = next
	s.promotions++
	return nil
}

func TestSlotPromotionRequiresNextGenerationAndDistinctRuntime(t *testing.T) {
	current := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "game-1@v42@a",
		Generation:        42,
	}
	next := discovery.SlotAssignment{
		SlotID:            current.SlotID,
		RuntimeInstanceID: "game-1@v43@b",
		Generation:        43,
	}
	if err := discovery.ValidateSlotPromotion(current, next); err != nil {
		t.Fatalf("ValidateSlotPromotion() error = %v", err)
	}

	for _, candidate := range []discovery.SlotAssignment{
		{SlotID: current.SlotID, RuntimeInstanceID: "game-1@v43@b", Generation: 42},
		{SlotID: current.SlotID, RuntimeInstanceID: "game-1@v42@a", Generation: 43},
		{SlotID: "game-slot-2", RuntimeInstanceID: "game-1@v43@b", Generation: 43},
	} {
		if !errors.Is(discovery.ValidateSlotPromotion(current, candidate), discovery.ErrInvalidSlotPromotion) {
			t.Fatalf("ValidateSlotPromotion(%#v, %#v) must return ErrInvalidSlotPromotion", current, candidate)
		}
	}
}
