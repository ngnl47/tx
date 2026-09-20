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
		RuntimeInstanceID: "pod-uid-a",
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
		RuntimeInstanceID: "pod-uid-a",
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

func TestCachedSlotStoreResolveDoesNotReadBackingStore(t *testing.T) {
	assignment := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	backing := &slotStoreFake{assignments: map[string]discovery.SlotAssignment{assignment.SlotID: assignment}}
	directory := discovery.NewSlotDirectory()
	if err := directory.Replace([]discovery.SlotAssignment{assignment}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	store, err := discovery.NewCachedSlotStore(backing, directory)
	if err != nil {
		t.Fatalf("NewCachedSlotStore() error = %v", err)
	}

	for range 100 {
		resolved, resolveErr := store.ResolveSlot(context.Background(), assignment.SlotID)
		if resolveErr != nil {
			t.Fatalf("ResolveSlot() error = %v", resolveErr)
		}
		if resolved != assignment {
			t.Fatalf("ResolveSlot() = %#v, want %#v", resolved, assignment)
		}
	}
	if backing.resolutions != 0 {
		t.Fatalf("backing ResolveSlot calls = %d, want 0", backing.resolutions)
	}
	next := assignment
	next.RuntimeInstanceID = "pod-uid-b"
	next.Generation++
	if err := store.PromoteSlot(context.Background(), assignment, next); err != nil {
		t.Fatal(err)
	}
	// 持久写入成功不代表本地 Watch 已发布新快照，也不能触发热路径回源。
	if got, err := store.ResolveSlot(context.Background(), assignment.SlotID); err != nil || got != assignment {
		t.Fatalf("Watch 更新前 = (%+v, %v), want 旧归属", got, err)
	}
	if err := directory.Replace([]discovery.SlotAssignment{next}); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ResolveSlot(context.Background(), assignment.SlotID); err != nil || got != next || backing.resolutions != 0 {
		t.Fatalf("Watch 更新后 = (%+v, %v), backing reads=%d", got, err, backing.resolutions)
	}
}

func TestSlotDirectoryReadyClosesAfterInitialSnapshot(t *testing.T) {
	directory := discovery.NewSlotDirectory()
	select {
	case <-directory.Ready():
		t.Fatal("Ready() closed before initial snapshot")
	default:
	}
	if err := directory.Replace(nil); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	select {
	case <-directory.Ready():
	default:
		t.Fatal("Ready() must close after initial snapshot")
	}
}

func TestCachedSlotStoreFailsClosedAfterDirectoryInvalidation(t *testing.T) {
	assignment := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	backing := &slotStoreFake{assignments: map[string]discovery.SlotAssignment{assignment.SlotID: assignment}}
	directory := discovery.NewSlotDirectory()
	if err := directory.Replace([]discovery.SlotAssignment{assignment}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	store, err := discovery.NewCachedSlotStore(backing, directory)
	if err != nil {
		t.Fatalf("NewCachedSlotStore() error = %v", err)
	}
	directory.Invalidate()

	if _, err := store.ResolveSlot(context.Background(), assignment.SlotID); !errors.Is(err, discovery.ErrSlotDirectoryUnavailable) {
		t.Fatalf("ResolveSlot() error = %v, want ErrSlotDirectoryUnavailable", err)
	}
	if backing.resolutions != 0 {
		t.Fatalf("backing ResolveSlot calls = %d, want 0", backing.resolutions)
	}
}

func TestSlotRouterPromotesOnlyToReadyRuntime(t *testing.T) {
	current := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	next := discovery.SlotAssignment{
		SlotID:            current.SlotID,
		RuntimeInstanceID: "pod-uid-b",
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
	resolutions int
	promotions  int
}

func (s *slotStoreFake) ResolveSlot(_ context.Context, slotID string) (discovery.SlotAssignment, error) {
	s.resolutions++
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
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	next := discovery.SlotAssignment{
		SlotID:            current.SlotID,
		RuntimeInstanceID: "pod-uid-b",
		Generation:        43,
	}
	if err := discovery.ValidateSlotPromotion(current, next); err != nil {
		t.Fatalf("ValidateSlotPromotion() error = %v", err)
	}

	for _, candidate := range []discovery.SlotAssignment{
		{SlotID: current.SlotID, RuntimeInstanceID: "pod-uid-b", Generation: 42},
		{SlotID: current.SlotID, RuntimeInstanceID: "pod-uid-a", Generation: 43},
		{SlotID: "game-slot-2", RuntimeInstanceID: "pod-uid-b", Generation: 43},
	} {
		if !errors.Is(discovery.ValidateSlotPromotion(current, candidate), discovery.ErrInvalidSlotPromotion) {
			t.Fatalf("ValidateSlotPromotion(%#v, %#v) must return ErrInvalidSlotPromotion", current, candidate)
		}
	}
}

func BenchmarkSlotRouterCachedResolve(b *testing.B) {
	router, backing, slotID := newBenchmarkSlotRouter(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := router.ResolveSlot(context.Background(), slotID); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if backing.resolutions != 0 {
		b.Fatalf("backing ResolveSlot calls = %d, want 0", backing.resolutions)
	}
}

func BenchmarkSlotRouterCachedResolveParallel(b *testing.B) {
	router, backing, slotID := newBenchmarkSlotRouter(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := router.ResolveSlot(context.Background(), slotID); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	if backing.resolutions != 0 {
		b.Fatalf("backing ResolveSlot calls = %d, want 0", backing.resolutions)
	}
}

func newBenchmarkSlotRouter(b *testing.B) (*discovery.SlotRouter, *slotStoreFake, string) {
	b.Helper()
	assignment := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	backing := &slotStoreFake{assignments: map[string]discovery.SlotAssignment{assignment.SlotID: assignment}}
	slots := discovery.NewSlotDirectory()
	if err := slots.Replace([]discovery.SlotAssignment{assignment}); err != nil {
		b.Fatalf("Replace slots: %v", err)
	}
	store, err := discovery.NewCachedSlotStore(backing, slots)
	if err != nil {
		b.Fatalf("NewCachedSlotStore: %v", err)
	}
	instances := discovery.NewServiceDirectory()
	if err := instances.Replace([]discovery.Instance{{
		ID:       assignment.RuntimeInstanceID,
		Services: []string{"game"},
		Endpoint: "127.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		b.Fatalf("Replace instances: %v", err)
	}
	router, err := discovery.NewSlotRouter(store, instances)
	if err != nil {
		b.Fatalf("NewSlotRouter: %v", err)
	}
	return router, backing, assignment.SlotID
}

func TestSlotRouterCachedResolveDoesNotAllocate(t *testing.T) {
	assignment := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	backing := &slotStoreFake{assignments: map[string]discovery.SlotAssignment{assignment.SlotID: assignment}}
	slots := discovery.NewSlotDirectory()
	if err := slots.Replace([]discovery.SlotAssignment{assignment}); err != nil {
		t.Fatalf("Replace slots: %v", err)
	}
	store, err := discovery.NewCachedSlotStore(backing, slots)
	if err != nil {
		t.Fatalf("NewCachedSlotStore: %v", err)
	}
	instances := discovery.NewServiceDirectory()
	if err := instances.Replace([]discovery.Instance{{
		ID:       assignment.RuntimeInstanceID,
		Services: []string{"game"},
		Endpoint: "127.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("Replace instances: %v", err)
	}
	router, err := discovery.NewSlotRouter(store, instances)
	if err != nil {
		t.Fatalf("NewSlotRouter: %v", err)
	}

	allocations := testing.AllocsPerRun(1000, func() {
		if _, resolveErr := router.ResolveSlot(context.Background(), assignment.SlotID); resolveErr != nil {
			t.Fatal(resolveErr)
		}
	})
	if allocations != 0 {
		t.Fatalf("ResolveSlot allocations = %.0f, want 0", allocations)
	}
}
