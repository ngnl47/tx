package discovery

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
)

func TestRuntimeStatusAndCloseOwnership(t *testing.T) {
	registry := &runtimeRegistryFake{}
	registration := &runtimeRegistrationFake{}
	slots := newRuntimeSlotStoreFake()
	runtime, err := newRuntime(registry, registration, slots)
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	if runtime.Directory() == nil || runtime.Slots() == nil {
		t.Fatal("Runtime did not expose cached directories")
	}
	if err := runtime.MarkRunning(context.Background()); err != nil {
		t.Fatalf("MarkRunning: %v", err)
	}
	if err := runtime.MarkDraining(context.Background()); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	if err := runtime.MarkStopping(context.Background()); err != nil {
		t.Fatalf("MarkStopping: %v", err)
	}
	want := []Status{StatusRunning, StatusDraining, StatusStopping}
	if !slices.Equal(registration.statuses, want) {
		t.Fatalf("statuses = %v, want %v", registration.statuses, want)
	}

	const callers = 4
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			if err := runtime.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	group.Wait()
	if registry.closeCalls != 1 || registration.closeCalls != 1 {
		t.Fatalf("close calls = registry:%d registration:%d, want 1 each", registry.closeCalls, registration.closeCalls)
	}
}

func TestRuntimeWaitReadyRequiresBothSnapshots(t *testing.T) {
	runtime, err := newRuntime(&runtimeRegistryFake{}, &runtimeRegistrationFake{}, newRuntimeSlotStoreFake())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.WaitReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady error = %v, want context canceled", err)
	}
}

type runtimeRegistryFake struct {
	closeCalls int
}

func (*runtimeRegistryFake) Watch(ctx context.Context, _ *ServiceDirectory) error {
	<-ctx.Done()
	return ctx.Err()
}

func (registry *runtimeRegistryFake) Close() error {
	registry.closeCalls++
	return nil
}

type runtimeRegistrationFake struct {
	statuses   []Status
	closeCalls int
}

func (registration *runtimeRegistrationFake) SetStatus(_ context.Context, status Status) error {
	registration.statuses = append(registration.statuses, status)
	return nil
}

func (registration *runtimeRegistrationFake) Close(context.Context) error {
	registration.closeCalls++
	return nil
}

type runtimeSlotStoreFake struct {
	assignments map[string]SlotAssignment
}

func newRuntimeSlotStoreFake() *runtimeSlotStoreFake {
	return &runtimeSlotStoreFake{assignments: make(map[string]SlotAssignment)}
}

func (store *runtimeSlotStoreFake) ResolveSlot(_ context.Context, slotID string) (SlotAssignment, error) {
	assignment, ok := store.assignments[slotID]
	if !ok {
		return SlotAssignment{}, ErrSlotNotFound
	}
	return assignment, nil
}

func (store *runtimeSlotStoreFake) CreateSlot(_ context.Context, assignment SlotAssignment) error {
	store.assignments[assignment.SlotID] = assignment
	return nil
}

func (store *runtimeSlotStoreFake) PromoteSlot(_ context.Context, _, next SlotAssignment) error {
	store.assignments[next.SlotID] = next
	return nil
}

func (*runtimeSlotStoreFake) Watch(ctx context.Context, _ *SlotDirectory) error {
	<-ctx.Done()
	return ctx.Err()
}
