package control

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestSlotPromoterRequiresRunningDestination(t *testing.T) {
	current := discovery.SlotAssignment{SlotID: "game-slot-1", RuntimeInstanceID: "pod-a", Generation: 3}
	store := &testSlotStore{current: current}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "pod-b",
		Endpoint: "127.0.0.1:9000",
		Services: []string{"game"},
		Status:   discovery.StatusStarting,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}

	promoter, err := NewSlotPromoter(store, directory)
	if err != nil {
		t.Fatalf("NewSlotPromoter returned error: %v", err)
	}
	promotion := SlotPromotion{Current: current, NextRuntimeInstanceID: "pod-b"}
	if err := promoter.PromoteSlot(context.Background(), promotion); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("PromoteSlot before ready error = %v", err)
	}

	if err := directory.Replace([]discovery.Instance{{
		ID:       "pod-b",
		Endpoint: "127.0.0.1:9000",
		Services: []string{"game"},
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace running directory: %v", err)
	}
	if err := promoter.PromoteSlot(context.Background(), promotion); err != nil {
		t.Fatalf("PromoteSlot returned error: %v", err)
	}
	want := discovery.SlotAssignment{SlotID: current.SlotID, RuntimeInstanceID: "pod-b", Generation: 4}
	if store.current != want {
		t.Fatalf("stored assignment = %#v, want %#v", store.current, want)
	}
}

type testSlotStore struct {
	current discovery.SlotAssignment
}

func (store *testSlotStore) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	return store.current, nil
}

func (store *testSlotStore) CreateSlot(context.Context, discovery.SlotAssignment) error {
	return errors.New("unexpected CreateSlot")
}

func (store *testSlotStore) PromoteSlot(_ context.Context, current, next discovery.SlotAssignment) error {
	if store.current != current {
		return discovery.ErrSlotConflict
	}
	store.current = next
	return nil
}
