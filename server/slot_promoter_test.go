package server

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/control"
	"github.com/ngnl47/tx/modules/discovery"
)

func TestSlotPromoterPublishesOnlyRunningNextRuntime(t *testing.T) {
	current := discovery.SlotAssignment{SlotID: "game-slot-1", RuntimeInstanceID: "pod-uid-a", Generation: 42}
	store := &controlSlotStore{current: current}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "pod-uid-b",
		Endpoint: "127.0.0.1:9000",
		Services: []string{"game"},
		Status:   discovery.StatusStarting,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	promoter, err := newSlotPromoter(store, directory)
	if err != nil {
		t.Fatalf("NewSlotPromoter returned error: %v", err)
	}
	promotion := control.SlotPromotion{Current: current, NextRuntimeInstanceID: "pod-uid-b"}
	if err := promoter.PromoteSlot(context.Background(), promotion); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("PromoteSlot before ready error = %v, want ErrSlotRuntimeUnavailable", err)
	}

	if err := directory.Replace([]discovery.Instance{{
		ID:       "pod-uid-b",
		Endpoint: "127.0.0.1:9000",
		Services: []string{"game"},
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace running directory: %v", err)
	}
	if err := promoter.PromoteSlot(context.Background(), promotion); err != nil {
		t.Fatalf("PromoteSlot returned error: %v", err)
	}
	assignment := discovery.SlotAssignment{SlotID: current.SlotID, RuntimeInstanceID: "pod-uid-b", Generation: 43}
	if assignment.Generation != 43 || assignment.RuntimeInstanceID != "pod-uid-b" {
		t.Fatalf("assignment = %#v", assignment)
	}
	if store.current != assignment {
		t.Fatalf("stored assignment = %#v, want %#v", store.current, assignment)
	}
}

type controlSlotStore struct {
	current discovery.SlotAssignment
}

func (store *controlSlotStore) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	return store.current, nil
}

func (store *controlSlotStore) CreateSlot(context.Context, discovery.SlotAssignment) error {
	return errors.New("unexpected CreateSlot")
}

func (store *controlSlotStore) PromoteSlot(_ context.Context, current, next discovery.SlotAssignment) error {
	if store.current != current {
		return discovery.ErrSlotConflict
	}
	store.current = next
	return nil
}
