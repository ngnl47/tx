package discovery

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestSlotOwnershipKeepsAssignmentAndChecksAuthority(t *testing.T) {
	store := newRuntimeSlotStoreFake()
	directory := NewServiceDirectory()
	if err := directory.Replace([]Instance{
		{ID: "a", Endpoint: "a:1", Services: []string{"world"}, Status: StatusRunning},
		{ID: "b", Endpoint: "b:1", Services: []string{"world"}, Status: StatusRunning},
	}); err != nil {
		t.Fatal(err)
	}
	owner, err := NewSlotOwnership(store, directory)
	if err != nil {
		t.Fatal(err)
	}
	probes := 0
	current, created, err := owner.Ensure(t.Context(), "world", "world-51", func(context.Context, string) (bool, error) { probes++; return probes == 2, nil })
	if err != nil || !created || probes != 2 {
		t.Fatalf("first: %v %v probes=%d err=%v", current, created, probes, err)
	}
	got, created, err := owner.Ensure(t.Context(), "world", current.SlotID, func(context.Context, string) (bool, error) {
		t.Error("已有归属不应重新选址")
		return false, nil
	})
	if err != nil || created || got != current {
		t.Fatalf("existing: %v %v %v", got, created, err)
	}
	if err := CheckSlotOwner(t.Context(), store, current.SlotID, current.RuntimeInstanceID); err != nil {
		t.Fatal(err)
	}
	if err := CheckSlotOwner(t.Context(), store, current.SlotID, "wrong"); !errors.Is(err, ErrSlotNotOwner) {
		t.Fatalf("authority: %v", err)
	}
	if _, _, err := owner.Ensure(t.Context(), "game", current.SlotID, nil); !errors.Is(err, ErrSlotRuntimeUnavailable) {
		t.Fatalf("wrong service: %v", err)
	}
}

func TestSlotOwnershipDoesNotFailOverUnknownCreation(t *testing.T) {
	for _, outcome := range []error{context.DeadlineExceeded, ErrSlotConflict} {
		t.Run(outcome.Error(), func(t *testing.T) {
			directory := NewServiceDirectory()
			if err := directory.Replace([]Instance{{ID: "a", Endpoint: "a:1", Services: []string{"world"}, Status: StatusRunning}, {ID: "b", Endpoint: "b:1", Services: []string{"world"}, Status: StatusRunning}}); err != nil {
				t.Fatal(err)
			}
			winner := SlotAssignment{SlotID: "world-51", RuntimeInstanceID: "b", Generation: 1}
			store := &ownershipStore{runtimeSlotStoreFake: newRuntimeSlotStoreFake()}
			writes, probes := 0, 0
			store.create = func(SlotAssignment) error { writes++; store.assignments[winner.SlotID] = winner; return outcome }
			owner, err := NewSlotOwnership(store, directory)
			if err != nil {
				t.Fatal(err)
			}
			got, created, err := owner.Ensure(t.Context(), "world", winner.SlotID, func(context.Context, string) (bool, error) { probes++; return true, nil })
			if writes != 1 || probes != 1 || created {
				t.Fatalf("writes=%d probes=%d created=%v", writes, probes, created)
			}
			if errors.Is(outcome, ErrSlotConflict) {
				if err != nil || got != winner {
					t.Fatalf("CAS winner: %v %v", got, err)
				}
			} else if !errors.Is(err, outcome) {
				t.Fatalf("unknown: %v", err)
			}
		})
	}
}

func TestSlotOwnershipHandoffRequiresPreparation(t *testing.T) {
	store := &ownershipStore{runtimeSlotStoreFake: newRuntimeSlotStoreFake()}
	directory := NewServiceDirectory()
	if err := directory.Replace([]Instance{{ID: "b", Endpoint: "b:1", Services: []string{"world"}, Status: StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	owner, err := NewSlotOwnership(store, directory)
	if err != nil {
		t.Fatal(err)
	}
	current := SlotAssignment{SlotID: "world-51", RuntimeInstanceID: "a", Generation: 1}
	store.assignments[current.SlotID] = current
	var events []string
	check := func(context.Context, string) (bool, error) { events = append(events, "capacity"); return true, nil }
	prepare := func(context.Context, string) error {
		events = append(events, "prepare")
		return context.DeadlineExceeded
	}
	store.promote = func(old, next SlotAssignment) error {
		events = append(events, "promote")
		store.assignments[next.SlotID] = next
		return nil
	}
	if _, err := owner.Handoff(t.Context(), "world", current, "b", check, prepare); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if store.assignments[current.SlotID] != current || !reflect.DeepEqual(events, []string{"capacity", "prepare"}) {
		t.Fatalf("failed prepare: %v %v", events, store.assignments)
	}
	events = nil
	next, err := owner.Handoff(t.Context(), "world", current, "b", check, func(_ context.Context, instance string) error {
		if instance != "a" {
			t.Errorf("source=%s", instance)
		}
		events = append(events, "prepare")
		return nil
	})
	if err != nil || next.Generation != 2 || next.RuntimeInstanceID != "b" || !reflect.DeepEqual(events, []string{"capacity", "prepare", "promote"}) {
		t.Fatalf("handoff: %v %v %v", next, events, err)
	}
	events = nil
	if _, err := owner.Handoff(t.Context(), "world", current, "b", check, prepare); !errors.Is(err, ErrSlotConflict) || len(events) != 0 {
		t.Fatalf("stale handoff: %v %v", err, events)
	}
}

type ownershipStore struct {
	*runtimeSlotStoreFake
	create  func(SlotAssignment) error
	promote func(SlotAssignment, SlotAssignment) error
}

func (s *ownershipStore) CreateSlot(_ context.Context, a SlotAssignment) error { return s.create(a) }
func (s *ownershipStore) PromoteSlot(_ context.Context, a, b SlotAssignment) error {
	return s.promote(a, b)
}
