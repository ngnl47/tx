package discovery

import (
	"context"
	"errors"
	"testing"
)

func TestSlotRouterCreateSlotRequiresRunningDestination(t *testing.T) {
	store := &routerSlotStore{}
	directory := NewServiceDirectory()
	if err := directory.Replace([]Instance{{
		ID:       "pod-a",
		Endpoint: "127.0.0.1:9000",
		Services: []string{"game"},
		Status:   StatusStarting,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	router, err := NewSlotRouter(store, directory)
	if err != nil {
		t.Fatalf("NewSlotRouter returned error: %v", err)
	}

	assignment := SlotAssignment{SlotID: "game-slot-1", RuntimeInstanceID: "pod-a", Generation: 1}
	if err := router.CreateSlot(context.Background(), assignment); !errors.Is(err, ErrSlotRuntimeUnavailable) {
		t.Fatalf("CreateSlot before ready error = %v, want ErrSlotRuntimeUnavailable", err)
	}
	if store.created != (SlotAssignment{}) {
		t.Fatalf("stored assignment = %#v before destination is running", store.created)
	}
}

func TestSlotRouterCreateSlotStoresInitialAssignment(t *testing.T) {
	store := &routerSlotStore{}
	directory := NewServiceDirectory()
	if err := directory.Replace([]Instance{{
		ID:       "pod-a",
		Endpoint: "127.0.0.1:9000",
		Services: []string{"game"},
		Status:   StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	router, err := NewSlotRouter(store, directory)
	if err != nil {
		t.Fatalf("NewSlotRouter returned error: %v", err)
	}

	want := SlotAssignment{SlotID: "game-slot-1", RuntimeInstanceID: "pod-a", Generation: 1}
	if err := router.CreateSlot(context.Background(), want); err != nil {
		t.Fatalf("CreateSlot returned error: %v", err)
	}
	if store.created != want {
		t.Fatalf("stored assignment = %#v, want %#v", store.created, want)
	}
}

type routerSlotStore struct {
	created SlotAssignment
}

func (store *routerSlotStore) ResolveSlot(context.Context, string) (SlotAssignment, error) {
	return SlotAssignment{}, ErrSlotNotFound
}

func (store *routerSlotStore) CreateSlot(_ context.Context, assignment SlotAssignment) error {
	store.created = assignment
	return nil
}

func (store *routerSlotStore) PromoteSlot(context.Context, SlotAssignment, SlotAssignment) error {
	return errors.New("unexpected PromoteSlot")
}

type readOnlySlot struct{ assignment SlotAssignment }

func (reader readOnlySlot) ResolveSlot(context.Context, string) (SlotAssignment, error) {
	return reader.assignment, nil
}

func TestSlotResolverAcceptsReadOnlyReaderAndRejectsUnavailableTarget(t *testing.T) {
	assignment := SlotAssignment{SlotID: "world-1", RuntimeInstanceID: "world-a", Generation: 2}
	directory := NewServiceDirectory()
	resolver, err := NewSlotResolver(readOnlySlot{assignment}, directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []Status{StatusStarting, StatusRunning, StatusDraining, StatusStopping} {
		if err := directory.Replace([]Instance{{ID: "world-a", Endpoint: "127.0.0.1:9000", Services: []string{"world"}, Status: status}}); err != nil {
			t.Fatal(err)
		}
		target, err := resolver.ResolveSlot(context.Background(), assignment.SlotID)
		if status == StatusRunning {
			if err != nil || target.Assignment != assignment || target.Instance.ID != "world-a" {
				t.Fatalf("解析结果 = %+v, %v", target, err)
			}
		} else if !errors.Is(err, ErrSlotRuntimeUnavailable) {
			t.Fatalf("状态 %v 的解析错误 = %v", status, err)
		}
	}
}
