package tests

import (
	"context"
	"net"
	"testing"

	"github.com/ngnl47/tx/modules/control"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestSlotControlPromotesExpectedAssignment(t *testing.T) {
	controller := &slotControllerFake{}
	client, cleanup := newSlotControlClient(t, controller)
	defer cleanup()

	response, err := client.PromoteSlot(context.Background(), &controlpb.PromoteSlotRequest{
		Current: &controlpb.SlotAssignment{
			SlotId:            "game-slot-1",
			RuntimeInstanceId: "pod-uid-a",
			Generation:        42,
		},
		NextRuntimeInstanceId: "pod-uid-b",
	})
	if err != nil {
		t.Fatalf("PromoteSlot returned error: %v", err)
	}
	if got, want := controller.promotion.Current.RuntimeInstanceID, "pod-uid-a"; got != want {
		t.Fatalf("current runtime = %q, want %q", got, want)
	}
	if got, want := controller.promotion.NextRuntimeInstanceID, "pod-uid-b"; got != want {
		t.Fatalf("next runtime = %q, want %q", got, want)
	}
	assignment := response.GetAssignment()
	if assignment.GetSlotId() != "game-slot-1" || assignment.GetRuntimeInstanceId() != "pod-uid-b" || assignment.GetGeneration() != 43 {
		t.Fatalf("assignment = %#v", assignment)
	}
}

func TestSlotControlCreatesInitialAssignment(t *testing.T) {
	controller := &slotControllerFake{}
	client, cleanup := newSlotControlClient(t, controller)
	defer cleanup()

	response, err := client.CreateSlot(context.Background(), &controlpb.CreateSlotRequest{
		SlotId:            "game-slot-1",
		RuntimeInstanceId: "pod-uid-a",
	})
	if err != nil {
		t.Fatalf("CreateSlot returned error: %v", err)
	}
	if got, want := controller.creation, (control.SlotCreation{SlotID: "game-slot-1", RuntimeInstanceID: "pod-uid-a"}); got != want {
		t.Fatalf("creation = %#v, want %#v", got, want)
	}
	assignment := response.GetAssignment()
	if assignment.GetSlotId() != "game-slot-1" || assignment.GetRuntimeInstanceId() != "pod-uid-a" || assignment.GetGeneration() != 1 {
		t.Fatalf("assignment = %#v", assignment)
	}
}

func TestSlotControlRejectsInvalidAndConflictingCreation(t *testing.T) {
	t.Run("invalid request", func(t *testing.T) {
		client, cleanup := newSlotControlClient(t, &slotControllerFake{})
		defer cleanup()

		_, err := client.CreateSlot(context.Background(), &controlpb.CreateSlotRequest{RuntimeInstanceId: "pod-uid-a"})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CreateSlot error = %v, want InvalidArgument", err)
		}
	})

	t.Run("assignment already exists", func(t *testing.T) {
		client, cleanup := newSlotControlClient(t, &slotControllerFake{err: discovery.ErrSlotConflict})
		defer cleanup()

		_, err := client.CreateSlot(context.Background(), &controlpb.CreateSlotRequest{
			SlotId:            "game-slot-1",
			RuntimeInstanceId: "pod-uid-a",
		})
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("CreateSlot error = %v, want AlreadyExists", err)
		}
	})
}

func TestSlotControlRejectsInvalidAndConflictingPromotion(t *testing.T) {
	t.Run("invalid request", func(t *testing.T) {
		client, cleanup := newSlotControlClient(t, &slotControllerFake{})
		defer cleanup()

		_, err := client.PromoteSlot(context.Background(), &controlpb.PromoteSlotRequest{})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("PromoteSlot error = %v, want InvalidArgument", err)
		}
	})

	t.Run("compare and swap conflict", func(t *testing.T) {
		client, cleanup := newSlotControlClient(t, &slotControllerFake{err: discovery.ErrSlotConflict})
		defer cleanup()

		_, err := client.PromoteSlot(context.Background(), &controlpb.PromoteSlotRequest{
			Current: &controlpb.SlotAssignment{
				SlotId:            "game-slot-1",
				RuntimeInstanceId: "pod-uid-a",
				Generation:        42,
			},
			NextRuntimeInstanceId: "pod-uid-b",
		})
		if status.Code(err) != codes.Aborted {
			t.Fatalf("PromoteSlot error = %v, want Aborted", err)
		}
	})

}

type slotControllerFake struct {
	creation  control.SlotCreation
	promotion control.SlotPromotion
	err       error
}

func (fake *slotControllerFake) CreateSlot(_ context.Context, creation control.SlotCreation) error {
	fake.creation = creation
	return fake.err
}

func (fake *slotControllerFake) PromoteSlot(_ context.Context, promotion control.SlotPromotion) error {
	fake.promotion = promotion
	return fake.err
}

func newSlotControlClient(t *testing.T, controller control.SlotController) (controlpb.SlotServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	if err := control.RegisterSlotService(server, controller); err != nil {
		t.Fatalf("RegisterSlotService returned error: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient(
		"passthrough:///control",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		listener.Close()
		t.Fatalf("create control client: %v", err)
	}
	return controlpb.NewSlotServiceClient(connection), func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
	}
}
