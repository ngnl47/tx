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
			RuntimeInstanceId: "game-1@v42",
			Generation:        42,
		},
		NextRuntimeInstanceId: "game-1@v43",
	})
	if err != nil {
		t.Fatalf("PromoteSlot returned error: %v", err)
	}
	if got, want := controller.promotion.Current.RuntimeInstanceID, "game-1@v42"; got != want {
		t.Fatalf("current runtime = %q, want %q", got, want)
	}
	if got, want := controller.promotion.NextRuntimeInstanceID, "game-1@v43"; got != want {
		t.Fatalf("next runtime = %q, want %q", got, want)
	}
	assignment := response.GetAssignment()
	if assignment.GetSlotId() != "game-slot-1" || assignment.GetRuntimeInstanceId() != "game-1@v43" || assignment.GetGeneration() != 43 {
		t.Fatalf("assignment = %#v", assignment)
	}
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
				RuntimeInstanceId: "game-1@v42",
				Generation:        42,
			},
			NextRuntimeInstanceId: "game-1@v43",
		})
		if status.Code(err) != codes.Aborted {
			t.Fatalf("PromoteSlot error = %v, want Aborted", err)
		}
	})

}

type slotControllerFake struct {
	promotion control.SlotPromotion
	err       error
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
