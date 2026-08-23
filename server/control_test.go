package server

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestControlServerDrainsThroughCoordinator(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() { root.Cancel(); root.WaitDoneAndWaitGroupWait() }()
	host := &blockingDrainHost{started: make(chan struct{}), release: make(chan struct{})}
	coordinator := newDrainCoordinator(host)
	server, err := startControlServer(root, "127.0.0.1:0", controlServerOptions{Lifecycle: coordinator})
	if err != nil {
		t.Fatalf("StartControlServer returned error: %v", err)
	}
	connection, err := grpc.NewClient(server.endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer connection.Close()
	client := controlpb.NewLifecycleServiceClient(connection)
	if response, err := client.Drain(context.Background(), &controlpb.Empty{}); err != nil || response.GetState() != controlpb.LifecycleState_LIFECYCLE_STATE_DRAINING {
		t.Fatalf("Drain = %#v, %v", response, err)
	}
	<-host.started
	close(host.release)
	if response, err := client.WaitDrained(context.Background(), &controlpb.Empty{}); err != nil || response.GetState() != controlpb.LifecycleState_LIFECYCLE_STATE_DRAINED {
		t.Fatalf("WaitDrained = %#v, %v", response, err)
	}
}

func TestControlServerPromotesLogicalSlot(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() { root.Cancel(); root.WaitDoneAndWaitGroupWait() }()
	current := discovery.SlotAssignment{SlotID: "game-slot-1", RuntimeInstanceID: "game-1@v42", Generation: 42}
	store := &controlSlotStore{current: current}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "game-1@v43",
		Endpoint: "127.0.0.1:9000",
		Services: []string{"game"},
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	promoter, err := newSlotPromoter(store, directory)
	if err != nil {
		t.Fatalf("NewSlotPromoter returned error: %v", err)
	}
	coordinator := newDrainCoordinator(&blockingDrainHost{})
	server, err := startControlServer(root, "127.0.0.1:0", controlServerOptions{Lifecycle: coordinator, Slots: promoter})
	if err != nil {
		t.Fatalf("StartControlServer returned error: %v", err)
	}
	connection, err := grpc.NewClient(server.endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer connection.Close()
	response, err := controlpb.NewSlotServiceClient(connection).PromoteSlot(context.Background(), &controlpb.PromoteSlotRequest{
		Current: &controlpb.SlotAssignment{
			SlotId:            current.SlotID,
			RuntimeInstanceId: current.RuntimeInstanceID,
			Generation:        current.Generation,
		},
		NextRuntimeInstanceId: "game-1@v43",
	})
	if err != nil {
		t.Fatalf("PromoteSlot returned error: %v", err)
	}
	assignment := response.GetAssignment()
	if assignment.GetGeneration() != 43 || assignment.GetRuntimeInstanceId() != "game-1@v43" {
		t.Fatalf("assignment = %#v", assignment)
	}
	if store.current.Generation != 43 {
		t.Fatalf("stored assignment = %#v", store.current)
	}
}
