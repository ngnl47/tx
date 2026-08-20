package tests

import (
	"context"
	"net"
	"testing"

	"github.com/ngnl47/tx/modules/control"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLifecycleControlExposesDrainAndWait(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen control server: %v", err)
	}
	server := grpc.NewServer()
	controller := &controlControllerFake{state: control.StateRunning}
	if err := control.RegisterLifecycleService(server, controller); err != nil {
		t.Fatalf("register control service: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer connection.Close()
	client := controlpb.NewLifecycleServiceClient(connection)

	before, err := client.Status(context.Background(), &controlpb.Empty{})
	if err != nil || before.GetState() != controlpb.LifecycleState_LIFECYCLE_STATE_RUNNING {
		t.Fatalf("initial Status = %#v, %v", before, err)
	}
	draining, err := client.Drain(context.Background(), &controlpb.Empty{})
	if err != nil || draining.GetState() != controlpb.LifecycleState_LIFECYCLE_STATE_DRAINING {
		t.Fatalf("Drain = %#v, %v", draining, err)
	}
	drained, err := client.WaitDrained(context.Background(), &controlpb.Empty{})
	if err != nil || drained.GetState() != controlpb.LifecycleState_LIFECYCLE_STATE_DRAINED {
		t.Fatalf("WaitDrained = %#v, %v", drained, err)
	}
}

type controlControllerFake struct {
	state control.State
}

func (fake *controlControllerFake) BeginDrain(context.Context) error {
	fake.state = control.StateDraining
	return nil
}

func (fake *controlControllerFake) WaitDrained(context.Context) error {
	fake.state = control.StateDrained
	return nil
}

func (fake *controlControllerFake) Status() control.Status {
	return control.Status{State: fake.state}
}
