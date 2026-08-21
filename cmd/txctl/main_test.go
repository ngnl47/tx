package main

import (
	"bytes"
	"context"
	"net"
	"testing"

	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestRunCallsDrainAndPrintsLifecycleState(t *testing.T) {
	listener, server := newControlTestServer(t, controlpb.LifecycleState_LIFECYCLE_STATE_DRAINING, "")
	defer server.Stop()

	var output bytes.Buffer
	err := run(context.Background(), []string{"--endpoint", "control", "drain"}, &output, bufconnDialer(listener))
	if err != nil {
		t.Fatalf("run drain returned error: %v", err)
	}
	if got, want := output.String(), "state=draining\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunReturnsErrorForFailedLifecycleState(t *testing.T) {
	listener, server := newControlTestServer(t, controlpb.LifecycleState_LIFECYCLE_STATE_FAILED, "drain failed")
	defer server.Stop()

	var output bytes.Buffer
	err := run(context.Background(), []string{"--endpoint", "control", "wait-drained"}, &output, bufconnDialer(listener))
	if err == nil {
		t.Fatal("run wait-drained error = nil, want failed lifecycle error")
	}
	if got, want := output.String(), "state=failed\nerror=drain failed\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunPromotesLogicalSlotAndPrintsNewAssignment(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	recorder := &slotControlTestServer{}
	controlpb.RegisterSlotServiceServer(server, recorder)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	var output bytes.Buffer
	err := run(context.Background(), []string{
		"--endpoint", "control",
		"promote-slot",
		"--slot", "game-slot-1",
		"--current-instance", "game-1@v42",
		"--current-generation", "42",
		"--next-instance", "game-1@v43",
	}, &output, bufconnDialer(listener))
	if err != nil {
		t.Fatalf("run promote-slot returned error: %v", err)
	}
	if got, want := output.String(), "slot=game-slot-1 runtimeInstance=game-1@v43 generation=43\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
	request := recorder.request
	if request == nil || request.GetCurrent().GetGeneration() != 42 || request.GetNextRuntimeInstanceId() != "game-1@v43" {
		t.Fatalf("promotion request = %#v", request)
	}
}

func TestRunRejectsIncompleteSlotPromotion(t *testing.T) {
	var output bytes.Buffer
	err := run(context.Background(), []string{
		"--endpoint", "control",
		"promote-slot",
		"--slot", "game-slot-1",
	}, &output, func(context.Context, string) (net.Conn, error) {
		return nil, context.Canceled
	})
	if err == nil {
		t.Fatal("run promote-slot error = nil, want invalid arguments")
	}
}

func newControlTestServer(t *testing.T, state controlpb.LifecycleState, message string) (*bufconn.Listener, *grpc.Server) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	controlpb.RegisterLifecycleServiceServer(server, controlTestServer{state: state, message: message})
	go func() { _ = server.Serve(listener) }()
	return listener, server
}

func bufconnDialer(listener *bufconn.Listener) dialFunc {
	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

type controlTestServer struct {
	controlpb.UnimplementedLifecycleServiceServer
	state   controlpb.LifecycleState
	message string
}

type slotControlTestServer struct {
	controlpb.UnimplementedSlotServiceServer
	request *controlpb.PromoteSlotRequest
}

func (server *slotControlTestServer) PromoteSlot(_ context.Context, request *controlpb.PromoteSlotRequest) (*controlpb.PromoteSlotResponse, error) {
	server.request = request
	current := request.GetCurrent()
	return &controlpb.PromoteSlotResponse{Assignment: &controlpb.SlotAssignment{
		SlotId:            current.GetSlotId(),
		RuntimeInstanceId: request.GetNextRuntimeInstanceId(),
		Generation:        current.GetGeneration() + 1,
	}}, nil
}

func (server controlTestServer) Status(context.Context, *controlpb.Empty) (*controlpb.LifecycleStatus, error) {
	return server.status(), nil
}

func (server controlTestServer) Drain(context.Context, *controlpb.Empty) (*controlpb.LifecycleStatus, error) {
	return server.status(), nil
}

func (server controlTestServer) WaitDrained(context.Context, *controlpb.Empty) (*controlpb.LifecycleStatus, error) {
	return server.status(), nil
}

func (server controlTestServer) status() *controlpb.LifecycleStatus {
	return &controlpb.LifecycleStatus{State: server.state, Error: server.message}
}
