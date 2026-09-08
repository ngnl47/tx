package main

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/ngnl47/tx/modules/control"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRunStorageIOPrintsLosslessSnapshot(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	if err := control.RegisterStorageIOService(server, cliIOProvider{}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	var output bytes.Buffer
	if err := run(context.Background(), []string{"--endpoint", "control", "storage-io"}, &output, bufconnDialer(listener)); err != nil {
		t.Fatal(err)
	}
	var snapshot controlpb.StorageIOSnapshot
	if err := protojson.Unmarshal(output.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.GetInstanceId() != "instance-1" || len(snapshot.GetStats()) != 1 || snapshot.GetStats()[0].GetCalls() != 9007199254740993 {
		t.Fatalf("snapshot = %+v", &snapshot)
	}
}

type cliIOProvider struct{}

func (cliIOProvider) StorageIOStats() control.StorageIOSnapshot {
	return control.StorageIOSnapshot{InstanceID: "instance-1", Stats: []storage.IOStat{{Backend: "redis", Operation: "get", Calls: 9007199254740993}}}
}

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
		"--current-instance", "pod-uid-a",
		"--current-generation", "42",
		"--next-instance", "pod-uid-b",
	}, &output, bufconnDialer(listener))
	if err != nil {
		t.Fatalf("run promote-slot returned error: %v", err)
	}
	if got, want := output.String(), "slot=game-slot-1 runtimeInstance=pod-uid-b generation=43\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
	request := recorder.request
	if request == nil || request.GetCurrent().GetGeneration() != 42 || request.GetNextRuntimeInstanceId() != "pod-uid-b" {
		t.Fatalf("promotion request = %#v", request)
	}
}

func TestRunCreatesLogicalSlotAndPrintsInitialAssignment(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	recorder := &slotControlTestServer{}
	controlpb.RegisterSlotServiceServer(server, recorder)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	var output bytes.Buffer
	err := run(context.Background(), []string{
		"--endpoint", "control",
		"create-slot",
		"--slot", "game-slot-1",
		"--instance", "pod-uid-a",
	}, &output, bufconnDialer(listener))
	if err != nil {
		t.Fatalf("run create-slot returned error: %v", err)
	}
	if got, want := output.String(), "slot=game-slot-1 runtimeInstance=pod-uid-a generation=1\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
	request := recorder.createRequest
	if request == nil || request.GetSlotId() != "game-slot-1" || request.GetRuntimeInstanceId() != "pod-uid-a" {
		t.Fatalf("create request = %#v", request)
	}
}

func TestRunRejectsIncompleteSlotCreation(t *testing.T) {
	var output bytes.Buffer
	err := run(context.Background(), []string{
		"--endpoint", "control",
		"create-slot",
		"--slot", "game-slot-1",
	}, &output, func(context.Context, string) (net.Conn, error) {
		return nil, context.Canceled
	})
	if err == nil {
		t.Fatal("run create-slot error = nil, want invalid arguments")
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
	createRequest *controlpb.CreateSlotRequest
	request       *controlpb.PromoteSlotRequest
}

func (server *slotControlTestServer) CreateSlot(_ context.Context, request *controlpb.CreateSlotRequest) (*controlpb.CreateSlotResponse, error) {
	server.createRequest = request
	return &controlpb.CreateSlotResponse{Assignment: &controlpb.SlotAssignment{
		SlotId:            request.GetSlotId(),
		RuntimeInstanceId: request.GetRuntimeInstanceId(),
		Generation:        1,
	}}, nil
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
