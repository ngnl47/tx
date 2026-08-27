package control

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStartServerExposesLifecycleController(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	}()
	controller := &testLifecycleController{state: StateRunning}

	server, err := StartServer(root, "127.0.0.1:0", ServerOptions{Lifecycle: controller})
	if err != nil {
		t.Fatalf("StartServer returned error: %v", err)
	}
	defer server.Stop()

	connection, err := grpc.NewClient(server.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer connection.Close()

	response, err := controlpb.NewLifecycleServiceClient(connection).Drain(context.Background(), &controlpb.Empty{})
	if err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if response.GetState() != controlpb.LifecycleState_LIFECYCLE_STATE_DRAINING {
		t.Fatalf("Drain state = %v", response.GetState())
	}
}

func TestStartServerRequiresLifecycleController(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	}()

	_, err := StartServer(root, "127.0.0.1:0", ServerOptions{})
	if !errors.Is(err, ErrControllerNil) {
		t.Fatalf("StartServer error = %v, want ErrControllerNil", err)
	}
}

type testLifecycleController struct {
	state State
}

func (controller *testLifecycleController) BeginDrain(context.Context) error {
	controller.state = StateDraining
	return nil
}

func (controller *testLifecycleController) WaitDrained(context.Context) error {
	controller.state = StateDrained
	return nil
}

func (controller *testLifecycleController) Status() Status {
	return Status{State: controller.state}
}
