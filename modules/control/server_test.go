package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/storage"
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

	server, err := StartServer(root, "127.0.0.1:0", ServerOptions{Lifecycle: controller, StorageIO: testIOProvider{}})
	if err != nil {
		t.Fatalf("StartServer returned error: %v", err)
	}
	defer server.Stop()

	connection, err := grpc.NewClient(server.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer connection.Close()
	for range 2 {
		snapshot, err := controlpb.NewStorageIOServiceClient(connection).Snapshot(context.Background(), &controlpb.Empty{})
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.GetInstanceId() != "instance-1" || len(snapshot.GetStats()) != 1 {
			t.Fatalf("snapshot = %+v", snapshot)
		}
		stat := snapshot.GetStats()[0]
		if stat.GetCalls() != 3 || stat.GetCommands() != 4 || stat.GetErrors() != 1 || stat.GetTotalDurationNs() != 5000000 || stat.GetMaxDurationNs() != 3000000 {
			t.Fatalf("stat = %+v", stat)
		}
	}

	response, err := controlpb.NewLifecycleServiceClient(connection).Drain(context.Background(), &controlpb.Empty{})
	if err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if response.GetState() != controlpb.LifecycleState_LIFECYCLE_STATE_DRAINING {
		t.Fatalf("Drain state = %v", response.GetState())
	}
}

type testIOProvider struct{}

func (testIOProvider) StorageIOStats() StorageIOSnapshot {
	return StorageIOSnapshot{InstanceID: "instance-1", Stats: []storage.IOStat{{Backend: "redis", Operation: "pipeline", Calls: 3, Commands: 4, Errors: 1, TotalDuration: 5 * time.Millisecond, MaxDuration: 3 * time.Millisecond}}}
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

func TestControlReportsUnexpectedServeFailure(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { root.Cancel(); root.Wait() }()
	failures := make(chan error, 1)
	server, err := StartServer(root, "127.0.0.1:0", ServerOptions{Lifecycle: &testLifecycleController{}, OnFailure: func(err error) { failures <- err }})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	if err := server.listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("未传递错误")
		}
	case <-time.After(time.Second):
		t.Fatal("未通知宿主")
	}
}

func TestControlNormalStopDoesNotReportFailure(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(t.Context())
	failures := make(chan error, 1)
	server, err := StartServer(root, "127.0.0.1:0", ServerOptions{Lifecycle: &testLifecycleController{}, OnFailure: func(err error) { failures <- err }})
	if err != nil {
		t.Fatal(err)
	}
	server.Stop()
	root.Cancel()
	root.Wait()
	select {
	case err := <-failures:
		t.Fatalf("正常关闭误报: %v", err)
	default:
	}
}
