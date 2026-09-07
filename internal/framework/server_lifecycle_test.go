package framework

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestFatalDiscoveryLossDrainsAndStopsServer(t *testing.T) {
	service := newBlockingServiceRecorder()
	server := newStartedServerForFatalTest(t, service)
	server.reportFatal(errors.New("workSlot ownership lost"))
	waitClosed(t, server.stopDone)
	if !service.drained || !service.stopped {
		t.Fatalf("lifecycle = %#v", service)
	}
	if server.stopErr == nil || !strings.Contains(server.stopErr.Error(), "workSlot ownership lost") {
		t.Fatalf("stop error = %v", server.stopErr)
	}
}

func TestStopWaitsForConcurrentStartAndClosesStartedResources(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	stopped := make(chan struct{})
	mod := &blockingStartCMod{
		startEntered: startEntered,
		releaseStart: releaseStart,
		stopped:      stopped,
	}
	app, err := New(
		Config{AppID: "concurrent-start-stop"},
		WithCMod(mod),
		WithService(testService{spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}}}),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	startResult := make(chan error, 1)
	go func() {
		startResult <- app.Start(context.Background(), RunOptions{Services: "game"})
	}()
	<-startEntered
	if runtime := app.Context(); runtime != nil {
		t.Fatalf("Context during Start = %#v, want nil", runtime)
	}

	stopResult := make(chan error, 1)
	go func() { stopResult <- app.Stop(context.Background()) }()
	select {
	case err := <-stopResult:
		t.Fatalf("Stop returned before Start completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseStart)
	if err := <-startResult; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := <-stopResult; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("CMod was not stopped")
	}
	if runtime := app.Context(); runtime != nil {
		t.Fatalf("Context after Stop = %#v, want nil", runtime)
	}
}

func TestContextStaysUnavailableAfterStartFailure(t *testing.T) {
	wantErr := errors.New("start failed")
	mod := &blockingStartCMod{startErr: wantErr}
	app, err := New(
		Config{AppID: "failed-context"},
		WithCMod(mod),
		WithService(testService{spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}}}),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	err = app.Start(context.Background(), RunOptions{Services: "game"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start error = %v, want %v", err, wantErr)
	}
	if runtime := app.Context(); runtime != nil {
		t.Fatalf("Context after failed Start = %#v, want nil", runtime)
	}
}

func TestStopFinishesServiceTasksBeforeCModDrain(t *testing.T) {
	taskStarted := make(chan struct{})
	taskStopped := make(chan struct{})
	mod := &serviceTaskDrainCMod{taskStopped: taskStopped}
	service := testService{
		spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}},
		start: func(runtime Context) error {
			return runtime.Tasks().Go(func(ctx context.Context) {
				close(taskStarted)
				<-ctx.Done()
				close(taskStopped)
			})
		},
	}
	app, err := New(
		Config{AppID: "service-task-drain"},
		WithCMod(mod),
		WithService(service),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := app.Start(context.Background(), RunOptions{Services: "game"}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	select {
	case <-taskStarted:
	case <-time.After(time.Second):
		t.Fatal("service task was not started")
	}

	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if !mod.drainObservedTaskExit {
		t.Fatal("CMod Drain ran before the service task exited")
	}
}

type blockingStartCMod struct {
	startEntered chan struct{}
	releaseStart chan struct{}
	stopped      chan struct{}
	startErr     error
}

type blockingServiceRecorder struct {
	drained bool
	stopped bool
}

func newBlockingServiceRecorder() *blockingServiceRecorder {
	return &blockingServiceRecorder{}
}

func (*blockingServiceRecorder) Spec() Spec {
	return Spec{Name: "fatal-discovery-loss"}
}

func (*blockingServiceRecorder) Start(Context) error { return nil }

func (service *blockingServiceRecorder) Drain(context.Context) error {
	service.drained = true
	return nil
}

func (service *blockingServiceRecorder) Stop(context.Context) error {
	service.stopped = true
	return nil
}

func newStartedServerForFatalTest(t *testing.T, service *blockingServiceRecorder) *Server {
	t.Helper()
	server, err := New(
		Config{AppID: "fatal-discovery-loss"},
		WithService(service),
	)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if err := server.Start(context.Background(), RunOptions{Services: "fatal-discovery-loss"}); err != nil {
		t.Fatalf("Start = %v", err)
	}
	return server
}

func waitClosed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

type serviceTaskDrainCMod struct {
	taskStopped           <-chan struct{}
	drainObservedTaskExit bool
}

func (*serviceTaskDrainCMod) Spec() CModSpec { return CModSpec{ID: "service-task-drain"} }

func (*serviceTaskDrainCMod) Start(CModContext) error { return nil }

func (mod *serviceTaskDrainCMod) Drain(context.Context) error {
	select {
	case <-mod.taskStopped:
		mod.drainObservedTaskExit = true
		return nil
	default:
		return errors.New("service task is still running")
	}
}

func (*blockingStartCMod) Spec() CModSpec { return CModSpec{ID: "blocking-start"} }

func (mod *blockingStartCMod) Start(CModContext) error {
	if mod.startEntered != nil {
		close(mod.startEntered)
	}
	if mod.releaseStart != nil {
		<-mod.releaseStart
	}
	return mod.startErr
}

func (mod *blockingStartCMod) Stop(context.Context) error {
	if mod.stopped != nil {
		close(mod.stopped)
	}
	return nil
}
