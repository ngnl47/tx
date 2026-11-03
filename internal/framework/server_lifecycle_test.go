package framework

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	txagent "github.com/ngnl47/tx/modules/agent"
	agentservice "github.com/ngnl47/tx/modules/agent/service"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
	"google.golang.org/grpc"
)

func TestFatalDiscoveryLossDrainsAndStopsServer(t *testing.T) {
	service := newBlockingServiceRecorder()
	server := newStartedServerForFatalTest(t, service)
	server.reportFatal(errors.New("discovery identity lost"))
	waitClosed(t, server.stopDone)
	if !service.drained || !service.stopped {
		t.Fatalf("lifecycle = %#v", service)
	}
	if server.stopErr == nil || !strings.Contains(server.stopErr.Error(), "discovery identity lost") {
		t.Fatalf("stop error = %v", server.stopErr)
	}
}

func TestServiceFatalReportDuringInitializationFailsStart(t *testing.T) {
	want := errors.New("agent listener failed")
	app, err := New(Config{AppID: "service-fatal-start"}, WithService(testService{
		spec: Spec{Name: "agent"},
		start: func(ctx Context) error {
			reporter, ok := ctx.(interface{ ReportFatal(error) })
			if !ok {
				t.Error("service context has no fatal error reporting capability")
				return nil
			}
			reporter.ReportFatal(want)
			return nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	if err := app.Start(context.Background(), RunOptions{Services: []string{"agent"}}); !errors.Is(err, want) {
		t.Fatalf("Start = %v, want listener failure", err)
	}
	if app.Context() != nil {
		t.Fatal("failed startup exposed a running context")
	}
}

func TestServiceFatalReportStopsRunningServer(t *testing.T) {
	want := errors.New("agent listener failed while running")
	var report func(error)
	stopped := false
	app, err := New(Config{AppID: "service-fatal-running"}, WithService(testService{
		spec:  Spec{Name: "agent"},
		start: func(ctx Context) error { report = ctx.ReportFatal; return nil },
		stop:  func(context.Context) error { stopped = true; return nil },
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background(), RunOptions{Services: []string{"agent"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	report(want)
	waitClosed(t, app.stopDone)
	if !stopped || !errors.Is(app.stopErr, want) {
		t.Fatalf("stopped = %v, Stop = %v", stopped, app.stopErr)
	}
}

func TestAgentStartupRollbackRetainsRouterUntilConnectionExits(t *testing.T) {
	for _, failurePhase := range []string{"init", "start"} {
		t.Run(failurePhase, func(t *testing.T) {
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			router := &rollbackAgentRouter{}
			var agent *agentservice.Service
			first := testService{
				spec: Spec{Name: "agent"},
				start: func(ctx Context) error {
					var err error
					agent, err = agentservice.New(agentservice.Options{
						TCPListen: "127.0.0.1:0", MaxFrameBytes: 1024, Sessions: router,
						Identity: txagent.IdentityResolverFunc(func(ctx context.Context, _ txagent.AuthRequest) (txagent.AuthResult, error) {
							close(entered)
							<-ctx.Done()
							close(canceled)
							<-release
							return txagent.AuthResult{SessionPrincipal: protocol.SessionPrincipal{}}, ctx.Err()
						}),
						Dispatcher: txagent.DispatchFunc(func(context.Context, txagent.DispatchRequest) error { return nil }),
					}, agentservice.Dependencies{Tasks: ctx.Tasks(), Admission: ctx.Admission(), ReportFatal: ctx.ReportFatal, RPC: rollbackAgentRPC{grpc.NewServer()}})
					if err != nil {
						return err
					}
					return agent.Init(ctx.Tasks())
				},
				drain: func(ctx context.Context) error { return agent.Drain(ctx) },
				stop:  func(ctx context.Context) error { return agent.Stop(ctx) },
			}
			want := errors.New("later service initialization failed")
			later := testService{spec: Spec{Name: "later"}, start: func(Context) error {
				connection, err := net.Dial("tcp", agent.TCPAddress())
				if err != nil {
					return err
				}
				t.Cleanup(func() { _ = connection.Close() })
				frame, err := txagent.EncodeFrame(txagent.Frame{Kind: txagent.FrameHandshake}, 1024)
				if err != nil {
					return err
				}
				packet := make([]byte, 4+len(frame))
				binary.BigEndian.PutUint32(packet[:4], uint32(len(frame)))
				copy(packet[4:], frame)
				if _, err := connection.Write(packet); err != nil {
					return err
				}
				select {
				case <-entered:
				case <-time.After(time.Second):
					return errors.New("agent did not enter identity callback")
				}
				return want
			}}
			var failing Service = later
			if failurePhase == "start" {
				failing = phasedService{testService: testService{spec: later.spec}, initHook: func(Context) error { return nil }, startHook: func(context.Context) error { return later.start(nil) }}
			}
			app, err := New(Config{AppID: "agent-startup-rollback", Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: 20 * time.Millisecond}}}, WithService(first, failing))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { unblock(); _ = app.Stop(context.Background()) })
			err = app.Start(context.Background(), RunOptions{Services: []string{"agent", "later"}})
			if !errors.Is(err, want) || !errors.Is(err, errStopDeferred) {
				t.Fatalf("Start = %v", err)
			}
			select {
			case <-canceled:
			default:
				t.Fatal("rollback did not cancel connection setup")
			}
			if router.closed.Load() || app.runtime == nil {
				t.Fatal("rollback released resources while connection callback was running")
			}
			if app.Context() != nil {
				t.Fatal("failed startup exposed a running context")
			}
			unblock()
			if err := app.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !router.closed.Load() {
				t.Fatal("Stop retry did not release router")
			}
		})
	}
}

type rollbackAgentRouter struct{ closed atomic.Bool }

func (*rollbackAgentRouter) Acquire(context.Context, txmsgq.AcquireSessionRequest) (txmsgq.SessionClient, error) {
	return nil, errors.New("unexpected acquire")
}
func (router *rollbackAgentRouter) Close() error { router.closed.Store(true); return nil }

type rollbackAgentRPC struct{ server *grpc.Server }

func (rpc rollbackAgentRPC) Register(register func(grpc.ServiceRegistrar) error) error {
	return register(rpc.server)
}
func (rollbackAgentRPC) Endpoint() string { return "127.0.0.1:0" }

func TestFatalDiscoveryLossDuringDeferredStopStillTerminates(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	app, err := New(Config{AppID: "fatal-during-stop"}, WithService(testService{
		spec: Spec{Name: "game"},
		drain: func(context.Context) error {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return errors.New("drain failed")
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- app.Stop(context.Background()) }()
	<-entered
	want := errors.New("discovery identity lost")
	app.reportFatal(want)
	close(release)
	<-stopped
	waitClosed(t, app.stopDone)
	if !errors.Is(app.stopErr, want) {
		t.Fatalf("Stop = %v", app.stopErr)
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
		startResult <- app.Start(context.Background(), RunOptions{Services: []string{"game"}})
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
	err = app.Start(context.Background(), RunOptions{Services: []string{"game"}})
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
	if err := app.Start(context.Background(), RunOptions{Services: []string{"game"}}); err != nil {
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

func TestStopRetainsResourcesUntilDrainAndPersistenceSucceed(t *testing.T) {
	for _, phase := range []string{"ingress", "persistence"} {
		t.Run(phase, func(t *testing.T) {
			failed := true
			stopped := false
			flushCalls := 0
			flushed := false
			want := errors.New("temporarily blocked")
			mod := &retryFlushCMod{flush: func() error {
				flushCalls++
				if failed && phase == "persistence" {
					return want
				}
				flushed = true
				return nil
			}}
			app, err := New(Config{AppID: "strict-stop"}, WithCMod(mod), WithService(testService{
				spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}},
				drain: func(context.Context) error {
					if failed && phase == "ingress" {
						return want
					}
					return nil
				},
				stop: func(context.Context) error {
					if !flushed {
						t.Error("service stopped before final persistence succeeded")
					}
					stopped = true
					return nil
				},
			}))
			if err != nil {
				t.Fatal(err)
			}
			if err := app.Start(context.Background(), RunOptions{Services: []string{"game"}}); err != nil {
				t.Fatal(err)
			}
			if err := app.Stop(context.Background()); !errors.Is(err, want) {
				t.Fatalf("Stop = %v", err)
			}
			if stopped || mod.stopped {
				t.Fatal("failed shutdown released resources")
			}
			wantFlushCalls := 0
			if phase == "persistence" {
				wantFlushCalls = 1
			}
			if flushCalls != wantFlushCalls || flushed {
				t.Fatalf("failed shutdown persistence: calls=%d, succeeded=%v", flushCalls, flushed)
			}
			select {
			case <-app.stopDone:
				t.Fatal("failed shutdown reported final exit")
			default:
			}
			failed = false
			if err := app.Stop(context.Background()); err != nil {
				t.Fatalf("retry Stop = %v", err)
			}
			if !stopped || !mod.stopped {
				t.Fatal("retry did not close resources")
			}
			if flushCalls != wantFlushCalls+1 || !flushed {
				t.Fatalf("retry persistence: calls=%d, succeeded=%v", flushCalls, flushed)
			}
		})
	}
}

func TestRunRetriesFailedPersistenceBeforeReturning(t *testing.T) {
	for _, recoverOn := range []int{3, 0} {
		t.Run(fmt.Sprint(recoverOn), func(t *testing.T) {
			attempts := 0
			want := errors.New("database unavailable")
			mod := &retryFlushCMod{flush: func() error {
				attempts++
				if recoverOn != 0 && attempts >= recoverOn {
					return nil
				}
				return want
			}}
			app, err := New(Config{AppID: "run-retry"}, WithCMod(mod), WithService(testService{
				spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}},
			}))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- app.Run(ctx, RunOptions{Services: []string{"game"}}) }()
			// 本用例验正常关闭重试；不能用已经取消的 ctx 跳过启动。
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for app.Context() == nil {
				select {
				case err := <-result:
					t.Fatalf("未启动即退出: %v", err)
				case <-deadline.C:
					t.Fatal("启动未完成")
				case <-time.After(time.Millisecond):
				}
			}
			cancel()
			err = <-result
			if recoverOn != 0 {
				if err != nil || attempts != 3 || !mod.stopped {
					t.Fatalf("Run = %v, attempts=%d, stopped=%v", err, attempts, mod.stopped)
				}
			} else {
				if !errors.Is(err, want) || attempts != 4 || mod.stopped {
					t.Fatalf("Run = %v, attempts=%d, stopped=%v", err, attempts, mod.stopped)
				}
				// 测试进程不退出，恢复依赖后释放本用例资源。
				mod.flush = func() error { return nil }
				if err := app.Stop(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type retryFlushCMod struct {
	flush   func() error
	stopped bool
}

func (*retryFlushCMod) Spec() CModSpec                  { return CModSpec{ID: "retry-flush"} }
func (*retryFlushCMod) Start(CModContext) error         { return nil }
func (mod *retryFlushCMod) Drain(context.Context) error { return mod.flush() }
func (mod *retryFlushCMod) Stop(context.Context) error  { mod.stopped = true; return nil }

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

func (*blockingServiceRecorder) Init(Context) error { return nil }

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
	if err := server.Start(context.Background(), RunOptions{Services: []string{"fatal-discovery-loss"}}); err != nil {
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

func (*rollbackAgentRouter) TerminateLogicalSession(context.Context, int64, int64) error {
	return errors.New("unexpected terminate")
}
