package framework

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/lifecycle"
	"testing"
)

type phasedService struct {
	testService
	initHook  func(Context) error
	startHook func(context.Context) error
}

func (s phasedService) Init(ctx Context) error          { return s.initHook(ctx) }
func (s phasedService) Start(ctx context.Context) error { return s.startHook(ctx) }

func TestServiceStartupPhasesAndAdmission(t *testing.T) {
	var server *Server
	var admission lifecycle.Admission
	service := phasedService{testService: testService{spec: Spec{Name: "world"}}}
	service.initHook = func(ctx Context) error {
		admission = ctx.Admission()
		state, _ := server.ServiceStatus("world")
		if state.State != lifecycle.ServiceInitializing {
			t.Fatalf("Init state = %v", state)
		}
		if _, err := admission.Enter(); !errors.Is(err, lifecycle.ErrNotReady) {
			t.Fatalf("Init admission = %v", err)
		}
		return nil
	}
	service.startHook = func(ctx context.Context) error {
		state, _ := server.ServiceStatus("world")
		if state.State != lifecycle.ServiceStarting {
			t.Fatalf("Start state = %v", state)
		}
		if _, err := admission.Enter(); !errors.Is(err, lifecycle.ErrNotReady) {
			t.Fatalf("Start admission = %v", err)
		}
		return nil
	}
	var err error
	server, err = New(Config{AppID: "phase-test"}, WithService(service))
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Start(context.Background(), RunOptions{Services: []string{"world"}}); err != nil {
		t.Fatal(err)
	}
	state, _ := server.ServiceStatus("world")
	if state.State != lifecycle.ServiceRunning {
		t.Fatalf("running = %v", state)
	}
	done, err := admission.Enter()
	if err != nil {
		t.Fatal(err)
	}
	done()
	if err := server.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ = server.ServiceStatus("world")
	if state.State != lifecycle.ServiceStopped {
		t.Fatalf("stopped = %v", state)
	}
}

func TestServiceStartFailureRollsBackInitializedServices(t *testing.T) {
	want := errors.New("required world failed")
	stopped := 0
	service := phasedService{testService: testService{spec: Spec{Name: "world"}, stop: func(context.Context) error { stopped++; return nil }},
		initHook: func(Context) error { return nil }, startHook: func(context.Context) error { return want }}
	server, err := New(Config{AppID: "phase-failure"}, WithService(service))
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Start(context.Background(), RunOptions{Services: []string{"world"}}); !errors.Is(err, want) {
		t.Fatalf("start = %v", err)
	}
	state, _ := server.ServiceStatus("world")
	if stopped != 1 || state.State != lifecycle.ServiceStopped || !errors.Is(state.Failure, want) {
		t.Fatalf("stop=%d status=%v", stopped, state)
	}
}

func TestAllServicesInitializeBeforeStartupAndOpenTogether(t *testing.T) {
	var secondInitialized bool
	var firstAdmission lifecycle.Admission
	var server *Server
	first := phasedService{testService: testService{spec: Spec{Name: "first"}},
		initHook: func(ctx Context) error { firstAdmission = ctx.Admission(); return nil },
		startHook: func(context.Context) error {
			if !secondInitialized {
				t.Fatal("尚未完成全部装配")
			}
			return nil
		}}
	second := phasedService{testService: testService{spec: Spec{Name: "second"}},
		initHook: func(Context) error { secondInitialized = true; return nil }, startHook: func(context.Context) error {
			if _, err := firstAdmission.Enter(); !errors.Is(err, lifecycle.ErrNotReady) {
				t.Fatal("组合启动未完成即接纳业务", err)
			}
			if server.runtime.instanceSnapshot().Status != "running" {
				t.Fatal("基础设施状态尚未就绪")
			}
			return nil
		}}
	var err error
	server, err = New(Config{AppID: "phase-composition"}, WithService(first), WithService(second))
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Start(t.Context(), RunOptions{Services: []string{"first", "second"}}); err != nil {
		t.Fatal(err)
	}
	if err = server.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRunCancellationReachesStartupHook(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan struct{})
	stopped := false
	service := phasedService{testService: testService{spec: Spec{Name: "world"}, stop: func(context.Context) error { stopped = true; return nil }},
		initHook: func(Context) error { return nil }, startHook: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}
	server, err := New(Config{AppID: "phase-cancel"}, WithService(service))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- server.Run(ctx, RunOptions{Services: []string{"world"}}) }()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) || !stopped {
		t.Fatal(err, stopped)
	}
}

func TestDrainTimeoutRetainsServicePhaseAndResources(t *testing.T) {
	var admission lifecycle.Admission
	stopped := false
	server, err := New(Config{AppID: "phase-drain"}, WithService(testService{spec: Spec{Name: "game"}, start: func(ctx Context) error { admission = ctx.Admission(); return nil }, stop: func(context.Context) error { stopped = true; return nil }}))
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	done, err := admission.Enter()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = server.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	state, _ := server.ServiceStatus("game")
	if state.State != lifecycle.ServiceDraining || stopped {
		t.Fatalf("status=%v stopped=%v", state, stopped)
	}
	done()
	if err = server.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, _ = server.ServiceStatus("game")
	if state.State != lifecycle.ServiceStopped || !stopped {
		t.Fatalf("status=%v stopped=%v", state, stopped)
	}
}

func TestFailedStartupDrainDoesNotOpenActorAdmission(t *testing.T) {
	phase := &servicePhase{}
	runtime := &runtime{servicePhases: map[string]*servicePhase{"world": phase}, actorCatalog: map[string]actorTypeSpec{"world": {service: "world"}}}
	phase.set(lifecycle.ServiceStarting, nil)
	if err := runtime.actorAdmission("world", true); err != nil {
		t.Fatal(err)
	}
	phase.set(lifecycle.ServiceDraining, errors.New("load failed"))
	if err := runtime.actorAdmission("world", false); !errors.Is(err, lifecycle.ErrNotReady) {
		t.Fatal("启动失败清理期间开放了业务", err)
	}
	phase.set(lifecycle.ServiceRunning, nil)
	phase.set(lifecycle.ServiceDraining, nil)
	if err := runtime.actorAdmission("world", false); err != nil {
		t.Fatal("正常排水提前阻断已领取任务", err)
	}
}
