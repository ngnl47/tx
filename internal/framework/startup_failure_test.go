package framework

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

// 身份失效来自真实 Discovery 观察入口；用同一 reportFatal 确定性控制时序。
func TestFatalDuringStartupCancelsHookAndNeverOpensAdmission(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	var admission lifecycle.Admission
	service := phasedService{testService: testService{spec: Spec{Name: "world"}},
		initHook: func(c Context) error { admission = c.Admission(); return nil },
		startHook: func(ctx context.Context) error {
			close(entered)
			select {
			case <-ctx.Done():
				close(canceled)
			case <-release:
			}
			// 即使回调没有转发错误，框架也不能宣布成功。
			return nil
		},
	}
	app, err := New(Config{AppID: "fatal-startup"}, WithService(service))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- app.Start(t.Context(), RunOptions{Services: []string{"world"}}) }()
	<-entered
	want := errors.New("identity lost during startup")
	app.reportFatal(want)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		close(release)
		<-result
		_ = app.Stop(context.Background())
		t.Fatal("致命错误没有取消启动回调")
	}
	if err := <-result; !errors.Is(err, want) {
		t.Fatalf("Start = %v", err)
	}
	if done, err := admission.Enter(); err == nil {
		done()
		t.Fatal("失败启动开放了业务准入")
	}
	if app.Context() != nil {
		t.Fatal("失败启动仍暴露运行时")
	}
	if app.runtime != nil {
		t.Fatal("失败启动未清理运行时")
	}
}

func TestCanceledStartupWithoutHookDoesNotBecomeRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	app, err := New(Config{AppID: "canceled-startup"}, WithService(testService{spec: Spec{Name: "game"}}))
	if err != nil {
		t.Fatal(err)
	}
	err = app.Start(ctx, RunOptions{Services: []string{"game"}})
	defer app.Stop(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v", err)
	}
}

func TestFatalDuringCModStartupSkipsFollowingComponents(t *testing.T) {
	var app *Server
	want := errors.New("framework startup failed")
	laterStarted := false
	source := &cleanupTestCMod{id: "source", start: func(CModContext) error { app.reportFatal(want); return nil }}
	later := &cleanupTestCMod{id: "later", requires: []CModID{"source"}, start: func(CModContext) error { laterStarted = true; return nil }}
	var err error
	app, err = New(Config{AppID: "fatal-cmod"}, WithCMod(source, later), WithService(testService{spec: Spec{Name: "game", Requires: []CModID{"later"}}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); !errors.Is(err, want) {
		t.Fatalf("Start = %v", err)
	}
	if laterStarted {
		t.Fatal("框架已经失败，仍继续启动后续组件")
	}
}
