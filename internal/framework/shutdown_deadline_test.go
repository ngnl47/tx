package framework

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/component"
	"github.com/ngnl47/tx/lifecycle"
)

// 正常 Stop 的原始 ctx 没有期限时，各收尾阶段仍须遵守配置；未完成阶段可重试。
func TestShutdownCallbackDeadlineAndRetry(t *testing.T) {
	for _, phase := range []string{"service-flush", "cmod-drain", "service-stop"} {
		t.Run(phase, func(t *testing.T) {
			var released bool
			var attempts, dependencyStops, stableStops int
			callback := func(ctx context.Context) error {
				attempts++
				if released {
					return nil
				}
				if _, ok := ctx.Deadline(); !ok {
					return errors.New("收尾阶段没有配置期限")
				}
				<-ctx.Done()
				if phase == "service-stop" {
					return errors.Join(component.ErrStopDeferred, ctx.Err())
				}
				return ctx.Err()
			}
			mod := &cleanupTestCMod{id: "dependency", stop: func(context.Context) error { dependencyStops++; return nil }}
			service := flushTestService{testService: testService{spec: Spec{Name: "game", Requires: []CModID{"dependency"}}}, flush: func(context.Context) error { return nil }}
			switch phase {
			case "service-flush":
				service.flush = callback
			case "cmod-drain":
				mod.drain = callback
			case "service-stop":
				service.stop = callback
			}
			app, err := New(Config{AppID: "shutdown-deadline", Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: 20 * time.Millisecond}}},
				WithCMod(mod), WithService(service, testService{spec: Spec{Name: "stable"}, stop: func(context.Context) error { stableStops++; return nil }}))
			if err != nil {
				t.Fatal(err)
			}
			if err := app.Start(t.Context(), RunOptions{Services: []string{"game", "stable"}}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { released = true; _ = app.Stop(context.Background()) })
			root := app.runtime.lifecycle
			if err := app.Stop(context.Background()); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errStopDeferred) {
				t.Fatalf("收尾未按期限保留资源: %v", err)
			}
			if dependencyStops != 0 || app.runtime == nil || root.Err() != nil {
				t.Fatal("未完成收尾就释放了依赖")
			}
			released = true
			if err := app.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			if attempts != 2 || dependencyStops != 1 || stableStops != 1 {
				t.Fatalf("重试阶段错误: attempts=%d dependencyStops=%d stableStops=%d", attempts, dependencyStops, stableStops)
			}
		})
	}
}

// 已完成关闭的期限错误不表示工作仍在运行，不能无限保留依赖。
func TestServiceStopFinalErrorReleasesDependencies(t *testing.T) {
	var stopped bool
	app, err := New(Config{AppID: "stop-final-error"},
		WithCMod(&cleanupTestCMod{id: "dependency", stop: func(context.Context) error { stopped = true; return nil }}),
		WithService(testService{spec: Spec{Name: "game", Requires: []CModID{"dependency"}}, stop: func(context.Context) error { return context.DeadlineExceeded }}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(context.Background()); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errStopDeferred) {
		t.Fatalf("把最终错误误判为未完成: %v", err)
	}
	if !stopped || app.runtime != nil {
		t.Fatal("关闭已结束却没有释放依赖")
	}
	status, _ := app.ServiceStatus("game")
	if status.State != lifecycle.ServiceStopped || !errors.Is(status.Failure, context.DeadlineExceeded) {
		t.Fatalf("关闭已结束的失败状态错误: %+v", status)
	}
}
