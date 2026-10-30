package framework

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/component"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
)

// 同一阻塞分别出现在正常 CMod 关闭、CMod 启动失败及服务初始化回滚中。
func TestLifecycleCleanupDeadlineAndRetry(t *testing.T) {
	for _, mode := range []string{"cmod-stop", "cmod-start-failure", "service-init-failure"} {
		t.Run(mode, func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var resourceStops, dependencyStops atomic.Int32
			startFailure := errors.New("初始化失败")
			work := func(tasks lifecycle.Tasks) error {
				return tasks.Go(func(ctx context.Context) { <-ctx.Done(); <-release })
			}
			dependency := &cleanupTestCMod{id: "dependency", stop: func(context.Context) error { dependencyStops.Add(1); return nil }}
			owner := &cleanupTestCMod{id: "owner", requires: []CModID{"dependency"},
				start: func(c CModContext) error {
					if mode == "service-init-failure" {
						return nil
					}
					if err := work(c.Tasks()); err != nil {
						return err
					}
					if mode == "cmod-start-failure" {
						return startFailure
					}
					return nil
				}, stop: func(context.Context) error { resourceStops.Add(1); return nil },
			}
			service := testService{spec: Spec{Name: "game", Requires: []CModID{"owner"}}, start: func(c Context) error {
				if mode != "service-init-failure" {
					return nil
				}
				if err := work(c.Tasks()); err != nil {
					return err
				}
				return startFailure
			}}
			config := Config{AppID: "cleanup-retry"}
			config.Runtime.Lifecycle.DrainTimeout = 20 * time.Millisecond
			app, err := New(config, WithCMod(dependency, owner), WithService(service))
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				err := app.Start(t.Context(), RunOptions{Services: []string{"game"}})
				if mode == "cmod-stop" && err == nil {
					err = app.Stop(context.Background())
				}
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, errStopDeferred) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("未按期限保留关闭状态: %v", err)
				}
				if mode != "cmod-stop" && !errors.Is(err, startFailure) {
					t.Fatalf("丢失原启动错误: %v", err)
				}
			case <-time.After(time.Second):
				unblock()
				<-result
				_ = app.Stop(context.Background())
				t.Fatal("清理未遵守配置期限")
			}
			t.Cleanup(func() { unblock(); _ = app.Stop(context.Background()) })
			if app.runtime == nil || dependencyStops.Load() != 0 || resourceStops.Load() != 0 {
				t.Fatal("实际工作未结束就释放了 CMod 资源")
			}
			if err := app.Stop(context.Background()); !errors.Is(err, errStopDeferred) {
				t.Fatalf("阻塞期间重试 = %v", err)
			}
			unblock()
			if err := app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			if app.runtime != nil || dependencyStops.Load() != 1 || resourceStops.Load() != 1 {
				t.Fatal("退出后未恰好释放一次资源")
			}
			if err := app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type cleanupTestCMod struct {
	id       CModID
	requires []CModID
	start    func(CModContext) error
	stop     func(context.Context) error
	drain    func(context.Context) error
}

func (m *cleanupTestCMod) Spec() CModSpec { return CModSpec{ID: m.id, Requires: m.requires} }
func (m *cleanupTestCMod) Start(c CModContext) error {
	if m.start != nil {
		return m.start(c)
	}
	return nil
}
func (m *cleanupTestCMod) Stop(ctx context.Context) error {
	if m.stop != nil {
		return m.stop(ctx)
	}
	return nil
}
func (m *cleanupTestCMod) Drain(ctx context.Context) error {
	if m.drain != nil {
		return m.drain(ctx)
	}
	return nil
}

func TestCModStopRetryDoesNotRepeatFinishedStages(t *testing.T) {
	var flushes, drains, serviceStops, cleanups, stops int
	app, err := New(Config{AppID: "stop-stages", Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: 20 * time.Millisecond}}},
		WithCMod(&cleanupTestCMod{id: "owner", drain: func(context.Context) error { drains++; return nil }, stop: func(ctx context.Context) error {
			stops++
			if stops == 1 {
				<-ctx.Done()
				return errors.Join(component.ErrStopDeferred, ctx.Err())
			}
			return nil
		}}),
		WithService(flushTestService{testService: testService{spec: Spec{Name: "game", Requires: []CModID{"owner"}}, stop: func(context.Context) error { serviceStops++; return nil }}, flush: func(context.Context) error { flushes++; return nil }}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	app.cleanups = append(app.cleanups, func() { cleanups++ })
	if err := app.Stop(t.Context()); !errors.Is(err, errStopDeferred) {
		t.Fatalf("Stop = %v", err)
	}
	if app.Context() != nil {
		t.Fatal("最终释放重试期间仍暴露运行中服务")
	}
	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if flushes != 1 || drains != 1 || serviceStops != 1 || cleanups != 1 || stops != 2 {
		t.Fatalf("重复收尾: flush=%d drain=%d service=%d cleanup=%d stop=%d", flushes, drains, serviceStops, cleanups, stops)
	}
}

func TestFailedCModStartupDoesNotDrainPartialComponent(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var drains atomic.Int32
	failure := errors.New("启动未完成")
	app, err := New(Config{AppID: "partial-start", Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: 20 * time.Millisecond}}},
		WithCMod(&cleanupTestCMod{id: "partial", start: func(c CModContext) error {
			if err := c.Tasks().Go(func(ctx context.Context) { <-ctx.Done(); <-release }); err != nil {
				return err
			}
			return failure
		}, drain: func(context.Context) error { drains.Add(1); return nil }}),
		WithService(testService{spec: Spec{Name: "game", Requires: []CModID{"partial"}}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock(); _ = app.Stop(context.Background()) })
	if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); !errors.Is(err, failure) || !errors.Is(err, errStopDeferred) {
		t.Fatalf("Start = %v", err)
	}
	unblock()
	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if drains.Load() != 0 {
		t.Fatal("对初始化未完成的组件执行了正常排水")
	}
}

func TestPlacementAndCModTasksHideOwnership(t *testing.T) {
	check := func(ctx context.Context) error {
		if _, ok := ctx.(interface{ Cancel() bool }); ok {
			return errors.New("任务视图暴露 Cancel")
		}
		if _, ok := ctx.(interface{ Wait() }); ok {
			return errors.New("任务视图暴露 Wait")
		}
		return nil
	}
	checkTasks := func(tasks lifecycle.Tasks) error {
		if err := check(tasks); err != nil {
			return err
		}
		result := make(chan error, 1)
		if err := tasks.Go(func(ctx context.Context) { result <- check(ctx) }); err != nil {
			return err
		}
		return <-result
	}
	t.Run("placement", func(t *testing.T) {
		root := lifecycle.NewWaitGroupContext(t.Context())
		defer func() { root.Cancel(); root.Wait() }()
		p := &actorPlacement{tasks: lifecycle.NewChildWaitGroupContext(root), ingress: lifecycle.NewIngress(root), providers: make(map[placementKey]txactor.GroupResolver)}
		err := p.initialize(&runtime{lifecycle: root}, []Service{testService{spec: Spec{Name: "game", Placements: []Placement{{Name: "server", Factory: func(c Context) (txactor.GroupResolver, error) {
			return func(context.Context, txactor.Identity) (string, error) { return "1001", nil }, checkTasks(c.Tasks())
		}}}}}})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("cmod", func(t *testing.T) {
		root := lifecycle.NewWaitGroupContext(t.Context())
		defer func() { root.Cancel(); root.Wait() }()
		server := &Server{runtime: &runtime{lifecycle: root}}
		host := newCModHost(context.Background(), newCModContext(server), cmodGraph{ordered: []CMod{&cleanupTestCMod{id: "owner", start: func(c CModContext) error { return checkTasks(c.Tasks()) }}}})
		defer host.stop(context.Background())
		if err := host.start(); err != nil {
			t.Fatal(err)
		}
	})
}

// 服务释放自己的发送器时，进程级传输入口必须仍然有效。
func TestServiceStopPrecedesRootCancellation(t *testing.T) {
	var app *Server
	service := testService{spec: Spec{Name: "game"}, stop: func(context.Context) error {
		if err := app.runtime.lifecycle.Err(); err != nil {
			return err
		}
		return nil
	}}
	var err error
	app, err = New(Config{AppID: "service-stop-order"}, WithService(service))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}
