package framework

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/component"
)

// 最终错误不重试 Stop，但任务实际退出和其他服务未完成的关闭仍须保留依赖。
func TestServiceRollbackRetainsFinalErrorAcrossDeferredCleanup(t *testing.T) {
	initErr := errors.New("初始化失败")
	finalErr := errors.New("资源已关闭但收尾失败")
	releaseTask := make(chan struct{})
	var taskOnce sync.Once
	finishTask := func() { taskOnce.Do(func() { close(releaseTask) }) }
	var pendingReleased bool
	var finalStops, pendingStops, dependencyStops int
	app, err := New(Config{AppID: "service-rollback", Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: 20 * time.Millisecond}}},
		WithCMod(&cleanupTestCMod{id: "dependency", stop: func(context.Context) error { dependencyStops++; return nil }}),
		WithService(
			testService{spec: Spec{Name: "pending", Requires: []CModID{"dependency"}}, stop: func(context.Context) error {
				pendingStops++
				if !pendingReleased {
					return component.ErrStopDeferred
				}
				return nil
			}},
			testService{spec: Spec{Name: "failure"}, start: func(c Context) error {
				if err := c.Tasks().Go(func(ctx context.Context) { <-ctx.Done(); <-releaseTask }); err != nil {
					return err
				}
				return initErr
			}, stop: func(context.Context) error { finalStops++; return finalErr }},
		))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { finishTask(); pendingReleased = true; _ = app.Stop(context.Background()) })
	if err := app.Start(t.Context(), RunOptions{Services: []string{"pending", "failure"}}); !errors.Is(err, initErr) || !errors.Is(err, finalErr) || !errors.Is(err, errStopDeferred) {
		t.Fatalf("初始化回滚未保留原错误和待结束工作: %v", err)
	}
	if finalStops != 1 || pendingStops != 0 || dependencyStops != 0 || app.runtime == nil {
		t.Fatalf("任务退出前重复停止或提前释放: final=%d pending=%d dependency=%d", finalStops, pendingStops, dependencyStops)
	}
	if err := app.Stop(context.Background()); !errors.Is(err, finalErr) || !errors.Is(err, errStopDeferred) {
		t.Fatalf("任务等待重试丢失最终错误: %v", err)
	}
	if finalStops != 1 || pendingStops != 0 || dependencyStops != 0 {
		t.Fatal("任务未退出时重试越过了回滚边界")
	}
	finishTask()
	if err := app.Stop(context.Background()); !errors.Is(err, finalErr) || !errors.Is(err, errStopDeferred) {
		t.Fatalf("其他服务未结束时丢失最终错误: %v", err)
	}
	if finalStops != 1 || pendingStops != 1 || dependencyStops != 0 {
		t.Fatal("已停止服务被重复关闭，或未结束服务的依赖被释放")
	}
	pendingReleased = true
	if err := app.Stop(context.Background()); !errors.Is(err, finalErr) || errors.Is(err, errStopDeferred) {
		t.Fatalf("已完成回滚仍被当作未结束，或丢失最终错误: %v", err)
	}
	if finalStops != 1 || pendingStops != 2 || dependencyStops != 1 || app.runtime != nil {
		t.Fatalf("最终关闭次数错误: final=%d pending=%d dependency=%d", finalStops, pendingStops, dependencyStops)
	}
}
