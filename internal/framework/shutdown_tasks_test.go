package framework

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestServiceTaskShutdownDeadlineRetainsResources(t *testing.T) {
	for _, test := range []struct {
		name       string
		prepare    bool
		configured bool
	}{{name: "Stop"}, {name: "PrepareHandoff", prepare: true}, {name: "ConfiguredDeadline", configured: true}} {
		t.Run(test.name, func(t *testing.T) {
			canceled, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var stops atomic.Int32
			config := Config{AppID: "task-stop"}
			if test.configured {
				config.Runtime.Lifecycle.DrainTimeout = 20 * time.Millisecond
			}
			app, err := New(config, WithService(testService{
				spec: Spec{Name: "game"},
				start: func(c Context) error {
					return c.Tasks().Go(func(ctx context.Context) {
						<-ctx.Done()
						close(canceled)
						<-release // 取消信号不能代替底层工作真正退出。
					})
				},
				stop: func(context.Context) error { stops.Add(1); return nil },
			}))
			if err != nil {
				t.Fatal(err)
			}
			if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { unblock(); _ = app.Stop(context.Background()) })
			app.runtime.setInstance(discovery.Instance{ID: "task-stop-a"})
			root := app.runtime.lifecycle
			ctx := t.Context()
			if !test.configured {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			result := make(chan error, 1)
			go func() {
				if test.prepare {
					result <- app.PrepareHandoff(ctx, "task-stop-a")
				} else {
					result <- app.Stop(ctx)
				}
			}()
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errStopDeferred) {
					t.Fatalf("应按期限返回并保留资源: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("业务任务未退出时关闭忽略了调用方期限")
			}
			select {
			case <-canceled:
			default:
				t.Fatal("未通知业务任务停止")
			}
			if stops.Load() != 0 || root.Err() != nil || app.runtime == nil {
				t.Fatal("任务仍在执行，却提前释放服务资源")
			}
			retry, cancelRetry := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancelRetry()
			if err := app.Stop(retry); !errors.Is(err, errStopDeferred) {
				t.Fatalf("实际工作未结束，重试不应提前完成: %v", err)
			}
			unblock()
			if err := app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			if stops.Load() != 1 || root.Err() == nil {
				t.Fatal("实际退出后未完成关闭")
			}
		})
	}
}
