package framework

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/internal/actorhost"
	"sync/atomic"
	"testing"
	"time"
)

type flushTestService struct {
	testService
	flush func(context.Context) error
}

func (s flushTestService) Flush(ctx context.Context) error { return s.flush(ctx) }

// 服务持久化失败必须保留运行时，下一次 Stop 重试成功后才释放服务。
func TestServiceFlushFailureRetainsRuntime(t *testing.T) {
	attempts, stops, stableFlushes := 0, 0, 0
	var unloaded atomic.Bool
	local := actorhost.New(2, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		return &actorhost.Behavior{
			Receive:           func(actorhost.Context, any) (any, error) { return nil, nil },
			OnDeactivateAfter: func(actorhost.Context) error { unloaded.Store(true); return nil },
		}, nil
	})
	failure := errors.New("持久化失败")
	app, err := New(Config{AppID: "service-flush"}, WithService(flushTestService{
		testService: testService{spec: Spec{Name: "world"}, stop: func(context.Context) error { stops++; return nil }},
		flush: func(context.Context) error {
			if !unloaded.Load() {
				t.Error("Actor 尚未卸载就进入服务最终保存")
			}
			attempts++
			if attempts == 1 {
				return failure
			}
			return nil
		},
	}), WithService(flushTestService{
		testService: testService{spec: Spec{Name: "stable"}},
		flush:       func(context.Context) error { stableFlushes++; return nil },
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err = app.Start(t.Context(), RunOptions{Services: []string{"world", "stable"}}); err != nil {
		t.Fatal(err)
	}
	app.runtime.entities = local
	if _, err := local.Call(t.Context(), actorhost.Identity{Type: "world", ID: "1"}, nil); err != nil {
		t.Fatal(err)
	}
	if err = app.Stop(t.Context()); !errors.Is(err, failure) || stops != 0 || app.Context() == nil {
		t.Fatalf("失败未保留资源：%v stops=%d", err, stops)
	}
	if err = app.Stop(t.Context()); err != nil || stops != 1 || attempts != 2 || stableFlushes != 1 {
		t.Fatalf("重试失败：%v attempts=%d stops=%d stable=%d", err, attempts, stops, stableFlushes)
	}
}
