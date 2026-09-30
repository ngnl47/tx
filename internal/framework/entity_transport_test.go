package framework

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/internal/actorrpc"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

func TestEntityTransportStartupFailureUnloadsBeforeDependencies(t *testing.T) {
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		t.Skip("设置 TX_ACTORTRIAL_NATS_URL 验证启动失败收尾")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	releaseWork := sync.OnceFunc(func() { close(release) })
	defer releaseWork()
	var pending *actorrpc.ResultFuture
	var unloaded atomic.Bool
	local := actorhost.New(4, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		return &actorhost.Behavior{
			Receive: func(actorhost.Context, any) (any, error) {
				close(entered)
				<-release
				return &actorpb.Result{MessageType: "ready"}, nil
			},
			OnDeactivateAfter: func(actorhost.Context) error { unloaded.Store(true); return nil },
		}, nil
	})
	ns := nats.NewInbox()
	clientHost := actorhost.New(4, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) { return nil, actorhost.ErrActivationDenied })
	client, err := actorrpc.ConnectNATS(url, ns, "caller", clientHost, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer clientHost.Drain(ctx)
	mod := &retryFlushCMod{flush: func() error {
		if !unloaded.Load() {
			t.Error("实体清理之前进入依赖收尾")
		}
		return nil
	}}
	failure := errors.New("后续服务启动失败")
	var transport *actorrpc.NATSTransport
	var app *Server
	app, err = New(Config{AppID: "entity-start-failure", Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: 10 * time.Millisecond}}}, WithCMod(mod), WithService(testService{
		spec: Spec{Name: "game", Requires: []CModID{CModRPC, mod.Spec().ID}},
		start: func(Context) error {
			app.runtime.setInstance(discovery.Instance{ID: "source"})
			if err := app.startEntityTransport(local, url, ns, 8); err != nil {
				return err
			}
			transport = app.runtime.actorRPC.(*actorrpc.NATSTransport)
			var err error
			pending, err = client.CallFuture(ctx, actorhost.Target{Instance: "source", Identity: actorhost.Identity{Kind: "role", Key: "1"}}, &actorpb.Delivery{})
			if err != nil {
				return err
			}
			select {
			case <-entered:
			case <-ctx.Done():
				return ctx.Err()
			}

			return failure
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseWork()
		if err := app.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if err := app.Start(ctx, RunOptions{Services: []string{"game"}}); !errors.Is(err, failure) || !errors.Is(err, errStopDeferred) {
		t.Fatalf("启动结果: %v", err)
	}
	if unloaded.Load() || mod.stopped || app.runtime == nil || !local.Draining() {
		t.Fatal("启动失败等待超时释放了依赖")
	}
	if err := app.Start(ctx, RunOptions{Services: []string{"game"}}); err == nil {
		t.Fatal("失败资源未回收却允许再次启动")
	}
	releaseWork()
	if result, err := pending.Await(ctx); err != nil || result.MessageType != "ready" {
		t.Fatalf("已接受消息丢失: %+v %v", result, err)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !unloaded.Load() || !local.Draining() || !mod.stopped || app.Context() != nil {
		t.Fatal("启动失败未完成宿主与依赖回收")
	}
	if _, err := transport.Probe(ctx, actorhost.Target{Instance: "absent", Identity: actorhost.Identity{Kind: "role", Key: "1"}}); !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("启动失败泄漏 NATS: %v", err)
	}
}

func TestRunRetriesEntityStartupCleanup(t *testing.T) {
	failure := errors.New("启动失败")
	attempts := 0
	mod := &retryFlushCMod{flush: func() error {
		attempts++
		if attempts == 1 {
			return errors.New("首次收尾失败")
		}
		return nil
	}}
	var app *Server
	var err error
	app, err = New(Config{AppID: "startup-retry"}, WithCMod(mod), WithService(testService{
		spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}},
		start: func(Context) error {
			app.runtime.entities = actorhost.New(1, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
				t.Error("启动失败测试不应加载实体")
				return nil, actorhost.ErrActivationDenied
			})
			return failure
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Run(t.Context(), RunOptions{Services: []string{"game"}}); !errors.Is(err, failure) {
		t.Fatalf("Run 丢失启动错误: %v", err)
	}
	if attempts != 2 || !mod.stopped || app.runtime != nil {
		t.Fatalf("Run 未重试收尾: attempts=%d stopped=%v", attempts, mod.stopped)
	}
}
