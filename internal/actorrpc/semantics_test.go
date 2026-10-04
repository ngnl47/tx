package actorrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

func TestNATSTellAcceptedOnlyConfirmsServer(t *testing.T) {
	url := natsURL(t)
	ctx := actorTestContext(t)
	ns := nats.NewInbox()
	a := peer(t, url, ns, "a", echoRuntime())
	// 没有实例订阅也可确认服务端收到；这不是离线任务持久化。
	if err := a.TellAccepted(ctx, actorhost.Target{Instance: "absent", Identity: actorhost.Identity{Kind: "role", Key: "offline"}}, &actorpb.Delivery{MessageType: "reward"}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	r := actorhost.New(4, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		close(entered)
		<-release
		return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) {
			return &actorpb.Result{MessageType: "ok"}, nil
		}}, nil
	})
	b := peer(t, url, ns, "b", r)
	defer func() { close(release); _ = r.Drain(ctx) }()
	// 目标在本进程也经过 NATS；工厂阻塞不能阻塞服务端确认。
	if err := b.TellAccepted(ctx, actorhost.Target{Instance: "b", Identity: actorhost.Identity{Kind: "role", Key: "cold"}}, &actorpb.Delivery{MessageType: "reward"}); err != nil {
		t.Fatal(err)
	}
	signal(t, ctx, entered)
}

func TestLocalCallFutureKeepsCreationCancellation(t *testing.T) {
	ctx := actorTestContext(t)
	release := make(chan struct{})
	r := actorhost.New(4, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		<-release
		return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
	})
	defer func() { close(release); _ = r.Drain(ctx) }()
	p := &NATSTransport{runtime: r, instance: "local"}
	creation, cancel := context.WithCancel(ctx)
	defer cancel()
	f, err := p.CallFuture(creation, actorhost.Target{Instance: "local", Identity: actorhost.Identity{Kind: "room", Key: "1"}}, &actorpb.Delivery{MessageType: "move"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	wait, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if _, err = f.Await(wait); !errors.Is(err, context.Canceled) {
		t.Fatalf("创建时的取消不能被较晚的 Await 上下文延长: %v", err)
	}
}

func TestNATSFutureAndRuntimeProbe(t *testing.T) {
	url := natsURL(t)
	ctx := actorTestContext(t)
	ns := nats.NewInbox()
	load, execute := make(chan struct{}), make(chan struct{})
	received := make(chan struct{}, 2)
	r := actorhost.New(4, time.Second, func(c actorhost.Context) (*actorhost.Behavior, error) {
		if c.Identity.Key == "missing" {
			return nil, &actorhost.RemoteError{Code: 404, Message: "world not found"}
		}
		<-load
		return &actorhost.Behavior{Receive: func(_ actorhost.Context, value any) (any, error) {
			received <- struct{}{}
			<-execute
			d := value.(*actorpb.Delivery)
			return &actorpb.Result{MessageType: d.MessageType, Payload: d.Payload}, nil
		}}, nil
	})
	r.SetActivationLimits(actorhost.ActivationLimits{Initializations: 1})
	b := peer(t, url, ns, "b", r)
	a := peer(t, url, ns, "a", echoRuntime())
	var releases [2]bool
	defer func() {
		if !releases[0] {
			close(load)
		}
		if !releases[1] {
			close(execute)
		}
		_ = r.Drain(ctx)
		_ = a.runtime.Drain(ctx)
	}()
	target := actorhost.Target{Instance: "b", Identity: actorhost.Identity{Kind: "world", Key: "51"}}
	status, err := a.Probe(ctx, target)
	if err != nil || status.State != actorhost.Unloaded {
		t.Fatalf("未加载: %+v %v", status, err)
	}
	status, err = a.EnsureActivation(ctx, target)
	if err != nil || status.State != actorhost.Initializing {
		t.Fatalf("初始化: %+v %v", status, err)
	}
	if _, err = a.EnsureActivation(ctx, actorhost.Target{Instance: "b", Identity: actorhost.Identity{Kind: "world", Key: "52"}}); !errors.Is(err, actorhost.ErrCapacity) {
		t.Fatal("远端容量错误丢失", err)
	}
	var futures []*ResultFuture
	for _, caller := range []*NATSTransport{a, b} {
		delivery := &actorpb.Delivery{MessageType: "move", Payload: []byte("original")}
		f, err := caller.CallFuture(ctx, target, delivery)
		if err != nil {
			t.Fatal(err)
		}
		delivery.Payload[0] = 'X'
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err = f.Await(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		// 未完成加载时已取得 actorhost.Future，元数据探测仍然立即返回。
		status, err = caller.Probe(ctx, target)
		if err != nil || status.State != actorhost.Initializing {
			t.Fatalf("%+v %v", status, err)
		}
		futures = append(futures, f)
	}
	close(load)
	releases[0] = true
	signal(t, ctx, received)
	status, err = a.Probe(ctx, target)
	if err != nil || status.State != actorhost.Ready {
		t.Fatalf("忙碌实体探测: %+v %v", status, err)
	}
	close(execute)
	releases[1] = true
	for _, f := range futures {
		result, err := f.Await(ctx)
		if err != nil || string(result.Payload) != "original" {
			t.Fatalf("结果或输入隔离: %v %v", result, err)
		}
	}
	missing := actorhost.Target{Instance: "b", Identity: actorhost.Identity{Kind: "world", Key: "missing"}}
	if _, err = a.EnsureActivation(ctx, missing); err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool {
		status, err = a.Probe(ctx, missing)
		return err == nil && status.State == actorhost.Failed
	})
	if status.Failure == nil || status.Failure.Code != 404 {
		t.Fatalf("业务不存在不能混同容量不足: %+v", status)
	}
	// Tell 不等待任何回复；目标服务不存在也能交给本地连接发送。
	if err = a.Tell(context.Background(), actorhost.Target{Instance: "absent", Identity: actorhost.Identity{Kind: "role", Key: "offline"}}, &actorpb.Delivery{MessageType: "reward"}); err != nil {
		t.Fatal(err)
	}
}
