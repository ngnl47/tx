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
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// 使用正式 Server 收尾流程与新宿主；测试服务仅承担待迁入 CModActor 的装配。
func TestActorHostPrepareHandoffRetainsTransportUntilStop(t *testing.T) {
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		t.Skip("设置 TX_ACTORTRIAL_NATS_URL 验证 NATS 与框架收尾")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	entered, release, draining := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseWork := sync.OnceFunc(func() { close(release) })
	var cleaned atomic.Bool
	local := actorhost.New(2, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		return &actorhost.Behavior{
			Receive: func(actorhost.Context, any) (any, error) {
				close(entered)
				<-release
				return &actorpb.Result{MessageType: "done"}, nil
			},
			OnDeactivateAfter: func(actorhost.Context) error { cleaned.Store(true); return nil },
		}, nil
	})
	ns := nats.NewInbox()
	client, err := actorrpc.ConnectNATS(url, ns, "caller", local, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var flushes atomic.Int32
	failure := errors.New("写回失败")
	mod := &retryFlushCMod{flush: func() error {
		if !cleaned.Load() {
			t.Error("实体卸载未完成就开始最终写回")
		}
		if flushes.Add(1) == 1 {
			return failure
		}
		return nil
	}}
	observeDrain := sync.OnceFunc(func() {
		if !local.Draining() {
			t.Error("框架未在服务排水前关闭实体准入")
		}
		close(draining)
	})
	var app *Server
	app, err = New(Config{AppID: "actor-handoff"}, WithCMod(mod), WithService(testService{
		spec: Spec{Name: "game", Requires: []CModID{CModRPC, mod.Spec().ID}},
		start: func(Context) error {
			// 隔离身份；真实 Discovery 租约由 etcd 交接测试覆盖。
			app.runtime.setInstance(discovery.Instance{ID: "source"})
			return app.startEntityTransport(local, url, ns, 8)
		},
		drain: func(context.Context) error { observeDrain(); return nil },
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(ctx, RunOptions{Services: "game"}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	defer releaseWork()
	transport := app.runtime.actorRPC.(*actorrpc.NATSTransport)
	output := &handoffFlushTransport{NATSTransport: transport, stored: &flushes}
	app.runtime.actorRPC = output
	target := actorhost.Target{Instance: "source", Identity: actorhost.Identity{Kind: "role", Key: "1"}}
	future, err := client.CallFuture(ctx, target, &actorpb.Delivery{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// 排水等待取消不能越过宿主屏障或释放仍在执行业务的资源。
	cancelled, stopWait := context.WithCancel(ctx)
	stopWait()
	if err := app.Stop(cancelled); !errors.Is(err, errStopDeferred) || !errors.Is(err, context.Canceled) {
		t.Fatalf("取消排水: %v", err)
	}
	if app.Context() == nil || cleaned.Load() || flushes.Load() != 0 || mod.stopped {
		t.Fatal("取消等待释放了业务资源或执行了最终写回")
	}
	prepared := make(chan error, 1)
	go func() { prepared <- app.PrepareHandoff(ctx, "source") }()
	select {
	case <-draining:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if flushes.Load() != 0 {
		t.Fatal("已接受业务未完成就执行写回")
	}
	releaseWork()
	if err := <-prepared; !errors.Is(err, failure) {
		t.Fatalf("首次准备: %v", err)
	}
	if result, err := future.Await(ctx); err != nil || result.MessageType != "done" {
		t.Fatalf("已接受请求回包: %+v %v", result, err)
	}
	// 写回失败保留 NATS 控制入口；业务准入仍关闭。
	if _, err := client.Probe(ctx, target); err != nil {
		t.Fatalf("失败提前关闭 NATS: %v", err)
	}
	if _, err := client.Call(ctx, target, &actorpb.Delivery{}); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatalf("失败后重新开放准入: %v", err)
	}
	// 写回成功后的传输确认失败同样保留资源，Stop 也不得绕过这个边界。
	if err := app.PrepareHandoff(ctx, "source"); !errors.Is(err, errHandoffFlush) {
		t.Fatalf("传输确认失败: %v", err)
	}
	if err := app.Stop(ctx); !errors.Is(err, errStopDeferred) || !errors.Is(err, errHandoffFlush) {
		t.Fatalf("Stop 提前释放资源: %v", err)
	}
	if app.Context() == nil || mod.stopped || flushes.Load() != 2 {
		t.Fatal("传输失败丢失写回进度或释放了资源")
	}
	if _, err := client.Probe(ctx, target); err != nil {
		t.Fatalf("传输失败关闭连接: %v", err)
	}
	for range 2 {
		if err := app.PrepareHandoff(ctx, "source"); err != nil {
			t.Fatal(err)
		}
	}
	if flushes.Load() != 2 || mod.stopped {
		t.Fatal("重复写回或提前停止")
	}
	if output.attempts != 3 {
		t.Fatalf("重复传输确认: %d", output.attempts)
	}
	// 成功准备之后，共享 gRPC 仍返回明确拒绝，而不是连接消失。
	conn, err := grpc.NewClient(app.runtime.rpc.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	data, err := proto.Marshal(&actorpb.Delivery{})
	if err != nil {
		t.Fatal(err)
	}
	callCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("actor-kind-bin", "role", "actor-key-bin", "1"))
	response, err := rpcpb.NewDeliveryServiceClient(conn).Call(callCtx, &rpcpb.Delivery{Route: &rpcpb.Route{Kind: rpcpb.RouteKind_ROUTE_KIND_INSTANCE, InstanceId: "source"}, MessageType: "actor.call", Payload: data})
	if err != nil || response.GetMessageType() != "actor.error" {
		t.Fatalf("交接准备关闭共享 gRPC: %+v %v", response, err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if flushes.Load() != 2 || !mod.stopped {
		t.Fatal("Stop 未复用收尾结果")
	}
	if _, err := transport.Probe(ctx, actorhost.Target{Instance: "absent", Identity: target.Identity}); !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("Stop 未关闭 NATS: %v", err)
	}
}

var errHandoffFlush = errors.New("传输确认暂时失败")

// 注入 Flush 失败，真实 NATS 连接及后续恢复保持不变。
type handoffFlushTransport struct {
	*actorrpc.NATSTransport
	stored   *atomic.Int32
	attempts int
}

func (p *handoffFlushTransport) Flush(ctx context.Context) error {
	if p.stored.Load() != 2 {
		return errors.New("最终写回完成前调用 Flush")
	}
	p.attempts++
	if p.attempts <= 2 {
		return errHandoffFlush
	}
	return p.NATSTransport.Flush(ctx)
}
