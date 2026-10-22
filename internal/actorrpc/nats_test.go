package actorrpc

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/protobuf/proto"
)

func natsTestContext(t *testing.T) (string, context.Context) {
	t.Helper()
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		t.Skip("设置 TX_ACTORTRIAL_NATS_URL 验证真实 NATS")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return url, ctx
}

func TestNATSSharedHostSemantics(t *testing.T) {
	url, ctx := natsTestContext(t)
	ns := nats.NewInbox()
	runtime := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		count := uint64(0)
		return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) {
			count++
			return &actorpb.Result{MessageType: "count", CommandHash: uint32(count)}, nil
		}}, nil
	})
	runtime.SetActivationLimits(actorhost.ActivationLimits{Actors: 1})
	server, err := ConnectNATS(url, ns, "server", runtime, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer runtime.Drain(ctx)
	clientRuntime := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) { return nil, actorhost.ErrActivationDenied })
	client, err := ConnectNATS(url, ns, "client", clientRuntime, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer clientRuntime.Drain(ctx)
	target := actorhost.Target{Instance: "server", Identity: actorhost.Identity{Type: "role", ID: "1"}}
	if err := client.CheckActivationCapacity(ctx, target); err != nil {
		t.Fatal(err)
	}
	// 预检不创建实体，也不预留容量。
	if usage := runtime.ActivationUsage(); usage.Actors != 0 || usage.Initializing != 0 {
		t.Fatalf("预检预留了容量: %+v", usage)
	}
	if status, err := client.Probe(ctx, target); err != nil || status.State != actorhost.Unloaded {
		t.Fatalf("probe: %+v %v", status, err)
	}
	if err := client.TellAccepted(ctx, target, &actorpb.Delivery{MessageType: "increment"}); err != nil {
		t.Fatal(err)
	}
	result, err := client.Call(ctx, target, &actorpb.Delivery{MessageType: "increment"})
	if err != nil || result.CommandHash != 2 {
		t.Fatalf("NATS count: %+v %v", result, err)
	}
	other := actorhost.Target{Instance: "server", Identity: actorhost.Identity{Type: "role", ID: "2"}}
	if err := client.CheckActivationCapacity(ctx, other); !errors.Is(err, actorhost.ErrCapacity) {
		t.Fatalf("容量不足: %v", err)
	}
	// 直连接收入口复用同一宿主，不能创建第二份实体状态。
	data, err := proto.Marshal(&actorpb.DirectStreamCall{Type: "role", Id: "1", TimeoutNano: int64(time.Second), Delivery: &actorpb.Delivery{MessageType: "increment", RequestId: 1, Mode: actorpb.DeliveryMode_DELIVERY_MODE_CALL}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := NewDirectReceiver("server", runtime).CallStream(ctx, rpc.Delivery{Route: rpc.InstanceRoute("server"), MessageType: "actor.streamcall", Payload: data})
	if err != nil {
		t.Fatal(err)
	}
	var direct actorpb.Result
	if err := proto.Unmarshal(response.Payload, &direct); err != nil || direct.CommandHash != 3 {
		t.Fatalf("direct count: %+v %v", &direct, err)
	}
	future, err := client.CallFuture(ctx, target, &actorpb.Delivery{MessageType: "increment"})
	if err != nil {
		t.Fatal(err)
	}
	result, err = future.Await(ctx)
	if err != nil || result.CommandHash != 4 {
		t.Fatalf("future count: %+v %v", result, err)
	}
	if err := runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Call(ctx, target, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatalf("drain rejection: %v", err)
	}
	if err := client.CheckActivationCapacity(ctx, target); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatalf("关闸后容量预检: %v", err)
	}
	// Broker 到达确认不要求目标在线；Call 则必须报告无接收者。
	target.Instance = "absent"
	if err := client.Tell(ctx, target, &actorpb.Delivery{}); err != nil {
		t.Fatal(err)
	}
	if err := client.TellAccepted(ctx, target, &actorpb.Delivery{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Call(ctx, target, &actorpb.Delivery{}); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("absent target: %v", err)
	}
	if err := server.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNATSPendingCleanup(t *testing.T) {
	for _, mode := range []string{"close", "cancel"} {
		t.Run(mode, func(t *testing.T) { testNATSPendingCleanup(t, mode) })
	}
}

func testNATSPendingCleanup(t *testing.T, mode string) {
	url, ctx := natsTestContext(t)
	ns := nats.NewInbox()
	entered, release := make(chan struct{}), make(chan struct{})
	runtime := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) {
			close(entered)
			<-release
			return &actorpb.Result{MessageType: "done"}, nil
		}}, nil
	})
	server, err := ConnectNATS(url, ns, "server", runtime, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer runtime.Drain(ctx)
	defer close(release)
	client, err := ConnectNATS(url, ns, "client", runtime, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	target := actorhost.Target{Instance: "server", Identity: actorhost.Identity{Type: "role", ID: "1"}}
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	future, err := client.CallFuture(requestCtx, target, &actorpb.Delivery{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := client.CallFuture(ctx, target, &actorpb.Delivery{}); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("pending limit: %v", err)
	}
	var want error = nats.ErrConnectionClosed
	if mode == "close" {
		client.Close()
	} else {
		cancel()
		want = context.Canceled
	}
	if _, err := future.Await(ctx); !errors.Is(err, want) {
		t.Fatalf("pending close: %v", err)
	}
	select {
	case <-future.future.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	client.mu.Lock()
	pending := len(client.pending)
	client.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending retained: %d", pending)
	}
}

func TestNATSDeactivateBeforeKeepsOutboundRPC(t *testing.T) {
	for _, destination := range []string{"online", "absent"} {
		t.Run(destination, func(t *testing.T) {
			url, ctx := natsTestContext(t)
			ns := nats.NewInbox()
			peerRuntime := actorhost.New(2, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
				return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) { return &actorpb.Result{MessageType: "notified"}, nil }}, nil
			})
			peer, err := ConnectNATS(url, ns, "online", peerRuntime, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			defer peerRuntime.Drain(ctx)
			entered, release := make(chan struct{}), make(chan struct{})
			releaseWork := sync.OnceFunc(func() { close(release) })
			before, after := make(chan error, 1), make(chan struct{})
			var source *NATSTransport
			runtime := actorhost.New(2, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
				return &actorhost.Behavior{
					Receive: func(actorhost.Context, any) (any, error) {
						close(entered)
						<-release
						return &actorpb.Result{MessageType: "completed"}, nil
					},
					OnDeactivateBefore: func(hook actorhost.Context) error {
						_, err := source.Call(hook, actorhost.Target{Instance: destination, Identity: actorhost.Identity{Type: "role", ID: "peer"}}, &actorpb.Delivery{})
						before <- err
						return err
					},
					OnDeactivateAfter: func(actorhost.Context) error { close(after); return nil },
				}, nil
			})
			source, err = ConnectNATS(url, ns, "source", runtime, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			defer runtime.Drain(ctx)
			defer releaseWork()
			target := actorhost.Target{Instance: "source", Identity: actorhost.Identity{Type: "room", ID: "1"}}
			future, err := peer.CallFuture(ctx, target, &actorpb.Delivery{})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			runtime.BeginDrain()
			if _, err := peer.Call(ctx, target, &actorpb.Delivery{}); !errors.Is(err, actorhost.ErrDraining) {
				t.Fatalf("关闸后 NATS 准入: %v", err)
			}
			releaseWork()
			if err := runtime.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			// 已接受请求的回包及退出 RPC 都须在断开 NATS 前完成。
			if err := source.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			result, err := future.Await(ctx)
			if err != nil || result.MessageType != "completed" {
				t.Fatalf("已接受请求回包: %v %v", result, err)
			}
			hookErr := <-before
			if destination == "online" && hookErr != nil {
				t.Fatalf("退出 RPC: %v", hookErr)
			}
			if destination == "absent" && !errors.Is(hookErr, nats.ErrNoResponders) {
				t.Fatalf("退出 RPC 失败: %v", hookErr)
			}
			select {
			case <-after:
			default:
				t.Fatal("退出 RPC 失败阻止了卸载清理")
			}
		})
	}
}
