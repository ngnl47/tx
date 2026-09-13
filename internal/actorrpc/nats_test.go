package actorrpc

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc/metadata"
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
			return &actorpb.Result{MessageType: "count", CommandId: count}, nil
		}}, nil
	})
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
	target := actorhost.Target{Instance: "server", Identity: actorhost.Identity{Kind: "role", Key: "1"}}
	if status, err := client.Probe(ctx, target); err != nil || status.State != actorhost.Unloaded {
		t.Fatalf("probe: %+v %v", status, err)
	}
	if err := client.TellAccepted(ctx, target, &actorpb.Delivery{MessageType: "increment"}); err != nil {
		t.Fatal(err)
	}
	result, err := client.Call(ctx, target, &actorpb.Delivery{MessageType: "increment"})
	if err != nil || result.CommandId != 2 {
		t.Fatalf("NATS count: %+v %v", result, err)
	}
	// 直连接收入口复用同一宿主，不能创建第二份实体状态。
	data, err := proto.Marshal(&actorpb.Delivery{MessageType: "increment", RequestId: 1})
	if err != nil {
		t.Fatal(err)
	}
	directCtx := metadata.NewIncomingContext(ctx, metadata.Pairs("actor-kind-bin", "role", "actor-key-bin", "1"))
	response, err := NewDirectReceiver("server", runtime).Call(directCtx, rpc.Delivery{Route: rpc.InstanceRoute("server"), MessageType: "actor.call", Payload: data})
	if err != nil {
		t.Fatal(err)
	}
	var direct actorpb.Result
	if err := proto.Unmarshal(response.Payload, &direct); err != nil || direct.CommandId != 3 {
		t.Fatalf("direct count: %+v %v", &direct, err)
	}
	future, err := client.CallFuture(ctx, target, &actorpb.Delivery{MessageType: "increment"})
	if err != nil {
		t.Fatal(err)
	}
	result, err = future.Await(ctx)
	if err != nil || result.CommandId != 4 {
		t.Fatalf("future count: %+v %v", result, err)
	}
	if err := runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Call(ctx, target, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatalf("drain rejection: %v", err)
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
	target := actorhost.Target{Instance: "server", Identity: actorhost.Identity{Kind: "role", Key: "1"}}
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
