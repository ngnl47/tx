package actorrpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 单条流必须承载多个独立 Actor 调用，慢 Actor 不能阻塞另一条结果。
func TestDirectStreamMultiplexesCallsWithoutBatching(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	started := make(chan struct{}, 1)
	local := actorhost.New(32, time.Second, func(c txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) {
			if c.Identity.Key == "slow" {
				started <- struct{}{}
				<-release
			}
			return &actorpb.Result{MessageType: c.Identity.Key}, nil
		}}, nil
	})
	defer local.Drain(ctx)
	var unary, batch, streams atomic.Int64
	host, err := rpc.NewHost("127.0.0.1:0",
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if info.FullMethod == rpcpb.DeliveryService_Call_FullMethodName {
				unary.Add(1)
			}
			return next(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			if info.FullMethod == rpcpb.DeliveryService_CallBatch_FullMethodName {
				batch.Add(1)
			}
			if info.FullMethod == rpcpb.DeliveryService_CallStream_FullMethodName {
				streams.Add(1)
			}
			return next(srv, stream)
		}))
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() { _ = host.Stop(context.Background()); root.Cancel(); root.Wait() }()
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return rpc.RegisterDeliveryService(registrar, nil, NewDirectReceiver("game-a", local))
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		t.Fatal(err)
	}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "game-a", Services: []string{"game"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	dialer, err := rpc.NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	transport := &StreamingDirectTransport{Sender: sender}
	type reply struct {
		result *actorpb.Result
		err    error
	}
	slow := make(chan reply, 1)
	go func() {
		result, err := transport.Call(ctx, actorhost.Target{Instance: "game-a", Identity: txactor.Identity{Kind: "role", Key: "slow"}}, &actorpb.Delivery{MessageType: "ping"})
		slow <- reply{result, err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	fast, err := transport.Call(ctx, actorhost.Target{Instance: "game-a", Identity: txactor.Identity{Kind: "role", Key: "fast"}}, &actorpb.Delivery{MessageType: "ping"})
	if err != nil || fast == nil || fast.MessageType != "fast" {
		t.Fatalf("快请求被阻塞或结果错配: result=%v err=%v", fast, err)
	}
	close(release)
	answer := <-slow
	if answer.err != nil || answer.result == nil || answer.result.MessageType != "slow" {
		t.Fatalf("慢请求结果异常: %+v", answer)
	}
	if unary.Load() != 0 || batch.Load() != 0 || streams.Load() != 1 {
		t.Fatalf("期望只建立一条双向流: unary=%d batch=%d stream=%d", unary.Load(), batch.Load(), streams.Load())
	}
}
