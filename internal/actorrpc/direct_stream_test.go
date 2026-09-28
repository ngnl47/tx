package actorrpc

import (
	"context"
	"errors"
	"sync"
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
	txprotocol "github.com/ngnl47/tx/protocol"
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
			if info.FullMethod == "/tx.rpc.v1.DeliveryService/CallBatch" {
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
	transport := &DirectTransport{Sender: sender}
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
	local.BeginDrain()
	if _, err := transport.Call(ctx, actorhost.Target{Instance: "game-a", Identity: txactor.Identity{Kind: "role", Key: "fast"}}, &actorpb.Delivery{MessageType: "ping"}); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatalf("排水关闸后仍接受新调用: %v", err)
	}
	drained := make(chan error, 1)
	go func() { drained <- local.Drain(ctx) }()
	select {
	case err := <-drained:
		t.Fatalf("排水未等待已接受调用: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	answer := <-slow
	if answer.err != nil || answer.result == nil || answer.result.MessageType != "slow" {
		t.Fatalf("慢请求结果异常: %+v", answer)
	}
	if err := <-drained; err != nil {
		t.Fatalf("已接受调用完成后排水失败: %v", err)
	}
	if unary.Load() != 0 || batch.Load() != 0 || streams.Load() != 1 {
		t.Fatalf("期望只建立一条双向流: unary=%d batch=%d stream=%d", unary.Load(), batch.Load(), streams.Load())
	}
}

// 单向流按接收顺序入邮箱；排水等待已接收工作，关闸后不再执行新消息。
func TestDirectStreamEnqueuePreservesMailboxOrderAndDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	releaseLast := make(chan struct{})
	var releaseLastOnce sync.Once
	defer releaseLastOnce.Do(func() { close(releaseLast) })
	entered := make(chan int64, 3)
	identity := txprotocol.SessionIdentity{SessionPrincipal: txprotocol.SessionPrincipal{
		AccountID: 1, ServerID: 1, RoleID: 42, SessionID: 99, AccessMode: txprotocol.AccessModeNormal,
	}, ConnectionID: 7}
	local := actorhost.New(8, time.Second, func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(_ txactor.Context, message any) (any, error) {
			delivery, ok := message.(txactor.Delivery)
			if !ok {
				return nil, txactor.ErrInvalidDelivery
			}
			entered <- delivery.UpSeq
			if delivery.UpSeq == 1 {
				<-release
			}
			if delivery.UpSeq == 3 {
				<-releaseLast
			}
			return nil, nil
		}}, nil
	})
	defer local.Drain(ctx)
	var streams atomic.Int64
	host, err := rpc.NewHost("127.0.0.1:0", grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
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
		return rpc.RegisterDeliveryService(registrar, NewDirectReceiver("game-a", local), NewDirectReceiver("game-a", local))
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
	transport := &DirectTransport{Sender: sender}
	for seq := int64(1); seq <= 3; seq++ {
		delivery, err := (txactor.Delivery{MessageType: "agent.command", Identity: &identity, UpSeq: seq}).ToProto()
		if err != nil {
			t.Fatal(err)
		}
		if err := transport.Enqueue(ctx, actorhost.Target{Instance: "game-a", Identity: txactor.Identity{Kind: "role", Key: "i:42"}}, delivery); err != nil {
			t.Fatalf("enqueue %d: %v", seq, err)
		}
		if seq == 1 {
			select {
			case got := <-entered:
				if got != 1 {
					t.Fatalf("first mailbox item = %d, want 1", got)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	releaseOnce.Do(func() { close(release) })
	for want := int64(2); want <= 3; want++ {
		select {
		case got := <-entered:
			if got != want {
				t.Fatalf("mailbox item = %d, want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("wait mailbox item %d: %v", want, ctx.Err())
		}
	}
	if streams.Load() != 1 {
		t.Fatalf("stream count = %d, want 1", streams.Load())
	}
	local.BeginDrain()
	drained := make(chan error, 1)
	go func() { drained <- local.Drain(ctx) }()
	select {
	case err := <-drained:
		t.Fatalf("排水未等待已接收的单向消息: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	delivery, err := (txactor.Delivery{MessageType: "agent.command", Identity: &identity, UpSeq: 4}).ToProto()
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Enqueue(ctx, actorhost.Target{Instance: "game-a", Identity: txactor.Identity{Kind: "role", Key: "i:42"}}, delivery); err != nil {
		t.Fatalf("单向流写入应保持无逐项回执: %v", err)
	}
	if _, err := transport.Call(ctx, actorhost.Target{Instance: "game-a", Identity: txactor.Identity{Kind: "role", Key: "i:42"}}, delivery); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatalf("关闸后同步探针未被拒绝: %v", err)
	}
	releaseLastOnce.Do(func() { close(releaseLast) })
	if err := <-drained; err != nil {
		t.Fatalf("已接收工作完成后排水失败: %v", err)
	}
	select {
	case got := <-entered:
		t.Fatalf("关闸后的消息仍被执行: %d", got)
	default:
	}
}
