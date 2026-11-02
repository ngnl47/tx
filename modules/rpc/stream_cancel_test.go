package rpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 用可控的传输阻塞检验调用者期限，不通过等待远端回执模拟 Send。
type blockedStreamClient struct {
	rpcpb.DeliveryServiceClient
	mu          sync.Mutex
	streams     []*blockedClientStream
	entered     chan struct{}
	openRelease chan struct{}
	blockOpen   bool
}

func (c *blockedStreamClient) CallStream(ctx context.Context, _ ...grpc.CallOption) (rpcpb.DeliveryService_CallStreamClient, error) {
	if c.blockOpen {
		close(c.entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.openRelease:
			return nil, ErrSenderClosed
		}
	}
	s := &blockedClientStream{ctx: ctx, entered: c.entered, release: make(chan struct{})}
	c.mu.Lock()
	c.streams = append(c.streams, s)
	c.mu.Unlock()
	return s, nil
}
func (c *blockedStreamClient) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.streams {
		s.releaseOnce.Do(func() { close(s.release) })
	}
}

type blockedClientStream struct {
	grpc.ClientStream
	ctx         context.Context
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	once        sync.Once
}

func (s *blockedClientStream) Send(r *rpcpb.StreamCallRequest) error {
	if r.GetDelivery().GetMessageType() != "blocked" {
		return nil
	}
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.release:
		return nil
	}
}
func (s *blockedClientStream) Recv() (*rpcpb.StreamCallResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func TestStreamSendCancellation(t *testing.T) {
	for _, mode := range []string{"tell", "call"} {
		t.Run(mode, func(t *testing.T) {
			client := &blockedStreamClient{entered: make(chan struct{})}
			connection := &grpcConnection{client: client}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			var sendErr error
			go func() {
				defer close(done)
				delivery := Delivery{Route: InstanceRoute("game"), MessageType: "blocked"}
				if mode == "tell" {
					sendErr = connection.TellStream(ctx, delivery)
				} else {
					_, sendErr = connection.CallStream(ctx, delivery)
				}
			}()
			<-client.entered
			t.Cleanup(func() {
				cancel()
				client.stop()
				connection.streamMu.Lock()
				s := connection.stream
				connection.streamMu.Unlock()
				if s != nil {
					s.fail(ErrSenderClosed)
				}
				<-done
			})
			cancel()
			select {
			case <-done:
				if !errors.Is(sendErr, context.Canceled) {
					t.Fatalf("发送取消=%v", sendErr)
				}
			case <-time.After(time.Second):
				t.Fatal("操作取消未中断实际 Send")
			}
			if err := connection.TellStream(t.Context(), Delivery{Route: InstanceRoute("game"), MessageType: "new"}); err != nil {
				t.Fatalf("新请求未建立新流：%v", err)
			}
			client.mu.Lock()
			n := len(client.streams)
			client.mu.Unlock()
			if n != 2 {
				t.Fatalf("流数量=%d，阻塞流必须失效且旧请求不得重投", n)
			}
		})
	}
}

func TestStreamSendWaiterCancellationDoesNotCloseActiveSend(t *testing.T) {
	client := &blockedStreamClient{entered: make(chan struct{})}
	connection := &grpcConnection{client: client}
	first := make(chan struct{})
	var firstErr error
	go func() {
		defer close(first)
		firstErr = connection.TellStream(t.Context(), Delivery{Route: InstanceRoute("game"), MessageType: "blocked"})
	}()
	<-client.entered
	t.Cleanup(func() {
		client.stop()
		<-first
		connection.streamMu.Lock()
		s := connection.stream
		connection.streamMu.Unlock()
		if s != nil {
			s.fail(ErrSenderClosed)
		}
	})
	for _, mode := range []string{"tell", "call"} {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			delivery := Delivery{Route: InstanceRoute("game"), MessageType: "waiting"}
			if mode == "tell" {
				done <- connection.TellStream(ctx, delivery)
			} else {
				_, err := connection.CallStream(ctx, delivery)
				done <- err
			}
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("等待取消=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s 等待发送锁未响应取消", mode)
		}
	}
	select {
	case <-first:
		t.Fatalf("等待者取消中断了发送者：%v", firstErr)
	default:
	}
	client.stop()
	<-first
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	if err := connection.TellStream(t.Context(), Delivery{Route: InstanceRoute("game"), MessageType: "next"}); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	n := len(client.streams)
	client.mu.Unlock()
	if n != 1 {
		t.Fatalf("等待者取消不应关闭共享流：%d", n)
	}
}

func TestStreamOpeningCancellation(t *testing.T) {
	client := &blockedStreamClient{entered: make(chan struct{}), openRelease: make(chan struct{}), blockOpen: true}
	connection := &grpcConnection{client: client}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- connection.TellStream(ctx, Delivery{Route: InstanceRoute("game"), MessageType: "open"})
	}()
	<-client.entered
	t.Cleanup(func() { close(client.openRelease) })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("开流取消=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("首次开流未响应操作取消")
	}
}

// BenchmarkTellStreamSend 单独观察框架发送路径；不把本地无阻塞传输当成网络吞吐。
func BenchmarkTellStreamSend(b *testing.B) {
	client := &blockedStreamClient{}
	connection := &grpcConnection{client: client}
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	defer func() {
		connection.streamMu.Lock()
		s := connection.stream
		connection.streamMu.Unlock()
		if s != nil {
			s.fail(ErrSenderClosed)
		}
	}()
	delivery := Delivery{Route: InstanceRoute("game"), MessageType: "bench", Payload: make([]byte, 128)}
	if err := connection.TellStream(ctx, delivery); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := connection.TellStream(ctx, delivery); err != nil {
			b.Fatal(err)
		}
	}
}

func TestStreamOpeningWaiterAndClose(t *testing.T) {
	client := &blockedStreamClient{entered: make(chan struct{}), openRelease: make(chan struct{}), blockOpen: true}
	transport, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	connection := &grpcConnection{client: client, connection: transport}
	done := make(chan error, 1)
	go func() {
		done <- connection.TellStream(t.Context(), Delivery{Route: InstanceRoute("game"), MessageType: "open"})
	}()
	<-client.entered
	t.Cleanup(func() { _ = connection.Close(); close(client.openRelease) })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := connection.TellStream(ctx, Delivery{Route: InstanceRoute("game"), MessageType: "waiting"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("共享开流等待=%v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("等待者影响首个开流：%v", err)
	default:
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrSenderClosed) {
			t.Fatalf("关闭后开流结果=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("关闭未中断开流")
	}
}

func TestCompletedSendCancellationKeepsSharedStream(t *testing.T) {
	client := &blockedStreamClient{}
	connection := &grpcConnection{client: client}
	ctx, cancel := context.WithCancel(t.Context())
	defer func() {
		cancel()
		connection.streamMu.Lock()
		s := connection.stream
		connection.streamMu.Unlock()
		if s != nil {
			s.fail(ErrSenderClosed)
		}
	}()
	delivery := Delivery{Route: InstanceRoute("game"), MessageType: "done"}
	if err := connection.TellStream(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := connection.TellStream(t.Context(), delivery); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	n := len(client.streams)
	client.mu.Unlock()
	if n != 1 {
		t.Fatalf("已完成项的取消关闭了共享流：%d", n)
	}
}
