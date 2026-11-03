package msgq

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 控制 NewStream 的返回时机，覆盖传输阻塞和取消后迟到的成功结果。
type openingMailboxClient struct {
	msgqpb.MailboxServiceClient

	entered     chan context.Context
	release     chan struct{}
	releaseOnce sync.Once

	ignoreCancel bool

	calls atomic.Int64

	recvDone chan struct{}
}

func (c *openingMailboxClient) wait(ctx context.Context) error {
	c.calls.Add(1)
	c.entered <- ctx
	if c.ignoreCancel {
		<-c.release
		return nil
	}
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *openingMailboxClient) MailboxStream(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse], error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return &openingClientStream[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]{ctx: ctx, closed: make(chan struct{}), recvDone: c.recvDone}, nil
}

func (c *openingMailboxClient) PublishStream(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[msgqpb.PublishRequest, msgqpb.PublishRoleRejection], error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return &openingClientStream[msgqpb.PublishRequest, msgqpb.PublishRoleRejection]{ctx: ctx, closed: make(chan struct{}), recvDone: c.recvDone}, nil
}

type openingClientStream[Request, Response any] struct {
	grpc.ClientStream
	ctx context.Context

	closed   chan struct{}
	recvDone chan struct{}
}

func (s *openingClientStream[Request, Response]) Send(*Request) error { return s.ctx.Err() }
func (s *openingClientStream[Request, Response]) CloseSend() error {
	close(s.closed)
	return nil
}
func (s *openingClientStream[Request, Response]) Recv() (*Response, error) {
	defer func() { s.recvDone <- struct{}{} }()
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case <-s.closed:
		return nil, io.EOF
	}
}

func newOpeningConnection(t *testing.T, ignoreCancel bool) (*grpcConnection, *openingMailboxClient) {
	t.Helper()
	transport, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := &openingMailboxClient{
		entered: make(chan context.Context, 32), release: make(chan struct{}),
		ignoreCancel: ignoreCancel, recvDone: make(chan struct{}, 32),
	}
	connection := &grpcConnection{connection: transport, client: client}
	t.Cleanup(func() {
		client.releaseOnce.Do(func() { close(client.release) })
		_ = connection.Close()
	})
	return connection, client
}

func openTestStream(ctx context.Context, c *grpcConnection, mode string, id int64) error {
	if mode == "mailbox" {
		session, err := c.OpenMailboxStream(ctx, SessionHeader{RoleID: id, SessionID: id, ConnectionID: id})
		if err == nil {
			_ = session.Close()
		}
		return err
	}
	return c.SendSession(ctx, id, []byte("down"))
}

func awaitOpeningResult(t *testing.T, result <-chan error, want error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Fatalf("操作结果 = %v，期望 %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("建流阻塞了调用取消或连接关闭")
	}
}

func TestGRPCSharedStreamOpeningCancellation(t *testing.T) {
	for _, mode := range []string{"mailbox", "publish"} {
		t.Run(mode, func(t *testing.T) {
			connection, client := newOpeningConnection(t, false)
			firstCtx, cancelFirst := context.WithCancel(t.Context())
			defer cancelFirst()
			first := make(chan error, 1)
			go func() { first <- openTestStream(firstCtx, connection, mode, 1) }()
			openingCtx := <-client.entered

			waitCtx, cancelWait := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancelWait()
			waiter := make(chan error, 1)
			go func() { waiter <- openTestStream(waitCtx, connection, mode, 2) }()
			awaitOpeningResult(t, waiter, context.DeadlineExceeded)
			cancelFirst()
			awaitOpeningResult(t, first, context.Canceled)
			if err := openingCtx.Err(); err != nil {
				t.Fatalf("调用取消中断了共享建流: %v", err)
			}

			const callers = 12
			results := make(chan error, callers)
			for id := int64(3); id < callers+3; id++ {
				go func() { results <- openTestStream(t.Context(), connection, mode, id) }()
			}
			client.releaseOnce.Do(func() { close(client.release) })
			for range callers {
				awaitOpeningResult(t, results, nil)
			}
			if calls := client.calls.Load(); calls != 1 {
				t.Fatalf("并发调用创建了 %d 条共享流", calls)
			}
		})
	}
}

func TestGRPCSharedStreamCloseCancelsOpening(t *testing.T) {
	for _, mode := range []string{"mailbox", "publish"} {
		for _, late := range []bool{false, true} {
			name := mode
			if late {
				name += "/late"
			}
			t.Run(name, func(t *testing.T) {
				connection, client := newOpeningConnection(t, late)
				result := make(chan error, 1)
				go func() { result <- openTestStream(t.Context(), connection, mode, 1) }()
				openingCtx := <-client.entered
				closed := make(chan error, 1)
				go func() { closed <- connection.Close() }()
				awaitOpeningResult(t, closed, nil)
				awaitOpeningResult(t, result, ErrSenderClosed)
				if !errors.Is(openingCtx.Err(), context.Canceled) {
					t.Fatal("Close 未取消建流上下文")
				}
				client.releaseOnce.Do(func() { close(client.release) })
				if late {
					select {
					case <-client.recvDone:
					case <-time.After(time.Second):
						t.Fatal("迟到流的读取协程未退出")
					}
				}
				if err := openTestStream(t.Context(), connection, mode, 2); !errors.Is(err, ErrSenderClosed) {
					t.Fatalf("关闭后仍可新建共享流: %v", err)
				}
				if calls := client.calls.Load(); calls != 1 {
					t.Fatalf("关闭后又创建了共享流: %d", calls)
				}
			})
		}
	}
}
