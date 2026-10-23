package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

func TestAgentUsesMailboxStreamForAckAndDownlink(t *testing.T) {
	client := &streamOnlySessionClient{down: make(chan msgq.FetchResult, 1), acks: make(chan int64, 4)}
	client.down <- msgq.FetchResult{Seq: 1, Payload: testDownPacket("down")}
	var dispatched atomic.Int64
	var connectionID atomic.Int64
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (protocol.SessionPrincipal, error) {
			return protocol.SessionPrincipal{RoleID: 470001, SessionID: 99}, nil
		}),
		Dispatcher: DispatchFunc(func(_ context.Context, request DispatchRequest) error {
			dispatched.Add(1)
			return nil
		}),
		Sessions:     sessionControlRouter{client: client},
		ConnectionID: func() (int64, error) { return connectionID.Add(1), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	transport := newInitializingSessionTransport()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, transport) }()
	select {
	case frame := <-transport.writes:
		if frame.Kind != FrameReady {
			t.Fatalf("first frame = %+v, want Ready", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("Ready timeout")
	}
	select {
	case frame := <-transport.writes:
		if frame.Kind != FrameDownMsg || frame.Seq != 1 || string(frame.Payload) != "down" {
			t.Fatalf("downlink = %+v", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("MailboxStream downlink timeout")
	}
	transport.reads <- Frame{Kind: FrameUpMsg, Seq: 5, Ack: 1, CommandHash: 1, Payload: []byte("request")}
	deadline := time.After(time.Second)
	for dispatched.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("upstream message was not dispatched")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case cursor := <-client.acks:
		if cursor != 1 {
			t.Fatalf("piggybacked cursor = %+v, want only Ack 1", cursor)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream Ack was not sent over MailboxStream")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve = %v, want context canceled", err)
	}
}

type streamOnlySessionClient struct {
	sessionControlClient
	fail context.CancelCauseFunc
	down chan msgq.FetchResult
	acks chan int64
}

func (client *streamOnlySessionClient) Init(context.Context, msgq.InitRequest) error {
	return nil
}

func (client *streamOnlySessionClient) OpenMailboxStream(ctx context.Context, _ msgq.SessionHeader) (msgq.MailboxStreamSession, error) {
	sessionCtx, cancel := context.WithCancelCause(ctx)
	client.fail = cancel
	return &streamOnlyMailboxSession{client: client, ctx: sessionCtx}, nil
}

type streamOnlyMailboxSession struct {
	client *streamOnlySessionClient
	ctx    context.Context
}

func (stream *streamOnlyMailboxSession) Recv() (msgq.FetchResult, error) {
	select {
	case message := <-stream.client.down:
		return message, nil
	case <-stream.ctx.Done():
		return msgq.FetchResult{}, stream.ctx.Err()
	}
}

func (stream *streamOnlyMailboxSession) Ack(_ context.Context, ack int64) error {
	stream.client.acks <- ack
	return nil
}

func (*streamOnlyMailboxSession) Renew(context.Context) error { return nil }
func (*streamOnlyMailboxSession) Close() error                { return nil }

type blockedDownTransport struct {
	*initializingSessionTransport
	writing chan struct{}
}

func (transport *blockedDownTransport) Write(ctx context.Context, frame Frame) error {
	if frame.Kind != FrameDownMsg {
		return transport.initializingSessionTransport.Write(ctx, frame)
	}
	close(transport.writing)
	<-ctx.Done()
	return ctx.Err()
}
func TestMailboxFailureInterruptsBlockedSocketWrite(t *testing.T) {
	client := &streamOnlySessionClient{down: make(chan msgq.FetchResult, 1), acks: make(chan int64, 4)}
	client.down <- msgq.FetchResult{Seq: 1, Payload: []byte("slow socket")}
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (protocol.SessionPrincipal, error) {
			return protocol.SessionPrincipal{RoleID: 47, SessionID: 99}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { return nil }),
		Sessions:   sessionControlRouter{client: client}, ConnectionID: func() (int64, error) { return 1, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &blockedDownTransport{initializingSessionTransport: newInitializingSessionTransport(), writing: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, transport) }()
	select {
	case <-transport.writing:
	case <-time.After(time.Second):
		t.Fatal("未开始下行写入")
	}
	client.fail(msgq.ErrMailboxBackpressure)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("邮箱失败后 Socket 写入仍阻塞")
	}
}

func (stream *streamOnlyMailboxSession) Context() context.Context { return stream.ctx }

type failedMailboxClient struct{ sessionControlClient }

func (*failedMailboxClient) OpenMailboxStream(context.Context, msgq.SessionHeader) (msgq.MailboxStreamSession, error) {
	return nil, msgq.ErrRouteUnavailable
}
func TestMailboxOpenFailureUnbindsInitializedSession(t *testing.T) {
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (protocol.SessionPrincipal, error) {
			return protocol.SessionPrincipal{RoleID: 47, SessionID: 99}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { return nil }),
		Sessions:   sessionControlRouter{client: &failedMailboxClient{}}, ConnectionID: func() (int64, error) { return 1, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Serve(ctx, newInitializingSessionTransport()); !errors.Is(err, msgq.ErrRouteUnavailable) {
		t.Fatalf("开流错误: %v", err)
	}
	server.activeSessions.mu.Lock()
	remaining := len(server.activeSessions.sessions)
	server.activeSessions.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("开流失败遗留 %d 个活动会话", remaining)
	}
}
func (*failedMailboxClient) Init(context.Context, msgq.InitRequest) error {
	return nil
}
