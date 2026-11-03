package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

func TestAgentDisconnectWaitsForMailboxCloseConfirmation(t *testing.T) {
	closeFailure := errors.New("远端解除绑定未确认")
	client := &closeConfirmationClient{
		streamOnlySessionClient: streamOnlySessionClient{down: make(chan msgq.FetchResult)},
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
		failure:                 closeFailure,
	}
	releaseClose := sync.OnceFunc(func() { close(client.release) })
	defer releaseClose()
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (AuthResult, error) {
			return AuthResult{SessionPrincipal: protocol.SessionPrincipal{RoleID: 1, SessionID: 2}}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { return nil }),
		Sessions:   sessionControlRouter{client: client},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transport := newInitializingSessionTransport()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, transport) }()
	select {
	case frame := <-transport.writes:
		if frame.Kind != FrameReady {
			t.Fatalf("接入返回=%+v，期望 Ready", frame)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.entered:
	case <-ctx.Done():
		t.Fatal("连接关闭后未请求解除远端绑定")
	}
	select {
	case err := <-done:
		t.Fatalf("解除绑定尚未确认，Serve 已返回：%v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseClose()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, closeFailure) {
			t.Fatalf("Serve=%v，应保留断线原因和关闭错误", err)
		}
	case <-ctx.Done():
		t.Fatal("解除绑定返回后 Serve 未退出")
	}
	if client.terminated.Load() {
		t.Fatal("普通断线不得终止逻辑会话")
	}
}

type closeConfirmationClient struct {
	streamOnlySessionClient
	entered chan struct{}
	release chan struct{}
	failure error

	terminated atomic.Bool
}

func (client *closeConfirmationClient) OpenMailboxStream(ctx context.Context, request msgq.InitRequest, revoke func()) (msgq.MailboxStreamSession, error) {
	stream, err := client.streamOnlySessionClient.OpenMailboxStream(ctx, request, revoke)
	if err != nil {
		return nil, err
	}
	return &closeConfirmationStream{MailboxStreamSession: stream, client: client}, nil
}

type closeConfirmationStream struct {
	msgq.MailboxStreamSession
	client *closeConfirmationClient
}

func (stream *closeConfirmationStream) Close() error {
	close(stream.client.entered)
	<-stream.client.release
	return stream.client.failure
}

func (stream *closeConfirmationStream) Terminate(context.Context) error {
	stream.client.terminated.Store(true)
	return nil
}
