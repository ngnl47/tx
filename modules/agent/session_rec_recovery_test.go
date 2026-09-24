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

// TestAcceptedUpMsgRecoveryBoundary 固定当前“MsgQ 已接受，但业务未执行”的恢复边界。
func TestAcceptedUpMsgRecoveryBoundary(t *testing.T) {
	client := &acceptedRecClient{committed: make(chan struct{})}
	var dispatched atomic.Int64
	var connectionID atomic.Int64
	options := ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (protocol.SessionPrincipal, error) {
			return protocol.SessionPrincipal{AccountID: 47, ServerID: 1, RoleID: 470001, SessionID: 99}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) (DispatchResult, error) {
			dispatched.Add(1)
			return DispatchResult{ProcessedUpSeq: 1}, nil
		}),
		Sessions: sessionControlRouter{client: client},
		ConnectionID: func() (int64, error) {
			return connectionID.Add(1), nil
		},
	}
	server, err := NewServer(options)
	if err != nil {
		t.Fatal(err)
	}

	first := newInitializingSessionTransport()
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- server.Serve(firstCtx, first) }()
	select {
	case ready := <-first.writes:
		if ready.Kind != FrameReady || ready.Ack != 0 {
			t.Fatalf("fresh Ready = %+v", ready)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh Ready timeout")
	}
	first.reads <- Frame{Kind: FrameUpMsg, Seq: 1, CommandID: 1}
	select {
	case <-client.committed:
	case <-time.After(time.Second):
		t.Fatal("Rec commit timeout")
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Serve = %v", err)
	}

	replacement, err := NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	restored := newInitializingSessionTransport()
	<-restored.reads
	restored.reads <- Frame{Kind: FrameHandshake, Restore: true, Payload: []byte("ticket")}
	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	restoreDone := make(chan error, 1)
	go func() { restoreDone <- replacement.Serve(restoreCtx, restored) }()
	select {
	case ready := <-restored.writes:
		if ready.Kind != FrameReady || ready.Ack != 1 {
			t.Fatalf("restored Ready = %+v, want Ack 1", ready)
		}
	case <-time.After(time.Second):
		t.Fatal("restored Ready timeout")
	}
	if got := dispatched.Load(); got != 0 {
		t.Fatalf("business dispatch = %d, want 0", got)
	}
	cancelRestore()
	if err := <-restoreDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("restored Serve = %v", err)
	}
}

type acceptedRecClient struct {
	sessionControlClient
	mu        sync.Mutex
	upSeq     int64
	committed chan struct{}
}

func (client *acceptedRecClient) Init(_ context.Context, _ msgq.InitRequest) (msgq.InitResult, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return msgq.InitResult{UpMsgSeq: client.upSeq}, nil
}

func (client *acceptedRecClient) Rec(ctx context.Context, request msgq.RecRequest) (msgq.RecResult, error) {
	client.mu.Lock()
	client.upSeq = request.Seq
	client.mu.Unlock()
	close(client.committed)
	<-ctx.Done()
	return msgq.RecResult{}, ctx.Err()
}

func (*acceptedRecClient) FetchSome(ctx context.Context, _ msgq.FetchSomeRequest) (msgq.FetchSomeResult, error) {
	<-ctx.Done()
	return msgq.FetchSomeResult{}, ctx.Err()
}
