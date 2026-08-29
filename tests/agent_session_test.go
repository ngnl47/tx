package tests

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txagent "github.com/ngnl47/tx/modules/agent"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
)

func TestAgentSessionAuthenticatesThenDispatchesAcceptedSequence(t *testing.T) {
	agent, transport, _, dispatched := newAgentSessionFixture(t, nil)
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 10001, RouteKey: txagent.Int64RouteKey(470001), Payload: []byte("bon-request")}
	request := receiveDispatch(t, dispatched)
	if request.Identity.RoleID != 470001 || request.Identity.SessionID != 99 || request.Seq != 1 || request.CommandID != 10001 || string(request.Payload) != "bon-request" {
		t.Fatalf("dispatch request = %#v", request)
	}
	if key, ok := request.RouteKey.Int64(); !ok || key != 470001 {
		t.Fatalf("dispatch route key = %#v, want int64(470001)", request.RouteKey)
	}

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionRejectsUpMsgFrameBeforeHandshake(t *testing.T) {
	agent, transport, _, _ := newAgentSessionFixture(t, nil)
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, Payload: []byte("bon-request")}

	err := <-result
	if !errors.Is(err, txagent.ErrHandshakeRequired) {
		t.Fatalf("serve error = %v, want handshake required", err)
	}
}

func TestAgentSessionFetchesDownMsgAndConfirmsAckOnNextUpMsg(t *testing.T) {
	agent, transport, mailbox, dispatched := newAgentSessionFixture(t, nil)
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("first")}
	_ = receiveDispatch(t, dispatched)
	assertAgentFrame(t, transport.writes, txagent.FrameAck, 0, 1, "")

	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Payload: []byte("bon-response")}); err != nil {
		t.Fatalf("publish downMsg: %v", err)
	}
	select {
	case downMsg := <-transport.writes:
		if downMsg.Kind != txagent.FrameDownMsg || downMsg.Seq != 1 || downMsg.Ack != 1 || string(downMsg.Payload) != "bon-response" {
			t.Fatalf("downMsg frame = %#v", downMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for downMsg frame")
	}

	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 2, Ack: 1, CommandID: 1, Payload: []byte("second")}
	_ = receiveDispatch(t, dispatched)
	assertAgentFrame(t, transport.writes, txagent.FrameAck, 0, 2, "")
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := mailbox.FetchSome(deadline, msgq.FetchSomeRequest{Header: msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}, Max: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fetch after acknowledgement error = %v, want deadline exceeded", err)
	}

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionPublishesDispatchResultToCurrentSession(t *testing.T) {
	store := newMailboxSessionStore()
	mailbox, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-local", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new mailbox: %v", err)
	}
	client, err := msgq.NewLocalSessionClient(mailbox)
	if err != nil {
		t.Fatalf("new local client: %v", err)
	}
	router, err := msgq.NewFixedSessionClientRouter(client)
	if err != nil {
		t.Fatalf("new fixed router: %v", err)
	}
	agent, err := txagent.NewServer(txagent.ServerOptions{
		IdentityResolver: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (txagent.Identity, error) {
			return txagent.Identity{RoleID: 470001, SessionID: 99}, nil
		}),
		Dispatcher: txagent.DispatchFunc(func(_ context.Context, request txagent.DispatchRequest) (txagent.DispatchResult, error) {
			if request.Seq != 1 {
				t.Fatalf("dispatch seq = %d, want 1", request.Seq)
			}
			return txagent.DispatchResult{Payload: []byte("bon-response")}, nil
		}),
		Sessions: router,
		ConnectionID: func() (int64, error) {
			return 7, nil
		},
	})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	transport := newAgentTransport()
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("bon-request")}
	select {
	case downMsg := <-transport.writes:
		if downMsg.Kind != txagent.FrameDownMsg || downMsg.Seq != 1 || downMsg.Ack != 1 || string(downMsg.Payload) != "bon-response" {
			t.Fatalf("downMsg frame = %#v", downMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatch response")
	}
	select {
	case frame := <-transport.writes:
		t.Fatalf("response was followed by redundant acknowledgement = %#v", frame)
	case <-time.After(20 * time.Millisecond):
	}

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionAcknowledgesUpMsgWithoutDispatchResponse(t *testing.T) {
	agent, transport, _, dispatched := newAgentSessionFixture(t, nil)
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("bon-request")}
	_ = receiveDispatch(t, dispatched)
	assertAgentFrame(t, transport.writes, txagent.FrameAck, 0, 1, "")

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionConfirmsAckWithoutUpMsgFrame(t *testing.T) {
	agent, transport, mailbox, dispatched := newAgentSessionFixture(t, nil)
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	waitForMailboxSession(t, mailbox)
	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Payload: []byte("bon-response")}); err != nil {
		t.Fatalf("publish downMsg: %v", err)
	}
	select {
	case downMsg := <-transport.writes:
		if downMsg.Kind != txagent.FrameDownMsg || downMsg.Seq != 1 {
			t.Fatalf("downMsg frame = %#v", downMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for downMsg frame")
	}

	transport.reads <- txagent.Frame{Kind: txagent.FrameAck, Ack: 1}
	select {
	case request := <-dispatched:
		t.Fatalf("ack frame dispatched = %#v", request)
	case <-time.After(20 * time.Millisecond):
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := mailbox.FetchSome(deadline, msgq.FetchSomeRequest{Header: msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}, Max: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fetch after acknowledgement error = %v, want deadline exceeded", err)
	}

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionAcceptsAcknowledgementReturnedBeforeDownMsgWriteCompletes(t *testing.T) {
	agent, _, mailbox, _ := newAgentSessionFixture(t, nil)
	transport := newImmediateAckAgentTransport()
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	waitForMailboxSession(t, mailbox)
	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Payload: []byte("bon-response")}); err != nil {
		t.Fatalf("publish downMsg: %v", err)
	}
	select {
	case <-transport.downWriteStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for downMsg write")
	}
	select {
	case err := <-result:
		t.Fatalf("serve returned while valid acknowledgement was in flight: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(transport.releaseDownWrite)

	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := mailbox.FetchSome(deadline, msgq.FetchSomeRequest{Header: msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}, Max: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fetch after acknowledgement error = %v, want deadline exceeded", err)
	}

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionRejectsAcknowledgementBeyondWrittenDownMsg(t *testing.T) {
	agent, transport, mailbox, _ := newAgentSessionFixture(t, nil)
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	waitForMailboxSession(t, mailbox)
	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Payload: []byte("bon-response")}); err != nil {
		t.Fatalf("publish downMsg: %v", err)
	}
	select {
	case <-transport.writes:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for downMsg frame")
	}
	transport.reads <- txagent.Frame{Kind: txagent.FrameAck, Ack: 2}
	if err := <-result; !errors.Is(err, txagent.ErrInvalidAcknowledgement) {
		t.Fatalf("serve error = %v, want invalid acknowledgement", err)
	}

	restored := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8}
	if _, err := mailbox.Init(context.Background(), msgq.InitRequest{Header: restored, Restore: true}); err != nil {
		t.Fatalf("restore after rejected acknowledgement: %v", err)
	}
	message, err := mailbox.Fetch(context.Background(), msgq.FetchRequest{Header: restored})
	if err != nil || message.Seq != 1 {
		t.Fatalf("fetch after rejected acknowledgement = %#v, %v", message, err)
	}
}

func TestAgentSessionDoesNotDispatchDuplicateSequence(t *testing.T) {
	agent, transport, _, dispatched := newAgentSessionFixture(t, nil)
	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()

	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 0, "")
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("first")}
	_ = receiveDispatch(t, dispatched)
	assertAgentFrame(t, transport.writes, txagent.FrameAck, 0, 1, "")
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("duplicate")}
	select {
	case request := <-dispatched:
		t.Fatalf("duplicate dispatched = %#v", request)
	case <-time.After(20 * time.Millisecond):
	}
	assertAgentFrame(t, transport.writes, txagent.FrameAck, 0, 1, "")

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionRestoreAcknowledgesAcceptedUpMsgAndSkipsReplay(t *testing.T) {
	agent, transport, mailbox, dispatched := newAgentSessionFixture(t, nil)
	previous := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 6}
	if _, err := mailbox.Init(context.Background(), msgq.InitRequest{Header: previous}); err != nil {
		t.Fatalf("init previous session: %v", err)
	}
	if _, err := mailbox.Rec(context.Background(), msgq.RecRequest{Header: previous, Seq: 1}); err != nil {
		t.Fatalf("record previous upMsg: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- agent.Serve(context.Background(), transport) }()
	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Restore: true, Payload: []byte("token")}
	assertAgentFrame(t, transport.writes, txagent.FrameReady, 0, 1, "")

	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("replay")}
	select {
	case request := <-dispatched:
		t.Fatalf("replayed upMsg dispatched = %#v", request)
	case <-time.After(20 * time.Millisecond):
	}
	assertAgentFrame(t, transport.writes, txagent.FrameAck, 0, 1, "")

	transport.closeReads()
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("serve error = %v, want EOF", err)
	}
}

func TestAgentSessionDispatchFailureDoesNotRollbackAcceptedSequence(t *testing.T) {
	dispatchErr := errors.New("dispatch failed")
	agent, transport, mailbox, _ := newAgentSessionFixture(t, dispatchErr)
	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("bon-request")}

	err := agent.Serve(context.Background(), transport)
	if !errors.Is(err, dispatchErr) {
		t.Fatalf("serve error = %v, want dispatch failure", err)
	}
	if _, err := mailbox.Rec(context.Background(), msgq.RecRequest{Header: msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}, Seq: 2}); err != nil {
		t.Fatalf("next sequence was not accepted after dispatch failure: %v", err)
	}
}

func TestAgentSessionDoesNotAdvanceSequenceWhenAdmissionRejects(t *testing.T) {
	store := newMailboxSessionStore()
	mailbox, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-local", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new mailbox: %v", err)
	}
	client, err := msgq.NewLocalSessionClient(mailbox)
	if err != nil {
		t.Fatalf("new local client: %v", err)
	}
	router, err := msgq.NewFixedSessionClientRouter(client)
	if err != nil {
		t.Fatalf("new fixed router: %v", err)
	}
	drainErr := errors.New("agent is draining")
	agent, err := txagent.NewServer(txagent.ServerOptions{
		IdentityResolver: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (txagent.Identity, error) {
			return txagent.Identity{RoleID: 470001, SessionID: 99}, nil
		}),
		Dispatcher: txagent.DispatchFunc(func(context.Context, txagent.DispatchRequest) (txagent.DispatchResult, error) {
			t.Fatal("dispatcher must not run when admission rejects")
			return txagent.DispatchResult{}, nil
		}),
		Admission: lifecycle.AdmissionFunc(func() (func(), error) { return nil, drainErr }),
		Sessions:  router,
		ConnectionID: func() (int64, error) {
			return 7, nil
		},
	})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	transport := newAgentTransport()
	transport.reads <- txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")}
	transport.reads <- txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("bon-request")}
	if err := agent.Serve(context.Background(), transport); !errors.Is(err, drainErr) {
		t.Fatalf("Serve error = %v, want admission error", err)
	}

	if _, err := mailbox.Rec(context.Background(), msgq.RecRequest{Header: msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}, Seq: 1}); err != nil {
		t.Fatalf("rejected frame advanced sequence: %v", err)
	}
}

func newAgentSessionFixture(t *testing.T, dispatchErr error) (*txagent.Server, *agentTransport, *msgq.Server, <-chan txagent.DispatchRequest) {
	t.Helper()
	store := newMailboxSessionStore()
	mailbox, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-local", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new mailbox: %v", err)
	}
	client, err := msgq.NewLocalSessionClient(mailbox)
	if err != nil {
		t.Fatalf("new local client: %v", err)
	}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "msgq-local", Services: []string{"msgq"}, Endpoint: "local", Status: discovery.StatusRunning}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	router, err := msgq.NewSessionClientRouter(directory, store, agentSessionDialer{client: client})
	if err != nil {
		t.Fatalf("new session client router: %v", err)
	}
	dispatched := make(chan txagent.DispatchRequest, 4)
	agent, err := txagent.NewServer(txagent.ServerOptions{
		IdentityResolver: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (txagent.Identity, error) {
			return txagent.Identity{RoleID: 470001, SessionID: 99}, nil
		}),
		Dispatcher: txagent.DispatchFunc(func(_ context.Context, request txagent.DispatchRequest) (txagent.DispatchResult, error) {
			dispatched <- request
			return txagent.DispatchResult{}, dispatchErr
		}),
		Sessions:     router,
		ConnectionID: func() (int64, error) { return 7, nil },
	})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	return agent, newAgentTransport(), mailbox, dispatched
}

func receiveDispatch(t *testing.T, dispatched <-chan txagent.DispatchRequest) txagent.DispatchRequest {
	t.Helper()
	select {
	case request := <-dispatched:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatch")
		return txagent.DispatchRequest{}
	}
}

func assertAgentFrame(t *testing.T, frames <-chan txagent.Frame, kind txagent.FrameKind, seq, ack int64, payload string) {
	t.Helper()
	select {
	case frame := <-frames:
		if frame.Kind != kind || frame.Seq != seq || frame.Ack != ack || string(frame.Payload) != payload {
			t.Fatalf("frame = %#v, want kind=%d seq=%d ack=%d payload=%q", frame, kind, seq, ack, payload)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for kind=%d seq=%d ack=%d", kind, seq, ack)
	}
}

func waitForMailboxSession(t *testing.T, mailbox *msgq.Server) {
	t.Helper()
	deadline := time.After(time.Second)
	for mailbox.SessionCount() != 1 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for mailbox session")
		case <-time.After(time.Millisecond):
		}
	}
}

type agentSessionDialer struct {
	client msgq.SessionClient
}

func (d agentSessionDialer) DialSession(context.Context, discovery.Instance) (msgq.SessionClient, error) {
	return d.client, nil
}

type agentTransport struct {
	reads     chan txagent.Frame
	writes    chan txagent.Frame
	readDone  chan struct{}
	closeOnce sync.Once
}

type immediateAckAgentTransport struct {
	*agentTransport
	downWriteStarted chan struct{}
	releaseDownWrite chan struct{}
}

func newImmediateAckAgentTransport() *immediateAckAgentTransport {
	return &immediateAckAgentTransport{
		agentTransport:   newAgentTransport(),
		downWriteStarted: make(chan struct{}),
		releaseDownWrite: make(chan struct{}),
	}
}

func (t *immediateAckAgentTransport) Write(ctx context.Context, frame txagent.Frame) error {
	if frame.Kind != txagent.FrameDownMsg {
		return t.agentTransport.Write(ctx, frame)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case t.writes <- frame:
	}
	t.reads <- txagent.Frame{Kind: txagent.FrameAck, Ack: frame.Seq}
	close(t.downWriteStarted)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.releaseDownWrite:
		return nil
	}
}

func newAgentTransport() *agentTransport {
	return &agentTransport{
		reads:    make(chan txagent.Frame, 8),
		writes:   make(chan txagent.Frame, 8),
		readDone: make(chan struct{}),
	}
}

func (t *agentTransport) Read(ctx context.Context) (txagent.Frame, error) {
	select {
	case <-ctx.Done():
		return txagent.Frame{}, ctx.Err()
	case <-t.readDone:
		return txagent.Frame{}, io.EOF
	case frame := <-t.reads:
		return frame, nil
	}
}

func (t *agentTransport) Write(ctx context.Context, frame txagent.Frame) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case t.writes <- frame:
		return nil
	}
}

func (t *agentTransport) Close() error {
	t.closeReads()
	return nil
}

func (t *agentTransport) closeReads() {
	t.closeOnce.Do(func() { close(t.readDone) })
}
