package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

func TestTerminateSessionFollowsConcurrentReplacementConnection(t *testing.T) {
	oldClient := &sessionControlClient{terminateErr: msgq.ErrSessionChanged}
	newClient := &sessionControlClient{}
	server := &Server{activeSessions: newSessionRegistry()}
	oldSession := testActiveSession(7, oldClient)
	newSession := testActiveSession(8, newClient)
	server.activeSessions.sessions[oldSession.header.SessionID] = oldSession
	server.activeSessions.beginInitialization(newSession)

	type outcome struct {
		result SessionControlResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := server.TerminateSession(context.Background(), 99)
		done <- outcome{result: result, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("TerminateSession returned before replacement initialization completed: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}
	previous, ready, terminateOnInitialization := server.activeSessions.completeInitialization(newSession, true)
	if previous != nil || ready {
		t.Fatalf("replacement activation = previous %#v, ready %t; want terminated pending session", previous, ready)
	}
	if !terminateOnInitialization {
		t.Fatal("replacement did not retain termination after initialization")
	}
	newSession.completeTermination(newClient.TerminateSession(context.Background(), newSession.header))

	got := <-done
	if got.err != nil {
		t.Fatalf("terminate session: %v", got.err)
	}
	if !got.result.Handled || got.result.SessionID != 99 || got.result.ConnectionID != 8 {
		t.Fatalf("terminate result = %#v", got.result)
	}
	if newClient.terminateCalls != 1 {
		t.Fatalf("replacement terminate calls = %d, want 1", newClient.terminateCalls)
	}
}

func TestTerminateSessionCompensatesAmbiguousInitializationFailure(t *testing.T) {
	oldClient := &sessionControlClient{terminateErr: msgq.ErrSessionChanged}
	newClient := &sessionControlClient{}
	server := &Server{activeSessions: newSessionRegistry()}
	oldSession := testActiveSession(7, oldClient)
	newSession := testActiveSession(8, newClient)
	server.activeSessions.sessions[oldSession.header.SessionID] = oldSession
	server.activeSessions.beginInitialization(newSession)

	done := make(chan error, 1)
	go func() {
		_, err := server.TerminateSession(context.Background(), 99)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("TerminateSession returned before ambiguous initialization was compensated: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	_, ready, terminateOnInitialization := server.activeSessions.completeInitialization(newSession, false)
	if ready || !terminateOnInitialization {
		t.Fatalf("ambiguous initialization = ready %t, terminate %t; want false/true", ready, terminateOnInitialization)
	}
	newSession.completeTermination(newClient.TerminateSession(context.Background(), newSession.header))
	if err := <-done; err != nil {
		t.Fatalf("TerminateSession returned error: %v", err)
	}
}

func TestTerminateSessionReportsReplacementCompensationFailure(t *testing.T) {
	compensationErr := errors.New("msgq unavailable")
	oldClient := &sessionControlClient{terminateEntered: make(chan struct{})}
	newClient := &sessionControlClient{terminateErr: compensationErr}
	server := &Server{activeSessions: newSessionRegistry()}
	oldSession := testActiveSession(7, oldClient)
	newSession := testActiveSession(8, newClient)
	server.activeSessions.sessions[oldSession.header.SessionID] = oldSession
	server.activeSessions.beginInitialization(newSession)

	done := make(chan error, 1)
	go func() {
		_, err := server.TerminateSession(context.Background(), 99)
		done <- err
	}()
	<-oldClient.terminateEntered
	_, ready, terminateOnInitialization := server.activeSessions.completeInitialization(newSession, true)
	if ready || !terminateOnInitialization {
		t.Fatalf("replacement initialization = ready %t, terminate %t; want false/true", ready, terminateOnInitialization)
	}
	newSession.completeTermination(newClient.TerminateSession(context.Background(), newSession.header))
	if err := <-done; !errors.Is(err, compensationErr) {
		t.Fatalf("TerminateSession error = %v, want %v", err, compensationErr)
	}
}

func TestConcurrentTerminateSessionCallsShareOneOperation(t *testing.T) {
	client := &sessionControlClient{terminateEntered: make(chan struct{}), releaseTerminate: make(chan struct{})}
	server := &Server{activeSessions: newSessionRegistry()}
	session := testActiveSession(7, client)
	server.activeSessions.sessions[session.header.SessionID] = session

	type outcome struct {
		result SessionControlResult
		err    error
	}
	firstDone := make(chan outcome, 1)
	secondDone := make(chan outcome, 1)
	go func() {
		result, err := server.TerminateSession(context.Background(), 99)
		firstDone <- outcome{result: result, err: err}
	}()
	<-client.terminateEntered
	go func() {
		result, err := server.TerminateSession(context.Background(), 99)
		secondDone <- outcome{result: result, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	client.mu.Lock()
	calls := client.terminateCalls
	client.mu.Unlock()
	if calls != 1 {
		t.Fatalf("concurrent terminate calls = %d, want one shared operation", calls)
	}
	close(client.releaseTerminate)
	for index, result := range []outcome{<-firstDone, <-secondDone} {
		if result.err != nil || !result.result.Handled || result.result.ConnectionID != 7 {
			t.Fatalf("outcome[%d] = %#v", index, result)
		}
	}
}

func TestServeRegistersInitializingSessionBeforeMsgQInitCompletes(t *testing.T) {
	client := &sessionControlClient{
		initEntered: make(chan struct{}),
		releaseInit: make(chan struct{}),
	}
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (protocol.SessionPrincipal, error) {
			return protocol.SessionPrincipal{AccountID: 47, ServerID: 1, RoleID: 470001, SessionID: 99}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) (DispatchResult, error) {
			return DispatchResult{}, nil
		}),
		Sessions:     sessionControlRouter{client: client},
		ConnectionID: func() (int64, error) { return 8, nil },
	})
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	transport := newInitializingSessionTransport()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background(), transport) }()
	<-client.initEntered

	result := server.DisconnectSession(99)
	if !result.Handled || result.ConnectionID != 8 {
		t.Fatalf("DisconnectSession result = %#v, want initializing connection", result)
	}
	close(client.releaseInit)
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after initializing connection was disconnected")
	}
	select {
	case frame := <-transport.writes:
		t.Fatalf("initializing connection wrote frame after disconnect: %#v", frame)
	default:
	}
}

func TestTerminateSessionCompensatesCanceledMsgQInit(t *testing.T) {
	client := &sessionControlClient{
		initEntered: make(chan struct{}),
		releaseInit: make(chan struct{}),
	}
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (protocol.SessionPrincipal, error) {
			return protocol.SessionPrincipal{AccountID: 47, ServerID: 1, RoleID: 470001, SessionID: 99}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) (DispatchResult, error) {
			return DispatchResult{}, nil
		}),
		Sessions:     sessionControlRouter{client: client},
		ConnectionID: func() (int64, error) { return 8, nil },
	})
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	transport := newInitializingSessionTransport()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background(), transport) }()
	<-client.initEntered

	result, terminateErr := server.TerminateSession(context.Background(), 99)
	if terminateErr != nil {
		t.Fatalf("TerminateSession returned error: %v", terminateErr)
	}
	if !result.Handled || result.ConnectionID != 8 {
		t.Fatalf("TerminateSession result = %#v", result)
	}
	if err := <-serveDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve returned error = %v, want context canceled", err)
	}
	client.mu.Lock()
	terminateCalls := client.terminateCalls
	client.mu.Unlock()
	if terminateCalls != 1 {
		t.Fatalf("compensating terminate calls = %d, want 1", terminateCalls)
	}
	select {
	case frame := <-transport.writes:
		t.Fatalf("terminated initializing connection wrote frame: %#v", frame)
	default:
	}
}

func testActiveSession(connectionID int64, client msgq.SessionClient) *activeSession {
	return &activeSession{
		header:    msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: connectionID},
		client:    client,
		cancel:    func() {},
		transport: &sessionControlTransport{},
	}
}

type sessionControlClient struct {
	mu               sync.Mutex
	initEntered      chan struct{}
	releaseInit      chan struct{}
	terminateEntered chan struct{}
	releaseTerminate chan struct{}
	terminateErr     error
	terminateCalls   int
	terminateOnce    sync.Once
}

func (client *sessionControlClient) Init(ctx context.Context, _ msgq.InitRequest) (msgq.InitResult, error) {
	if client.initEntered == nil {
		return msgq.InitResult{}, errors.New("unexpected Init")
	}
	close(client.initEntered)
	select {
	case <-ctx.Done():
		return msgq.InitResult{}, ctx.Err()
	case <-client.releaseInit:
		return msgq.InitResult{}, nil
	}
}

func (*sessionControlClient) OpenMailboxStream(context.Context, msgq.SessionHeader) (msgq.MailboxStreamSession, error) {
	return nil, errors.New("unexpected MailboxStream")
}

func (client *sessionControlClient) TerminateSession(context.Context, msgq.SessionHeader) error {
	client.mu.Lock()
	client.terminateCalls++
	entered, release, err := client.terminateEntered, client.releaseTerminate, client.terminateErr
	client.mu.Unlock()
	if entered != nil {
		client.terminateOnce.Do(func() { close(entered) })
	}
	if release != nil {
		<-release
	}
	return err
}

func (*sessionControlClient) Close() error { return nil }

type sessionControlRouter struct{ client msgq.SessionClient }

func (router sessionControlRouter) Acquire(context.Context, msgq.AcquireSessionRequest) (msgq.SessionClient, error) {
	return router.client, nil
}

type sessionControlTransport struct{}

func (*sessionControlTransport) Read(context.Context) (Frame, error) {
	return Frame{}, errors.New("unexpected Read")
}

func (*sessionControlTransport) Write(context.Context, Frame) error {
	return errors.New("unexpected Write")
}

func (*sessionControlTransport) Close() error { return nil }

type initializingSessionTransport struct {
	reads     chan Frame
	writes    chan Frame
	closed    chan struct{}
	closeOnce sync.Once
}

func newInitializingSessionTransport() *initializingSessionTransport {
	transport := &initializingSessionTransport{
		reads:  make(chan Frame, 1),
		writes: make(chan Frame, 1),
		closed: make(chan struct{}),
	}
	transport.reads <- Frame{Kind: FrameHandshake, Payload: []byte("ticket")}
	return transport
}

func (transport *initializingSessionTransport) Read(ctx context.Context) (Frame, error) {
	select {
	case frame := <-transport.reads:
		return frame, nil
	case <-transport.closed:
		return Frame{}, context.Canceled
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}

func (transport *initializingSessionTransport) Write(ctx context.Context, frame Frame) error {
	select {
	case transport.writes <- frame:
		return nil
	case <-transport.closed:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (transport *initializingSessionTransport) Close() error {
	transport.closeOnce.Do(func() { close(transport.closed) })
	return nil
}
