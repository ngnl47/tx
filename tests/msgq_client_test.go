package tests

import (
	"context"
	"net"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestSessionClientRouterPrefersExistingOwner(t *testing.T) {
	store := newMailboxSessionStore()
	_, _, err := store.Bind(context.Background(), msgq.SessionBinding{
		RoleID:       470001,
		SessionID:    99,
		ConnectionID: 7,
		InstanceID:   "msgq-b",
	})
	if err != nil {
		t.Fatalf("bind owner: %v", err)
	}
	directory := sessionClientDirectory(t)
	dialer := &sessionClientDialerFake{client: &sessionClientFake{}}
	router, err := msgq.NewSessionClientRouter(directory, store, dialer)
	if err != nil {
		t.Fatalf("new client router: %v", err)
	}

	_, err = router.Acquire(context.Background(), 470001)
	if err != nil {
		t.Fatalf("acquire existing owner: %v", err)
	}
	if len(dialer.instances) != 1 || dialer.instances[0].ID != "msgq-b" {
		t.Fatalf("dialed instances = %#v, want only msgq-b", dialer.instances)
	}
}

func TestSessionClientRouterReusesEndpointClientAndClosesIt(t *testing.T) {
	store := newMailboxSessionStore()
	_, _, err := store.Bind(context.Background(), msgq.SessionBinding{
		RoleID:       470001,
		SessionID:    99,
		ConnectionID: 7,
		InstanceID:   "msgq-b",
	})
	if err != nil {
		t.Fatalf("bind owner: %v", err)
	}
	client := &sessionClientFake{}
	dialer := &sessionClientDialerFake{client: client}
	router, err := msgq.NewSessionClientRouter(sessionClientDirectory(t), store, dialer)
	if err != nil {
		t.Fatalf("new client router: %v", err)
	}
	for range 2 {
		if _, err := router.Acquire(context.Background(), 470001); err != nil {
			t.Fatalf("acquire existing owner: %v", err)
		}
	}
	if len(dialer.instances) != 1 {
		t.Fatalf("dial calls = %d, want 1", len(dialer.instances))
	}
	if err := router.Close(); err != nil {
		t.Fatalf("close client router: %v", err)
	}
	if client.closeCalls != 1 {
		t.Fatalf("client close calls = %d, want 1", client.closeCalls)
	}
}

func TestSessionClientRouterSelectsRunningServiceWhenNoOwner(t *testing.T) {
	directory := sessionClientDirectory(t)
	dialer := &sessionClientDialerFake{client: &sessionClientFake{}}
	router, err := msgq.NewSessionClientRouter(directory, newMailboxSessionStore(), dialer)
	if err != nil {
		t.Fatalf("new client router: %v", err)
	}

	for _, want := range []string{"msgq-a", "msgq-b"} {
		_, err := router.Acquire(context.Background(), 470001)
		if err != nil {
			t.Fatalf("acquire running service: %v", err)
		}
		if got := dialer.instances[len(dialer.instances)-1].ID; got != want {
			t.Fatalf("dialed instance = %q, want %q", got, want)
		}
	}
}

func TestLocalSessionClientDelegatesMailboxCalls(t *testing.T) {
	mailbox, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-local",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new mailbox: %v", err)
	}
	client, err := msgq.NewLocalSessionClient(mailbox)
	if err != nil {
		t.Fatalf("new local client: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := client.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := client.Rec(context.Background(), msgq.RecRequest{Header: header, Seq: 1}); err != nil {
		t.Fatalf("record inbound message: %v", err)
	}
	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: header.RoleID}, Payload: []byte("bon")}); err != nil {
		t.Fatalf("publish outbound message: %v", err)
	}
	batch, err := client.FetchSome(context.Background(), msgq.FetchSomeRequest{Header: header, Max: 1})
	if err != nil {
		t.Fatalf("fetch outbound message: %v", err)
	}
	if len(batch.Messages) != 1 || batch.Messages[0].Seq != 1 || string(batch.Messages[0].Payload) != "bon" {
		t.Fatalf("fetched batch = %#v", batch)
	}
	if err := client.Confirm(context.Background(), msgq.ConfirmRequest{Header: header, Ack: 1}); err != nil {
		t.Fatalf("confirm outbound message: %v", err)
	}
}

func TestFixedSessionClientRouterAlwaysReturnsSharedLocalClient(t *testing.T) {
	client := &sessionClientFake{}
	router, err := msgq.NewFixedSessionClientRouter(client)
	if err != nil {
		t.Fatalf("new fixed session client router: %v", err)
	}
	for range 2 {
		got, err := router.Acquire(context.Background(), 470001)
		if err != nil {
			t.Fatalf("acquire fixed session client: %v", err)
		}
		if got != client {
			t.Fatalf("client = %T %p, want shared %p", got, got, client)
		}
	}
	if err := router.Close(); err != nil {
		t.Fatalf("close fixed session client router: %v", err)
	}
	if client.closeCalls != 0 {
		t.Fatalf("fixed router closed shared client %d times, want 0", client.closeCalls)
	}
}

func TestGRPCSessionClientDelegatesMailboxCalls(t *testing.T) {
	mailbox, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-remote",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new mailbox: %v", err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()
	if err := msgq.RegisterMailboxService(server, mailbox); err != nil {
		t.Fatalf("register mailbox service: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	dialer, err := msgq.NewGRPCDialer(insecure.NewCredentials(), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	if err != nil {
		t.Fatalf("new gRPC dialer: %v", err)
	}
	client, err := dialer.DialSession(context.Background(), discovery.Instance{ID: "msgq-remote", Services: []string{"msgq"}, Endpoint: "bufnet", Status: discovery.StatusRunning})
	if err != nil {
		t.Fatalf("dial session client: %v", err)
	}
	defer client.Close()
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := client.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := client.Rec(context.Background(), msgq.RecRequest{Header: header, Seq: 1}); err != nil {
		t.Fatalf("record inbound message: %v", err)
	}
	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: header.RoleID}, Payload: []byte("bon")}); err != nil {
		t.Fatalf("publish outbound message: %v", err)
	}
	batch, err := client.FetchSome(context.Background(), msgq.FetchSomeRequest{Header: header, Max: 1})
	if err != nil {
		t.Fatalf("fetch outbound message: %v", err)
	}
	if len(batch.Messages) != 1 || batch.Messages[0].Seq != 1 || string(batch.Messages[0].Payload) != "bon" {
		t.Fatalf("fetched batch = %#v", batch)
	}
	if err := client.Confirm(context.Background(), msgq.ConfirmRequest{Header: header, Ack: 1}); err != nil {
		t.Fatalf("confirm outbound message: %v", err)
	}
}

func sessionClientDirectory(t *testing.T) *discovery.ServiceDirectory {
	t.Helper()
	directory := discovery.NewServiceDirectory()
	err := directory.Replace([]discovery.Instance{
		{ID: "msgq-a", Services: []string{"msgq"}, Endpoint: "127.0.0.1:9101", Status: discovery.StatusRunning},
		{ID: "msgq-b", Services: []string{"msgq"}, Endpoint: "127.0.0.1:9102", Status: discovery.StatusRunning},
	})
	if err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	return directory
}

type sessionClientDialerFake struct {
	instances []discovery.Instance
	client    msgq.SessionClient
}

func (d *sessionClientDialerFake) DialSession(_ context.Context, instance discovery.Instance) (msgq.SessionClient, error) {
	d.instances = append(d.instances, instance)
	return d.client, nil
}

type sessionClientFake struct {
	closeCalls int
}

func (*sessionClientFake) Init(context.Context, msgq.InitRequest) (msgq.InitResult, error) {
	return msgq.InitResult{}, nil
}

func (*sessionClientFake) Rec(context.Context, msgq.RecRequest) (msgq.RecResult, error) {
	return msgq.RecResult{}, nil
}

func (*sessionClientFake) FetchSome(context.Context, msgq.FetchSomeRequest) (msgq.FetchSomeResult, error) {
	return msgq.FetchSomeResult{}, nil
}

func (*sessionClientFake) Confirm(context.Context, msgq.ConfirmRequest) error {
	return nil
}

func (*sessionClientFake) CloseSession(context.Context, msgq.SessionHeader) error {
	return nil
}

func (c *sessionClientFake) Close() error {
	c.closeCalls++
	return nil
}
