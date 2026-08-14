package tests

import (
	"context"
	"net"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestMsgQSenderResolvesSessionOwnerAndReusesConnection(t *testing.T) {
	store := newMailboxSessionStore()
	mailbox, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      store,
		MaxBacklog: 4,
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

	ctx := context.Background()
	clientConn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial mailbox service: %v", err)
	}
	defer clientConn.Close()
	client := msgqpb.NewMailboxServiceClient(clientConn)
	header := &msgqpb.SessionHeader{RoleId: 470001, SessionId: 99, ConnectionId: 7}
	if _, err := client.Init(ctx, &msgqpb.InitRequest{Header: header}); err != nil {
		t.Fatalf("init mailbox: %v", err)
	}

	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "msgq-1",
		Services: []string{"msgq"},
		Endpoint: "bufnet",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	router, err := msgq.NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("new session router: %v", err)
	}
	grpcDialer, err := msgq.NewGRPCDialer(insecure.NewCredentials(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("new msgq gRPC dialer: %v", err)
	}
	dialer := &countingMsgQDialer{delegate: grpcDialer}
	sender, err := msgq.NewSender(router, dialer)
	if err != nil {
		t.Fatalf("new msgq sender: %v", err)
	}
	defer sender.Close()

	for _, message := range [][]byte{[]byte("first-bon"), []byte("second-bon")} {
		if _, err := sender.Pub(ctx, 470001, message); err != nil {
			t.Fatalf("publish %q: %v", message, err)
		}
	}

	batch, err := client.FetchSome(ctx, &msgqpb.FetchSomeRequest{Header: header, Max: 2})
	if err != nil {
		t.Fatalf("fetch messages: %v", err)
	}
	if len(batch.GetMessages()) != 2 || string(batch.GetMessages()[0].GetMessage()) != "first-bon" || string(batch.GetMessages()[1].GetMessage()) != "second-bon" {
		t.Fatalf("messages = %#v", batch.GetMessages())
	}
	if dialer.calls != 1 {
		t.Fatalf("dial calls = %d, want 1", dialer.calls)
	}
}

type countingMsgQDialer struct {
	delegate msgq.Dialer
	calls    int
}

func (d *countingMsgQDialer) Dial(ctx context.Context, instance discovery.Instance) (msgq.Connection, error) {
	d.calls++
	return d.delegate.Dial(ctx, instance)
}
