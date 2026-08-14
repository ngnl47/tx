package tests

import (
	"context"
	"net"
	"testing"

	"github.com/ngnl47/tx/modules/msgq"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestMsgQGRPCServicePassesMailboxLifecycle(t *testing.T) {
	mailbox, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      newMailboxSessionStore(),
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
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial mailbox service: %v", err)
	}
	defer conn.Close()

	client := msgqpb.NewMailboxServiceClient(conn)
	header := &msgqpb.SessionHeader{RoleId: 470001, SessionId: 99, ConnectionId: 7}
	if _, err := client.Init(ctx, &msgqpb.InitRequest{Header: header}); err != nil {
		t.Fatalf("init mailbox: %v", err)
	}
	if _, err := client.Pub(ctx, &msgqpb.PubRequest{
		Header:  &msgqpb.SessionHeader{RoleId: header.GetRoleId()},
		Message: []byte("bon-body"),
	}); err != nil {
		t.Fatalf("publish mailbox: %v", err)
	}
	message, err := client.Fetch(ctx, &msgqpb.FetchRequest{Header: header})
	if err != nil {
		t.Fatalf("fetch mailbox: %v", err)
	}
	if message.GetSeq() != 1 || string(message.GetMessage()) != "bon-body" {
		t.Fatalf("fetch result = %#v", message)
	}
	if _, err := client.Confirm(ctx, &msgqpb.ConfirmRequest{Header: header, Ack: message.GetSeq()}); err != nil {
		t.Fatalf("confirm mailbox: %v", err)
	}
}
