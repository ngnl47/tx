package tests

import (
	"context"
	"net"
	"slices"
	"testing"

	"github.com/ngnl47/tx/modules/msgq"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestMsgQGRPCServicePassesMailboxLifecycle(t *testing.T) {
	store := newMailboxSessionStore()
	mailbox, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              store,
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
	restoredHeader := &msgqpb.SessionHeader{RoleId: header.GetRoleId(), SessionId: header.GetSessionId(), ConnectionId: 8}
	if _, err := client.Init(ctx, &msgqpb.InitRequest{Header: restoredHeader, Restore: true}); err != nil {
		t.Fatalf("restore mailbox: %v", err)
	}
	if _, err := client.PubSession(ctx, &msgqpb.PubSessionRequest{
		SessionId: restoredHeader.GetSessionId(),
		Payload:   []byte("bon-body"),
	}); err != nil {
		t.Fatalf("publish mailbox: %v", err)
	}
	message, err := client.Fetch(ctx, &msgqpb.FetchRequest{Header: restoredHeader})
	if err != nil {
		t.Fatalf("fetch mailbox: %v", err)
	}
	if message.GetSeq() != 1 || string(message.GetPayload()) != "bon-body" {
		t.Fatalf("fetch result = %#v", message)
	}
	if _, err := client.Confirm(ctx, &msgqpb.ConfirmRequest{Header: restoredHeader, Ack: message.GetSeq()}); err != nil {
		t.Fatalf("confirm mailbox: %v", err)
	}
}

func TestMsgQGRPCDescriptorExposesOnlyExplicitPublishRPC(t *testing.T) {
	methods := make([]string, len(msgqpb.MailboxService_ServiceDesc.Methods))
	for index, method := range msgqpb.MailboxService_ServiceDesc.Methods {
		methods[index] = method.MethodName
	}
	if !slices.Contains(methods, "PubSession") {
		t.Fatalf("service methods = %#v, missing PubSession", methods)
	}
	if slices.Contains(methods, "Pub") {
		t.Fatalf("service methods = %#v, legacy Pub remains", methods)
	}
	requestFields := (&msgqpb.PubSessionRequest{}).ProtoReflect().Descriptor().Fields()
	if requestFields.Len() != 2 || requestFields.ByNumber(1).Name() != "session_id" || requestFields.ByNumber(2).Name() != "payload" {
		t.Fatalf("PubSession request fields = %v", requestFields)
	}
}
