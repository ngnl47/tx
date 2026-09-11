package tests

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestMsgQSenderPublishesExactAndSharedRoleWithReusedConnection(t *testing.T) {
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
	second := &msgqpb.SessionHeader{RoleId: 470001, SessionId: 100, ConnectionId: 8}
	if _, err := client.Init(ctx, &msgqpb.InitRequest{Header: second}); err != nil {
		t.Fatal(err)
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
	router, err := msgq.NewSessionRouter(directory, store, 128)
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
	roles, err := msgq.NewRoleRouter(directory, store.FindRole, 16)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := msgq.NewSender(router, roles, dialer)
	if err != nil {
		t.Fatalf("new msgq sender: %v", err)
	}
	defer sender.Close()

	if _, err := sender.PubSession(ctx, 99, []byte("first-bon")); err != nil {
		t.Fatalf("publish exact session: %v", err)
	}
	if _, err := sender.PubRole(ctx, 470001, []byte("second-bon")); err != nil {
		t.Fatalf("publish role: %v", err)
	}

	batch, err := client.FetchSome(ctx, &msgqpb.FetchSomeRequest{Header: header, Max: 2})
	if err != nil {
		t.Fatalf("fetch messages: %v", err)
	}
	if len(batch.GetMessages()) != 2 || string(batch.GetMessages()[0].GetPayload()) != "first-bon" || string(batch.GetMessages()[1].GetPayload()) != "second-bon" {
		t.Fatalf("messages = %#v", batch.GetMessages())
	}
	if dialer.calls != 1 {
		t.Fatalf("dial calls = %d, want 1", dialer.calls)
	}
	fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	shared, err := client.FetchSome(fetchCtx, &msgqpb.FetchSomeRequest{Header: second, Max: 2})
	if err != nil || len(shared.GetMessages()) != 1 || string(shared.GetMessages()[0].GetPayload()) != "second-bon" || shared.GetMessages()[0].GetSeq() != 1 {
		t.Fatalf("second session shared role delivery = %v, %v", shared, err)
	}
	// 共享 Role 的两个 Session 各自从上行 Seq=1 开始。
	for _, current := range []*msgqpb.SessionHeader{header, second} {
		result, err := client.Rec(fetchCtx, &msgqpb.RecRequest{Header: current, Seq: 1})
		if err != nil || result.GetReceivedSeq() != 1 {
			t.Fatalf("session %d Rec = %v, %v", current.SessionId, result, err)
		}
	}
	restored := &msgqpb.SessionHeader{RoleId: header.RoleId, SessionId: header.SessionId, ConnectionId: 9}
	state, err := client.Init(fetchCtx, &msgqpb.InitRequest{Header: restored, Restore: true, Ack: 1})
	if err != nil || state.GetReceivedSeq() != 1 {
		t.Fatalf("restore = %v, %v", state, err)
	}
	if _, err := client.Confirm(fetchCtx, &msgqpb.ConfirmRequest{Header: header, Ack: 2}); status.Code(err) != codes.Aborted {
		t.Fatalf("old Ack = %v, want Aborted", err)
	}
	if _, err := client.Rec(fetchCtx, &msgqpb.RecRequest{Header: header, Seq: 2}); status.Code(err) != codes.Aborted {
		t.Fatalf("old Rec = %v, want Aborted", err)
	}
	if _, err := client.TerminateSession(fetchCtx, &msgqpb.TerminateSessionRequest{Header: header}); status.Code(err) != codes.Aborted {
		t.Fatalf("old terminate = %v, want Aborted", err)
	}
	replay, err := client.FetchSome(fetchCtx, &msgqpb.FetchSomeRequest{Header: restored, Max: 2})
	if err != nil || len(replay.GetMessages()) != 1 || replay.Messages[0].Seq != 2 || string(replay.Messages[0].Payload) != "second-bon" {
		t.Fatalf("restored replay = %v, %v", replay, err)
	}
	if _, err := client.Confirm(fetchCtx, &msgqpb.ConfirmRequest{Header: restored, Ack: 2}); err != nil {
		t.Fatal(err)
	}
	// 第一会话确认完毕，第二会话重连仍重放自己的未确认 Seq=1。
	secondRestored := &msgqpb.SessionHeader{RoleId: second.RoleId, SessionId: second.SessionId, ConnectionId: 10}
	if _, err := client.Init(fetchCtx, &msgqpb.InitRequest{Header: secondRestored, Restore: true}); err != nil {
		t.Fatal(err)
	}
	replay, err = client.FetchSome(fetchCtx, &msgqpb.FetchSomeRequest{Header: secondRestored, Max: 2})
	if err != nil || len(replay.GetMessages()) != 1 || replay.Messages[0].Seq != 1 || string(replay.Messages[0].Payload) != "second-bon" {
		t.Fatalf("other session replay = %v, %v", replay, err)
	}
	result, err := client.Rec(fetchCtx, &msgqpb.RecRequest{Header: restored, Seq: 2, Ack: 2})
	if err != nil || result.GetReceivedSeq() != 2 {
		t.Fatalf("new connection Rec = %v, %v", result, err)
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
