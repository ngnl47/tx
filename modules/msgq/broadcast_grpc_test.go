package msgq

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 此测试同时约束本地部分入队结果和真实 gRPC 错误返回，防止成功邮箱被误判为全部失败。
func TestBroadcastTopicGRPCPreservesPartialDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	second := SessionHeader{RoleID: 47, SessionID: 12, ConnectionID: 2}
	for _, header := range []SessionHeader{first, second} {
		if _, err := mailbox.Init(ctx, InitRequest{Header: header}); err != nil {
			t.Fatal(err)
		}
		if err := mailbox.JoinTopics(ctx, TopicRequest{Header: header, Topics: []string{"role:47"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := mailbox.PubSession(ctx, first.SessionID, []byte("old")); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	if err := RegisterMailboxService(server, mailbox); err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	transport, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	client := &grpcConnection{client: msgqpb.NewMailboxServiceClient(transport)}
	result, err := client.BroadcastTopic(ctx, BroadcastTopicRequest{Topic: "role:47", Payload: []byte("shared")})
	if !errors.Is(err, ErrDownMsgOverflow) {
		t.Fatalf("broadcast error = %v", err)
	}
	if result != (BroadcastResult{Attempted: 2, Delivered: 1, Overflowed: 1}) {
		t.Fatalf("partial result = %+v", result)
	}
	message, err := mailbox.Fetch(ctx, FetchRequest{Header: second})
	if err != nil || message.Seq != 1 || string(message.Payload) != "shared" {
		t.Fatalf("healthy mailbox = %+v, %v", message, err)
	}
	if err := mailbox.Confirm(ctx, ConfirmRequest{Header: second, Ack: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.Fetch(ctx, FetchRequest{Header: first}); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("overflow mailbox = %v", err)
	}
}
