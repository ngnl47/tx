package msgq

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// 此测试同时约束本地部分入队结果和真实 gRPC 错误返回，防止成功邮箱被误判为全部失败。
func TestBroadcastGRPCPreservesPartialDelivery(t *testing.T) {
	for _, rolePublish := range []bool{false, true} {
		name := "topic"
		if rolePublish {
			name = "role"
		}
		t.Run(name, func(t *testing.T) { testBroadcastGRPC(t, rolePublish) })
	}
}

func testBroadcastGRPC(t *testing.T, rolePublish bool) {
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
	var loseResponse atomic.Bool
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, req)
		if info.FullMethod == msgqpb.MailboxService_PubRole_FullMethodName && loseResponse.CompareAndSwap(true, false) {
			return nil, status.Error(codes.DeadlineExceeded, "response lost after enqueue")
		}
		return response, err
	}))
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
	var result BroadcastResult
	if rolePublish {
		binding, _, findErr := mailbox.store.FindRole(ctx, 47)
		if findErr != nil {
			t.Fatal(findErr)
		}
		// 任一归属字段不匹配都不能入队；后续健康邮箱必须仍从 Seq=1 开始。
		for _, invalid := range []RoleBinding{
			{RoleID: 47, InstanceID: "msgq-b", Generation: binding.Generation},
			{RoleID: 47, InstanceID: binding.InstanceID, Generation: "old-generation"},
			{RoleID: 48, InstanceID: binding.InstanceID, Generation: binding.Generation},
		} {
			_, rejectErr := client.client.PubRole(ctx, &msgqpb.PubRoleRequest{
				RoleId: invalid.RoleID, InstanceId: invalid.InstanceID, Generation: invalid.Generation, Payload: []byte("rejected"),
			})
			if status.Code(rejectErr) != codes.Aborted || !errors.Is(mailboxClientError(rejectErr), ErrRoleBindingChanged) {
				t.Fatalf("invalid role binding = %+v, error = %v", invalid, rejectErr)
			}
		}
		result, err = client.PubRole(ctx, binding, []byte("shared"))
	} else {
		result, err = client.BroadcastTopic(ctx, BroadcastTopicRequest{Topic: "role:47", Payload: []byte("shared")})
	}
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
	if rolePublish {
		binding, _, _ := mailbox.store.FindRole(ctx, 47)
		loseResponse.Store(true)
		result, err := client.PubRole(ctx, binding, []byte("response-lost"))
		if !errors.Is(err, context.DeadlineExceeded) || result != (BroadcastResult{}) {
			t.Fatalf("unknown delivery result = %+v, %v", result, err)
		}
		message, err := mailbox.Fetch(ctx, FetchRequest{Header: second})
		if err != nil || message.Seq != 2 || string(message.Payload) != "response-lost" {
			t.Fatalf("delivery despite response loss = %+v, %v", message, err)
		}
		pending, _ := mailbox.lookup(second.SessionID)
		pending.mu.Lock()
		count := pending.downMsgs.len()
		pending.mu.Unlock()
		if count != 1 {
			t.Fatalf("ambiguous publish was replayed: pending = %d", count)
		}
	}
}
