package msgq

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMailboxStreamMultiplexesSessionsAndKeepsAckIsolated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
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
	client := msgqpb.NewMailboxServiceClient(transport)
	first := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	second := SessionHeader{RoleID: 48, SessionID: 12, ConnectionID: 1}
	for _, header := range []SessionHeader{first, second} {
		if _, err := mailbox.Init(ctx, InitRequest{Header: header}); err != nil {
			t.Fatal(err)
		}
	}
	streamCtx, stopStream := context.WithCancel(ctx)
	stream, err := client.MailboxStream(streamCtx)
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []SessionHeader{first, second} {
		if err := stream.Send(&msgqpb.MailboxStreamRequest{Header: headerToProto(header), Kind: msgqpb.MailboxStreamRequest_KIND_OPEN, Max: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := mailbox.PubSession(ctx, first.SessionID, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.PubSession(ctx, first.SessionID, []byte("one-more")); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.PubSession(ctx, second.SessionID, []byte("two")); err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64][]string)
	for range 3 {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("receive mailbox item: %v", err)
		}
		sessionID := response.GetHeader().GetSessionId()
		seen[sessionID] = append(seen[sessionID], string(response.GetMessage().GetPayload()))
	}
	if len(seen[first.SessionID]) != 2 || seen[first.SessionID][0] != "one" || seen[first.SessionID][1] != "one-more" || len(seen[second.SessionID]) != 1 || seen[second.SessionID][0] != "two" {
		t.Fatalf("shared stream mixed Sessions: %v", seen)
	}
	if err := stream.Send(&msgqpb.MailboxStreamRequest{Header: headerToProto(first), Kind: msgqpb.MailboxStreamRequest_KIND_ACK, Ack: 1, ReceivedSeq: 17}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		session, found := mailbox.lookup(first.SessionID)
		if found {
			session.mu.Lock()
			confirmed := session.downMsgs.confirmedSeq
			accepted := session.upMsgSeq
			session.mu.Unlock()
			if confirmed == 1 && accepted == 17 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("第一会话 Ack 未由共享流处理")
		}
		time.Sleep(time.Millisecond)
	}
	defer stopStream()
	oldFirst := first
	first.ConnectionID = 2
	second.ConnectionID = 2
	if restored, err := mailbox.Init(ctx, InitRequest{Header: first, Restore: true, Ack: 1}); err != nil || restored.UpMsgSeq != 17 {
		t.Fatalf("restored up message cursor = %+v, %v; want 17", restored, err)
	}
	if _, err := mailbox.Init(ctx, InitRequest{Header: second, Restore: true, Ack: 0}); err != nil {
		t.Fatal(err)
	}
	// 接管后旧连接不能推进游标、裁剪下行或终止新连接。
	if _, err := mailbox.Rec(ctx, RecRequest{Header: oldFirst, Seq: 100, Ack: 2}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("旧连接 Rec: %v", err)
	}
	if err := mailbox.TerminateSession(ctx, oldFirst); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("旧连接 Terminate: %v", err)
	}
	retired := make(map[int64]bool)
	for range 2 {
		response, err := stream.Recv()
		if err != nil || response.GetErrorMessage() == "" || response.GetHeader().GetConnectionId() != 1 {
			t.Fatalf("旧订阅应收到接管错误: %+v, %v", response, err)
		}
		retired[response.GetHeader().GetSessionId()] = true
	}
	if !retired[first.SessionID] || !retired[second.SessionID] {
		t.Fatalf("未隔离全部旧订阅: %v", retired)
	}
	firstPending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: first, Max: 1})
	if err != nil || len(firstPending.Messages) != 1 || firstPending.Messages[0].Seq != 2 {
		t.Fatalf("第一会话确认后待恢复消息 = %+v, %v", firstPending, err)
	}
	secondPending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: second, Max: 1})
	if err != nil || len(secondPending.Messages) != 1 || secondPending.Messages[0].Seq != 1 {
		t.Fatalf("第二会话未确认消息 = %+v, %v", secondPending, err)
	}
}

func TestPublishStreamProcessesSessionAndRoleMessagesInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &testSessionStore{}
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 8})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if _, err := mailbox.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	peer := SessionHeader{RoleID: 47, SessionID: 12, ConnectionID: 2}
	if _, err := mailbox.Init(ctx, InitRequest{Header: peer}); err != nil {
		t.Fatal(err)
	}
	binding, found, err := store.FindRole(ctx, header.RoleID)
	if err != nil || !found {
		t.Fatalf("FindRole = %+v, %v, found=%v", binding, err, found)
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
	publisher, err := newGRPCPublishStream(msgqpb.NewMailboxServiceClient(transport), nil)
	if err != nil {
		t.Fatal(err)
	}
	requests := []*msgqpb.PublishRequest{
		{Kind: msgqpb.PublishRequest_KIND_SESSION, SessionId: header.SessionID, Payload: []byte("first")},
		{Kind: msgqpb.PublishRequest_KIND_ROLE, RoleId: binding.RoleID, InstanceId: binding.InstanceID, Generation: binding.Generation, Payload: []byte("role")},
		{Kind: msgqpb.PublishRequest_KIND_SESSION, SessionId: header.SessionID, Payload: []byte("last")},
	}
	for _, request := range requests {
		if err := publisher.send(ctx, request); err != nil {
			t.Fatalf("send publish request: %v", err)
		}
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("排空发布队列: %v", err)
	}
	pending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: header, Max: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Messages) != 3 {
		t.Fatalf("published message count = %d, want 3", len(pending.Messages))
	}
	for index, want := range []string{"first", "role", "last"} {
		if got := string(pending.Messages[index].Payload); got != want {
			t.Fatalf("message[%d] = %q, want %q", index, got, want)
		}
	}
	peerPending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: peer, Max: 1})
	if err != nil || len(peerPending.Messages) != 1 || string(peerPending.Messages[0].Payload) != "role" {
		t.Fatalf("角色消息没有扇出到同角色的第二邮箱: %+v, %v", peerPending, err)
	}
}

func TestPublishStreamRoleRejectionReaddressesOnlyRejectedPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &testSessionStore{}
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 8})
	if err != nil {
		t.Fatal(err)
	}
	first := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if _, err := mailbox.Init(ctx, InitRequest{Header: first}); err != nil {
		t.Fatal(err)
	}
	oldBinding, found, err := store.FindRole(ctx, first.RoleID)
	if err != nil || !found {
		t.Fatalf("old role binding = %+v, found=%v, err=%v", oldBinding, found, err)
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
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID: "msgq-a", Endpoint: listener.Addr().String(), Services: []string{"msgq"}, Status: discovery.StatusRunning,
	}}); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewSessionRouter(directory, store, 8)
	if err != nil {
		t.Fatal(err)
	}
	roles, err := NewRoleRouter(directory, store.FindRole, 8)
	if err != nil {
		t.Fatal(err)
	}
	dialer, err := NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(sessions, roles, dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	if err := sender.SendRole(ctx, first.RoleID, []byte("warm")); err != nil {
		t.Fatal(err)
	}
	for {
		pending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: first, Max: 1})
		if err == nil && len(pending.Messages) == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("等待首条异步角色消息: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := mailbox.TerminateSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := mailbox.maintainRole(ctx, first.RoleID, time.Now()); err != nil {
		t.Fatal(err)
	}
	second := SessionHeader{RoleID: first.RoleID, SessionID: 12, ConnectionID: 2}
	if _, err := mailbox.Init(ctx, InitRequest{Header: second}); err != nil {
		t.Fatal(err)
	}
	newBinding, found, err := store.FindRole(ctx, second.RoleID)
	if err != nil || !found || newBinding == oldBinding {
		t.Fatalf("new role binding = %+v, found=%v, err=%v", newBinding, found, err)
	}

	if err := sender.SendRole(ctx, second.RoleID, []byte("stale")); err != nil {
		t.Fatal(err)
	}
	for {
		pending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: second, Max: 2})
		if err == nil && len(pending.Messages) == 1 && pending.Messages[0].Seq == 1 && string(pending.Messages[0].Payload) == "stale" {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("旧归属明确拒绝后的消息未到新邮箱: %+v, %v", pending, err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := sender.SendRole(ctx, second.RoleID, []byte("next")); err != nil {
		t.Fatal(err)
	}
	for {
		pending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: second, Max: 2})
		if err == nil && len(pending.Messages) == 1 && pending.Messages[0].Seq == 2 && string(pending.Messages[0].Payload) == "next" {
			break
		}
		if err == nil && len(pending.Messages) != 0 {
			t.Fatalf("new session mailbox = %+v; want next", pending)
		}
		if ctx.Err() != nil {
			t.Fatalf("等待新的异步角色消息: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

// 历史断流只记录日志，不阻断最终关闭；后续发布可新建流，但不重投旧消息。
func TestPublishStreamHistoricalDisconnectDoesNotBlockClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if _, err := mailbox.Init(ctx, InitRequest{Header: header}); err != nil {
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
	defer server.Stop()
	transport, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	connection := &grpcConnection{client: msgqpb.NewMailboxServiceClient(transport)}
	if err := connection.SendSession(ctx, header.SessionID, []byte("before")); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.Fetch(ctx, FetchRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	previous := connection.publisher
	server.Stop()
	select {
	case <-previous.done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !errors.Is(previous.closeErr, ErrRouteUnavailable) {
		t.Fatalf("未记录历史断流: %v", previous.closeErr)
	}
	for range 2 {
		if err := previous.Close(); err != nil {
			t.Fatalf("历史断流不能阻断关闭: %v", err)
		}
	}
	// 同址恢复服务，模拟底层连接重建；真实实例身份轮换由连接缓存另外覆盖。
	listener, err = net.Listen("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	replacement := grpc.NewServer()
	if err := RegisterMailboxService(replacement, mailbox); err != nil {
		t.Fatal(err)
	}
	go func() { _ = replacement.Serve(listener) }()
	defer replacement.Stop()
	transport.ResetConnectBackoff()
	if err := connection.SendSession(ctx, header.SessionID, []byte("after")); err != nil {
		t.Fatal(err)
	}
	if connection.publisher == previous {
		t.Fatal("新发布仍复用已断开的流")
	}
	if err := connection.closePublisher(); err != nil {
		t.Fatal(err)
	}
	pending, err := mailbox.FetchSome(ctx, FetchSomeRequest{Header: header, Max: 2})
	if err != nil || len(pending.Messages) != 1 || string(pending.Messages[0].Payload) != "after" {
		t.Fatalf("新发布结果: %+v, %v", pending, err)
	}
}
