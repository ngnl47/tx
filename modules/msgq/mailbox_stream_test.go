package msgq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
)

type mailboxReadClient struct {
	grpc.BidiStreamingClient[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]
	ctx       context.Context
	responses chan *msgqpb.MailboxStreamResponse
}

func (c *mailboxReadClient) Recv() (*msgqpb.MailboxStreamResponse, error) {
	select {
	case r := <-c.responses:
		return r, nil
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}
func testMailboxStream() *grpcMailboxStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &grpcMailboxStream{ctx: ctx, cancel: cancel, requests: make(chan *msgqpb.MailboxStreamRequest, 256), done: make(chan struct{}), sessions: make(map[int64]*grpcMailboxSession)}
}

func TestMailboxSessionTryRecvOnlyTakesQueuedMessage(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	session := &grpcMailboxSession{ctx: ctx, cancel: cancel, items: make(chan *msgqpb.MailboxStreamResponse, 1)}
	if _, ready, err := session.TryRecv(); err != nil || ready {
		t.Fatalf("empty TryRecv = %t, %v", ready, err)
	}
	session.items <- &msgqpb.MailboxStreamResponse{Message: &msgqpb.FetchResponse{Seq: 7, Payload: []byte("down")}}
	message, ready, err := session.TryRecv()
	if err != nil || !ready || message.Seq != 7 || string(message.Payload) != "down" {
		t.Fatalf("queued TryRecv = %+v, %t, %v", message, ready, err)
	}
	cancel(context.Canceled)
	if _, ready, err := session.TryRecv(); !errors.Is(err, context.Canceled) || ready {
		t.Fatalf("canceled TryRecv = %t, %v", ready, err)
	}
}

func TestMailboxSessionCancellationUnblocksRecv(t *testing.T) {
	s := testMailboxStream()
	defer s.fail(context.Canceled)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, err := s.open(ctx, SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := session.Recv(); done <- err }()
	cancel()
	returned := false
	select {
	case <-done:
		returned = true
	case <-time.After(100 * time.Millisecond):
		t.Error("取消会话后 Recv 仍阻塞")
	}
	session.Close()
	if !returned {
		<-done
	}
}
func TestMailboxSlowSessionDoesNotBlockOtherSession(t *testing.T) {
	s := testMailboxStream()
	defer s.fail(context.Canceled)
	fake := &mailboxReadClient{ctx: s.ctx, responses: make(chan *msgqpb.MailboxStreamResponse, 100)}
	s.client = fake
	a, _ := s.open(context.Background(), SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1})
	b, _ := s.open(context.Background(), SessionHeader{RoleID: 2, SessionID: 2, ConnectionID: 2})
	for i := 0; i < 65; i++ {
		fake.responses <- &msgqpb.MailboxStreamResponse{Header: headerToProto(a.(*grpcMailboxSession).header), Message: &msgqpb.FetchResponse{Seq: int64(i + 1)}}
	}
	fake.responses <- &msgqpb.MailboxStreamResponse{Header: headerToProto(b.(*grpcMailboxSession).header), Message: &msgqpb.FetchResponse{Seq: 1}}
	go s.readLoop()
	done := make(chan error, 1)
	go func() { _, err := b.Recv(); done <- err }()
	returned := false
	select {
	case <-done:
		returned = true
	case <-time.After(100 * time.Millisecond):
		t.Error("慢会话 A 阻塞了健康会话 B")
	}
	if _, err := a.Recv(); !errors.Is(err, ErrMailboxBackpressure) {
		t.Fatalf("慢会话应终止: %v", err)
	}
	if err := a.Renew(context.Background()); !errors.Is(err, ErrMailboxBackpressure) {
		t.Fatalf("失败会话仍可续期: %v", err)
	}
	a.Close()
	if !returned {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("关闭 A 后 B 仍阻塞")
		}
	}
}
func TestMailboxClosedStreamRejectsOpen(t *testing.T) {
	s := testMailboxStream()
	s.fail(errors.New("closed"))
	for i := int64(1); i <= 100; i++ {
		session, err := s.open(context.Background(), SessionHeader{RoleID: i, SessionID: i, ConnectionID: i})
		if err == nil {
			t.Errorf("已关闭的流仍接纳了会话 %d", i)
			session.(*grpcMailboxSession).finish(context.Canceled)
			return
		}
	}
}

func TestMailboxControlDeadlineAndLocalClose(t *testing.T) {
	s := testMailboxStream()
	defer s.fail(context.Canceled)
	item, err := s.open(context.Background(), SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1})
	if err != nil {
		t.Fatal(err)
	}
	for len(s.requests) < cap(s.requests) {
		s.requests <- &msgqpb.MailboxStreamRequest{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := item.Ack(ctx, 1, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ack 未遵守期限: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- item.Close() }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("本地 Close 被满控制队列阻塞")
	}
	if _, err := item.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("关闭后仍返回消息: %v", err)
	}
}

type blockedMailboxServer struct {
	grpc.BidiStreamingServer[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]
	ctx      context.Context
	requests chan *msgqpb.MailboxStreamRequest
	sending  chan struct{}
}

func (s *blockedMailboxServer) Context() context.Context { return s.ctx }
func (s *blockedMailboxServer) Recv() (*msgqpb.MailboxStreamRequest, error) {
	select {
	case req := <-s.requests:
		return req, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}
func (s *blockedMailboxServer) Send(*msgqpb.MailboxStreamResponse) error {
	close(s.sending)
	<-s.ctx.Done()
	return s.ctx.Err()
}
func TestMailboxServerProcessesAckWhileDownstreamBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 2, ConnectionID: 3}
	if _, err = mailbox.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	stream := &blockedMailboxServer{ctx: ctx, requests: make(chan *msgqpb.MailboxStreamRequest, 2), sending: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- (&mailboxService{mailbox: mailbox}).MailboxStream(stream) }()
	stream.requests <- &msgqpb.MailboxStreamRequest{Kind: msgqpb.MailboxStreamRequest_KIND_OPEN, Header: headerToProto(header), Max: 1}
	if _, err = mailbox.PubSession(ctx, header.SessionID, []byte("down")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.sending:
	case <-time.After(time.Second):
		t.Fatal("下行未开始")
	}
	stream.requests <- &msgqpb.MailboxStreamRequest{Kind: msgqpb.MailboxStreamRequest_KIND_ACK, Header: headerToProto(header), Ack: 1}
	mailbox.mu.Lock()
	session := mailbox.sessions[header.SessionID]
	mailbox.mu.Unlock()
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		session.mu.Lock()
		remaining := session.downMsgs.len()
		session.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Error("下行 Send 阻塞了 Ack 处理")
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

func TestMailboxRemoteSlowSessionCanRestoreWithoutLosingUnackedMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 128})
	if err != nil {
		t.Fatal(err)
	}
	client := startMailboxTestGRPC(t, mailbox)
	defer func() {
		if client.mailbox != nil {
			client.mailbox.fail(context.Canceled)
		}
	}()
	first := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	second := SessionHeader{RoleID: 2, SessionID: 2, ConnectionID: 1}
	for _, header := range []SessionHeader{first, second} {
		if _, err := client.Init(ctx, InitRequest{Header: header}); err != nil {
			t.Fatal(err)
		}
	}
	a, err := client.OpenMailboxStream(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	bctx, bcancel := context.WithCancel(ctx)
	defer bcancel()
	b, err := client.OpenMailboxStream(bctx, second)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for range 65 {
		if _, err := mailbox.PubSession(ctx, first.SessionID, []byte("pending")); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-a.(*grpcMailboxSession).done:
	case <-ctx.Done():
		t.Fatal("慢会话未被隔离")
	}
	if _, err := a.Recv(); !errors.Is(err, ErrMailboxBackpressure) {
		t.Fatalf("慢会话错误: %v", err)
	}
	if _, err := mailbox.PubSession(ctx, second.SessionID, []byte("healthy")); err != nil {
		t.Fatal(err)
	}
	if message, err := b.Recv(); err != nil || string(message.Payload) != "healthy" {
		t.Fatalf("健康会话: %+v %v", message, err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	first.ConnectionID++
	if _, err := client.Init(ctx, InitRequest{Header: first, Restore: true}); err != nil {
		t.Fatal(err)
	}
	restored, err := client.OpenMailboxStream(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for seq := int64(1); seq <= 65; seq++ {
		message, err := restored.Recv()
		if err != nil || message.Seq != seq {
			t.Fatalf("未确认下行丢失: seq=%d message=%+v err=%v", seq, message, err)
		}
	}
	bcancel()
	if _, err := b.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("空闲取消未退出: %v", err)
	}
	if _, err := mailbox.PubSession(ctx, first.SessionID, []byte("still alive")); err != nil {
		t.Fatal(err)
	}
	if message, err := restored.Recv(); err != nil || message.Seq != 66 {
		t.Fatalf("另一会话取消影响恢复后的会话: %+v %v", message, err)
	}
}

func TestMailboxOpenAndFailureHaveOneLifecycleBoundary(t *testing.T) {
	for range 100 {
		stream := testMailboxStream()
		ready := make(chan struct{})
		finished := make(chan struct{})
		go func() { <-ready; stream.fail(context.Canceled); close(finished) }()
		close(ready)
		session, err := stream.open(context.Background(), SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1})
		<-finished
		if err == nil {
			if _, err = session.Recv(); err == nil {
				t.Fatal("关闭后会话仍返回成功")
			}
		}
		stream.mu.Lock()
		remaining := len(stream.sessions)
		stream.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("关闭后遗留 %d 个会话", remaining)
		}
	}
}

type expiringFetchStore struct {
	testSessionStore
	calls        atomic.Int64
	fetchContext chan context.Context
}

func (*expiringFetchStore) leaseRefreshInterval() time.Duration { return time.Nanosecond }
func (store *expiringFetchStore) Refresh(ctx context.Context, _ SessionBinding) (bool, error) {
	if store.calls.Add(1) == 2 {
		store.fetchContext <- ctx
		return false, nil
	}
	return true, nil
}
func TestMailboxServerReclaimsExpiredSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := &expiringFetchStore{fetchContext: make(chan context.Context, 1)}
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	if _, err = mailbox.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	client := startMailboxTestGRPC(t, mailbox)
	stream, err := client.client.MailboxStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&msgqpb.MailboxStreamRequest{Kind: msgqpb.MailboxStreamRequest_KIND_OPEN, Header: headerToProto(header), Max: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case fetchCtx := <-store.fetchContext:
		select {
		case <-fetchCtx.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("读取者结束后订阅 Context 未回收")
		}
	case <-ctx.Done():
		t.Fatal("未触发过期读取")
	}
}
