package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

func TestMsgQServerRestoresUnconfirmedOutboundMessages(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      store,
		MaxBacklog: 4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer server.Close(context.Background(), msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8})

	first := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	init, err := server.Init(context.Background(), msgq.InitRequest{Header: first})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if init.ReceivedSeq != 0 {
		t.Fatalf("received seq = %d, want 0", init.ReceivedSeq)
	}
	if _, err := server.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Message: []byte("first")}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := server.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Message: []byte("second")}); err != nil {
		t.Fatalf("publish second: %v", err)
	}

	firstMessage, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: first})
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if firstMessage.Seq != 1 || string(firstMessage.Message) != "first" {
		t.Fatalf("first fetch = %#v", firstMessage)
	}

	secondConn := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{
		Header:  secondConn,
		Restore: true,
		Ack:     1,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	secondMessage, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: secondConn})
	if err != nil {
		t.Fatalf("fetch restored: %v", err)
	}
	if secondMessage.Seq != 2 || string(secondMessage.Message) != "second" {
		t.Fatalf("restored fetch = %#v", secondMessage)
	}
	if err := server.Confirm(context.Background(), msgq.ConfirmRequest{Header: secondConn, Ack: 2}); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = server.Fetch(ctx, msgq.FetchRequest{Header: secondConn})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty fetch error = %v, want deadline exceeded", err)
	}
}

func TestMsgQServerSerializesInboundAndRejectsStaleConnection(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      newMailboxSessionStore(),
		MaxBacklog: 2,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	current := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: current}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer server.Close(context.Background(), current)

	received, err := server.Rec(context.Background(), msgq.RecRequest{Header: current, Seq: 1})
	if err != nil || received.ReceivedSeq != 1 {
		t.Fatalf("rec = %#v, %v", received, err)
	}
	if _, err := server.Rec(context.Background(), msgq.RecRequest{Header: current, Seq: 3}); !errors.Is(err, msgq.ErrInboundSequence) {
		t.Fatalf("over seq error = %v, want inbound sequence", err)
	}
	if _, err := server.Rec(context.Background(), msgq.RecRequest{
		Header: msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7},
		Seq:    2,
	}); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("stale rec error = %v, want session changed", err)
	}
}

func TestMsgQServerBoundsOutboundQueue(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      newMailboxSessionStore(),
		MaxBacklog: 1,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Message: []byte("one")}); err != nil {
		t.Fatalf("publish one: %v", err)
	}
	if _, err := server.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Message: []byte("two")}); !errors.Is(err, msgq.ErrOutboundOverflow) {
		t.Fatalf("publish overflow error = %v, want outbound overflow", err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after overflow = %d, want 0", server.SessionCount())
	}
}

func TestMsgQServerBoundsOutboundQueueBytes(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:      "msgq-1",
		Store:           newMailboxSessionStore(),
		MaxBacklog:      4,
		MaxBacklogBytes: 3,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: header.RoleID}, Message: []byte("123")}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := server.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: header.RoleID}, Message: []byte("4")}); !errors.Is(err, msgq.ErrOutboundOverflow) {
		t.Fatalf("publish byte overflow error = %v, want outbound overflow", err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after byte overflow = %d, want 0", server.SessionCount())
	}
}

func TestMsgQServerRejectsAcknowledgementBeyondSentOutbound(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-1", Store: newMailboxSessionStore(), MaxBacklog: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: header.RoleID}, Message: []byte("one")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := server.Confirm(context.Background(), msgq.ConfirmRequest{Header: header, Ack: 1}); !errors.Is(err, msgq.ErrOutboundAcknowledgement) {
		t.Fatalf("confirm before fetch error = %v, want invalid acknowledgement", err)
	}
	message, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: header})
	if err != nil || message.Seq != 1 {
		t.Fatalf("fetch = %#v, %v", message, err)
	}
	if err := server.Confirm(context.Background(), msgq.ConfirmRequest{Header: header, Ack: 1}); err != nil {
		t.Fatalf("confirm sent message: %v", err)
	}
}

func TestMsgQServerFetchSomeKeepsUnconfirmedSequence(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      newMailboxSessionStore(),
		MaxBacklog: 4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer server.Close(context.Background(), header)
	for _, message := range []string{"one", "two", "three"} {
		if _, err := server.Pub(context.Background(), msgq.PubRequest{
			Header:  msgq.SessionHeader{RoleID: header.RoleID},
			Message: []byte(message),
		}); err != nil {
			t.Fatalf("publish %q: %v", message, err)
		}
	}

	batch, err := server.FetchSome(context.Background(), msgq.FetchSomeRequest{Header: header, Max: 2})
	if err != nil {
		t.Fatalf("fetch some: %v", err)
	}
	if len(batch.Messages) != 2 || batch.Messages[0].Seq != 1 || batch.Messages[1].Seq != 2 || string(batch.Messages[1].Message) != "two" {
		t.Fatalf("batch = %#v", batch.Messages)
	}
	last, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: header})
	if err != nil || last.Seq != 3 || string(last.Message) != "three" {
		t.Fatalf("last = %#v, %v", last, err)
	}
}

func TestMsgQServerBroadcastTopicReportsPartialOverflow(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      newMailboxSessionStore(),
		MaxBacklog: 1,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	second := msgq.SessionHeader{RoleID: 470002, SessionID: 100, ConnectionID: 8}
	for _, header := range []msgq.SessionHeader{first, second} {
		if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
			t.Fatalf("init %#v: %v", header, err)
		}
		defer server.Close(context.Background(), header)
		if err := server.JoinTopics(context.Background(), msgq.TopicRequest{Header: header, Topics: []string{"guild:9"}}); err != nil {
			t.Fatalf("join topic: %v", err)
		}
	}

	firstBroadcast, err := server.BroadcastTopic(context.Background(), msgq.BroadcastTopicRequest{
		Topic:   "guild:9",
		Message: []byte("first"),
	})
	if err != nil || firstBroadcast.Attempted != 2 || firstBroadcast.Delivered != 2 || firstBroadcast.Overflowed != 0 {
		t.Fatalf("first broadcast = %#v, %v", firstBroadcast, err)
	}
	secondBroadcast, err := server.BroadcastTopic(context.Background(), msgq.BroadcastTopicRequest{
		Topic:   "guild:9",
		Message: []byte("second"),
	})
	if !errors.Is(err, msgq.ErrOutboundOverflow) || secondBroadcast.Attempted != 2 || secondBroadcast.Delivered != 0 || secondBroadcast.Overflowed != 2 {
		t.Fatalf("second broadcast = %#v, %v", secondBroadcast, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after topic overflow = %d, want 0", server.SessionCount())
	}
}

func TestMsgQServerRejectsOldConnectionCloseAfterRestore(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      store,
		MaxBacklog: 2,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	newHeader := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: oldHeader}); err != nil {
		t.Fatalf("init old connection: %v", err)
	}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: newHeader, Restore: true}); err != nil {
		t.Fatalf("restore new connection: %v", err)
	}
	if err := server.Close(context.Background(), oldHeader); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("old close error = %v, want session changed", err)
	}
	if server.SessionCount() != 1 {
		t.Fatalf("session count = %d, want 1", server.SessionCount())
	}
	binding, found, err := store.Find(context.Background(), 470001)
	if err != nil || !found || binding.SessionID != 99 || binding.ConnectionID != 8 || binding.InstanceID != "msgq-1" {
		t.Fatalf("binding after old close = %#v, found = %t, err = %v", binding, found, err)
	}
	if err := server.Close(context.Background(), newHeader); err != nil {
		t.Fatalf("close new connection: %v", err)
	}
}

func TestMsgQServerRedisIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run msgq mailbox Redis integration test")
	}
	ctx := context.Background()
	raw := redis.NewClient(&redis.Options{Addr: endpoint})
	defer raw.Close()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}
	prefix := fmt.Sprintf("tx:test:msgq-mailbox:%d", time.Now().UnixNano())
	key := prefix + ":session:{470001}"
	defer raw.Del(ctx, key)

	bindings, err := msgq.NewRedisSessionStore(goredis.NewClient(raw), msgq.SessionRedisOptions{
		Prefix: prefix,
		TTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new redis session store: %v", err)
	}
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID: "msgq-1",
		Store:      bindings,
		MaxBacklog: 2,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(ctx, msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, found, err := bindings.Find(ctx, header.RoleID); err != nil || !found {
		t.Fatalf("find owner after init = %t, %v", found, err)
	}
	if _, err := server.Pub(ctx, msgq.PubRequest{Header: msgq.SessionHeader{RoleID: header.RoleID}, Message: []byte("notify")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	message, err := server.Fetch(ctx, msgq.FetchRequest{Header: header})
	if err != nil || message.Seq != 1 || string(message.Message) != "notify" {
		t.Fatalf("fetch = %#v, %v", message, err)
	}
	if err := server.Confirm(ctx, msgq.ConfirmRequest{Header: header, Ack: message.Seq}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if removed, err := server.Reap(ctx, time.Now().Add(2*time.Minute)); err != nil || removed != 1 {
		t.Fatalf("reap = %d, %v", removed, err)
	}
	if _, found, err := bindings.Find(ctx, header.RoleID); err != nil || found {
		t.Fatalf("find owner after reap = %t, %v", found, err)
	}
}

func TestMsgQServerReapRemovesOfflineSessionAndBinding(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:     "msgq-1",
		Store:          store,
		MaxBacklog:     2,
		OfflineTimeout: time.Second,
		SweepInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	removed, err := server.Reap(context.Background(), time.Now().Add(2*time.Second))
	if err != nil || removed != 1 {
		t.Fatalf("reap = %d, %v", removed, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
	if _, found, err := store.Find(context.Background(), header.RoleID); err != nil || found {
		t.Fatalf("binding after reap = found %t, err %v", found, err)
	}
}

func TestMsgQServerStartUsesLifecycleWorker(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:     "msgq-1",
		Store:          newMailboxSessionStore(),
		MaxBacklog:     2,
		OfflineTimeout: time.Nanosecond,
		SweepInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline := time.After(time.Second)
	for server.SessionCount() != 0 {
		select {
		case <-deadline:
			t.Fatal("session was not reaped by lifecycle worker")
		case <-time.After(time.Millisecond):
		}
	}
	ctx.Cancel()
	ctx.WaitDoneAndWaitGroupWait()
}

type mailboxSessionStore struct {
	mu       sync.Mutex
	bindings map[int64]msgq.SessionBinding
}

func newMailboxSessionStore() *mailboxSessionStore {
	return &mailboxSessionStore{bindings: make(map[int64]msgq.SessionBinding)}
}

func (s *mailboxSessionStore) Bind(_ context.Context, binding msgq.SessionBinding) (msgq.SessionBinding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, replaced := s.bindings[binding.RoleID]
	s.bindings[binding.RoleID] = binding
	return previous, replaced, nil
}

func (s *mailboxSessionStore) Find(_ context.Context, roleID int64) (msgq.SessionBinding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, found := s.bindings[roleID]
	return binding, found, nil
}

func (s *mailboxSessionStore) Refresh(_ context.Context, binding msgq.SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.bindings[binding.RoleID]
	return found && current == binding, nil
}

func (s *mailboxSessionStore) Unbind(_ context.Context, binding msgq.SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.bindings[binding.RoleID]
	if !found || current != binding {
		return false, nil
	}
	delete(s.bindings, binding.RoleID)
	return true, nil
}
