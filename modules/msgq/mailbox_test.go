package msgq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBroadcastTopicSharesImmutablePayload(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	second := SessionHeader{RoleID: 2, SessionID: 1, ConnectionID: 2}
	for _, header := range []SessionHeader{first, second} {
		if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
			t.Fatalf("init %#v: %v", header, err)
		}
		if err := server.JoinTopics(context.Background(), TopicRequest{Header: header, Topics: []string{"guild:1"}}); err != nil {
			t.Fatalf("join topic: %v", err)
		}
	}
	payload := []byte("already-encoded-bon")
	if _, err := server.BroadcastTopic(context.Background(), BroadcastTopicRequest{Topic: "guild:1", Payload: payload}); err != nil {
		t.Fatalf("broadcast topic: %v", err)
	}
	firstSession, _ := server.lookup(first.RoleID)
	secondSession, _ := server.lookup(second.RoleID)
	firstSession.mu.Lock()
	defer firstSession.mu.Unlock()
	secondSession.mu.Lock()
	defer secondSession.mu.Unlock()
	firstPayload := firstSession.outbound.entries[firstSession.outbound.head].payload
	secondPayload := secondSession.outbound.entries[secondSession.outbound.head].payload
	if &firstPayload[0] != &payload[0] || &secondPayload[0] != &payload[0] {
		t.Fatal("broadcast payload was copied instead of shared")
	}
}

func TestOutboundQueueReleasesAcknowledgedStorage(t *testing.T) {
	var queue outboundQueue
	payload := []byte("payload")
	for range 128 {
		queue.enqueue(payload)
	}
	if &queue.entries[0].payload[0] != &payload[0] {
		t.Fatal("queue payload was copied")
	}
	entries := queue.dequeueSome(96)
	if len(entries) != 96 || queue.sentSeq != 96 {
		t.Fatalf("dequeue = %d entries, sent seq = %d", len(entries), queue.sentSeq)
	}
	if err := queue.confirm(96); err != nil {
		t.Fatalf("confirm first batch: %v", err)
	}
	if queue.head != 0 || len(queue.entries) != 32 || cap(queue.entries) > 64 || queue.sent != 0 {
		t.Fatalf("queue was not compacted: head=%d len=%d cap=%d sent=%d", queue.head, len(queue.entries), cap(queue.entries), queue.sent)
	}
	entries = queue.dequeueSome(0)
	if len(entries) != 32 || queue.sentSeq != 128 {
		t.Fatalf("dequeue remainder = %d entries, sent seq = %d", len(entries), queue.sentSeq)
	}
	if err := queue.confirm(128); err != nil {
		t.Fatalf("confirm all: %v", err)
	}
	if queue.entries != nil || queue.head != 0 || queue.sent != 0 || queue.bytes != 0 {
		t.Fatalf("queue retained acknowledged storage: %#v", queue)
	}
}

func TestSlowSessionWithoutAcknowledgementHitsByteLimit(t *testing.T) {
	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              &testSessionStore{},
		MaxBacklogMessages: 4,
		MaxBacklogBytes:    8,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for _, message := range [][]byte{[]byte("one!"), []byte("two!")} {
		if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: message}); err != nil {
			t.Fatalf("publish %q: %v", message, err)
		}
		if _, err := server.Fetch(context.Background(), FetchRequest{Header: header}); err != nil {
			t.Fatalf("fetch %q: %v", message, err)
		}
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("more")}); !errors.Is(err, ErrOutboundOverflow) {
		t.Fatalf("publish after unacknowledged backlog error = %v, want %v", err, ErrOutboundOverflow)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after overflow = %d, want 0", server.SessionCount())
	}
}

func TestAcknowledgementReleasesOutboundByteBudget(t *testing.T) {
	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              &testSessionStore{},
		MaxBacklogMessages: 2,
		MaxBacklogBytes:    4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("full")}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	message, err := server.Fetch(context.Background(), FetchRequest{Header: header})
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if err := server.Confirm(context.Background(), ConfirmRequest{Header: header, Ack: message.Seq}); err != nil {
		t.Fatalf("confirm first: %v", err)
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("next")}); err != nil {
		t.Fatalf("publish after acknowledgement: %v", err)
	}
}

func TestSlowSessionExpiresDespiteClientActivity(t *testing.T) {
	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              &testSessionStore{},
		MaxBacklogMessages: 2,
		OfflineTimeout:     time.Minute,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("pending")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := server.Fetch(context.Background(), FetchRequest{Header: header}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	session, found := server.lookup(header.RoleID)
	if !found {
		t.Fatal("session is missing")
	}
	session.mu.Lock()
	session.lastActivity = time.Now()
	session.outbound.entries[session.outbound.head].createdAt = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	removed, err := server.Reap(context.Background(), time.Now())
	if err != nil || removed != 1 {
		t.Fatalf("reap = %d, %v", removed, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after stale unacknowledged message = %d, want 0", server.SessionCount())
	}
}

type testSessionStore struct{}

func (*testSessionStore) Bind(context.Context, SessionBinding) (SessionBinding, bool, error) {
	return SessionBinding{}, false, nil
}

func (*testSessionStore) Find(context.Context, int64) (SessionBinding, bool, error) {
	return SessionBinding{}, false, nil
}

func (*testSessionStore) Refresh(context.Context, SessionBinding) (bool, error) {
	return true, nil
}

func (*testSessionStore) Unbind(context.Context, SessionBinding) (bool, error) {
	return true, nil
}
