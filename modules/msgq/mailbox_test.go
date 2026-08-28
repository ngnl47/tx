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
	firstPayload := firstSession.downMsgs.entries[firstSession.downMsgs.head].payload
	secondPayload := secondSession.downMsgs.entries[secondSession.downMsgs.head].payload
	if &firstPayload[0] != &payload[0] || &secondPayload[0] != &payload[0] {
		t.Fatal("broadcast payload was copied instead of shared")
	}
}

func TestDownMsgQueueReleasesAcknowledgedStorage(t *testing.T) {
	var queue downMsgQueue
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
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("more")}); !errors.Is(err, ErrDownMsgOverflow) {
		t.Fatalf("publish after unacknowledged backlog error = %v, want %v", err, ErrDownMsgOverflow)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after overflow = %d, want 0", server.SessionCount())
	}
}

func TestAcknowledgementReleasesDownMsgByteBudget(t *testing.T) {
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

func TestInitWithoutRestoreAlwaysCreatesFreshSession(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: first}); err != nil {
		t.Fatalf("init first: %v", err)
	}
	if _, err := server.Rec(context.Background(), RecRequest{Header: first, Seq: 1}); err != nil {
		t.Fatalf("rec first: %v", err)
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: first.RoleID}, Payload: []byte("pending")}); err != nil {
		t.Fatalf("publish first: %v", err)
	}

	second := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 2}
	result, err := server.Init(context.Background(), InitRequest{Header: second})
	if err != nil {
		t.Fatalf("init fresh: %v", err)
	}
	if result.UpMsgSeq != 0 {
		t.Fatalf("fresh upMsg seq = %d, want 0", result.UpMsgSeq)
	}
	session, found := server.lookup(second.RoleID)
	if !found {
		t.Fatal("fresh session is missing")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.connectionID != second.ConnectionID || session.upMsgSeq != 0 || session.downMsgs.len() != 0 {
		t.Fatalf("fresh session retained old state: %#v", session)
	}
}

func TestInitRestoreRequiresExistingMatchingSession(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	missing := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: missing, Restore: true}); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("restore missing error = %v, want %v", err, ErrSessionOffline)
	}
	if _, found := server.lookup(missing.RoleID); found {
		t.Fatal("restore missing installed an empty session")
	}

	current := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 2}
	if _, err := server.Init(context.Background(), InitRequest{Header: current}); err != nil {
		t.Fatalf("init current: %v", err)
	}
	mismatch := SessionHeader{RoleID: 1, SessionID: 8, ConnectionID: 3}
	if _, err := server.Init(context.Background(), InitRequest{Header: mismatch, Restore: true}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("restore mismatched error = %v, want %v", err, ErrSessionChanged)
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
	session.downMsgs.entries[session.downMsgs.head].createdAt = time.Now().Add(-2 * time.Minute)
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
