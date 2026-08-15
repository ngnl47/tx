package msgq

import (
	"context"
	"testing"
)

func TestBroadcastTopicSharesImmutablePayload(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklog: 4})
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
	if _, err := server.BroadcastTopic(context.Background(), BroadcastTopicRequest{Topic: "guild:1", Message: payload}); err != nil {
		t.Fatalf("broadcast topic: %v", err)
	}
	firstSession, _ := server.lookup(first.RoleID)
	secondSession, _ := server.lookup(second.RoleID)
	firstSession.mu.Lock()
	defer firstSession.mu.Unlock()
	secondSession.mu.Lock()
	defer secondSession.mu.Unlock()
	firstPayload := firstSession.outbound.entries[firstSession.outbound.head].message
	secondPayload := secondSession.outbound.entries[secondSession.outbound.head].message
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
	if &queue.entries[0].message[0] != &payload[0] {
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
