package msgq

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestSenderJoinTopicsUsesCurrentConnectionHeader(t *testing.T) {
	connection := &senderConnectionFake{}
	sender := newSenderForTest(t, connection, SessionBinding{
		RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a",
	})

	if err := sender.JoinTopics(context.Background(), 47, "guild:1", "region:2"); err != nil {
		t.Fatalf("JoinTopics returned error: %v", err)
	}
	wantHeader := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 13}
	if connection.topicRequest.Header != wantHeader {
		t.Fatalf("topic header = %#v, want %#v", connection.topicRequest.Header, wantHeader)
	}
	if got := connection.topicRequest.Topics; len(got) != 2 || got[0] != "guild:1" || got[1] != "region:2" {
		t.Fatalf("topics = %#v", got)
	}
}

func TestSenderBroadcastTopicUsesOwnerMsgQInstance(t *testing.T) {
	connection := &senderConnectionFake{broadcastResult: BroadcastTopicResult{
		Attempted: 3, Delivered: 2, Skipped: 1,
	}}
	sender := newSenderForTest(t, connection, SessionBinding{
		RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a",
	})
	request := BroadcastTopicRequest{Topic: "guild:1", Payload: []byte("encoded-bon")}

	result, err := sender.BroadcastTopic(context.Background(), 47, request)
	if err != nil {
		t.Fatalf("BroadcastTopic returned error: %v", err)
	}
	if result != connection.broadcastResult {
		t.Fatalf("result = %#v, want %#v", result, connection.broadcastResult)
	}
	if connection.broadcastRequest.Topic != request.Topic || string(connection.broadcastRequest.Payload) != "encoded-bon" {
		t.Fatalf("broadcast request = %#v", connection.broadcastRequest)
	}
}

func TestSenderTopicOperationsRejectOfflineOwnerBeforeDial(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace(nil); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	router, err := NewSessionRouter(directory, senderStoreFake{})
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	dialer := &senderDialerFake{connection: &senderConnectionFake{}}
	sender, err := NewSender(router, dialer)
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}

	if err := sender.JoinTopics(context.Background(), 47, "guild:1"); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("JoinTopics error = %v, want ErrSessionOffline", err)
	}
	if _, err := sender.BroadcastTopic(context.Background(), 47, BroadcastTopicRequest{Topic: "guild:1"}); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("BroadcastTopic error = %v, want ErrSessionOffline", err)
	}
	if dialer.calls != 0 {
		t.Fatalf("dial calls = %d, want 0", dialer.calls)
	}
}

func newSenderForTest(t *testing.T, connection Connection, binding SessionBinding) *Sender {
	t.Helper()
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       binding.InstanceID,
		Endpoint: "msgq-a:9000",
		Services: []string{"msgq"},
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	router, err := NewSessionRouter(directory, senderStoreFake{binding: binding, found: true})
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	sender, err := NewSender(router, &senderDialerFake{connection: connection})
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	return sender
}

type senderStoreFake struct {
	binding SessionBinding
	found   bool
}

func (senderStoreFake) Bind(context.Context, SessionBinding) (SessionBinding, bool, error) {
	return SessionBinding{}, false, errors.New("unexpected Bind")
}

func (store senderStoreFake) Find(context.Context, int64) (SessionBinding, bool, error) {
	return store.binding, store.found, nil
}

func (senderStoreFake) Refresh(context.Context, SessionBinding) (bool, error) { return true, nil }

func (senderStoreFake) Unbind(context.Context, SessionBinding) (bool, error) {
	return false, errors.New("unexpected Unbind")
}

type senderDialerFake struct {
	connection Connection
	calls      int
}

func (dialer *senderDialerFake) Dial(context.Context, discovery.Instance) (Connection, error) {
	dialer.calls++
	return dialer.connection, nil
}

type senderConnectionFake struct {
	topicRequest     TopicRequest
	broadcastRequest BroadcastTopicRequest
	broadcastResult  BroadcastTopicResult
}

func (*senderConnectionFake) Pub(context.Context, PubRequest) (PubResult, error) {
	return PubResult{}, errors.New("unexpected Pub")
}

func (connection *senderConnectionFake) JoinTopics(_ context.Context, request TopicRequest) error {
	connection.topicRequest = request
	return nil
}

func (connection *senderConnectionFake) BroadcastTopic(_ context.Context, request BroadcastTopicRequest) (BroadcastTopicResult, error) {
	connection.broadcastRequest = request
	return connection.broadcastResult, nil
}

func (*senderConnectionFake) Close() error { return nil }
