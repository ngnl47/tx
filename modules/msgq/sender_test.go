package msgq

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestSenderPubSessionUsesExactSession(t *testing.T) {
	connection := &senderConnectionFake{pubResult: PubResult{Seq: 3}}
	sender, store, _ := newSenderForTest(t, connection, SessionBinding{
		RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a",
	})

	result, err := sender.PubSession(context.Background(), 11, []byte("encoded-bon"))
	if err != nil {
		t.Fatalf("PubSession returned error: %v", err)
	}
	if result.Seq != 3 || connection.pubSessionID != 11 || string(connection.pubPayload) != "encoded-bon" {
		t.Fatalf("publish = %#v, sessionID = %d, payload = %q", result, connection.pubSessionID, connection.pubPayload)
	}
	if len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 || len(store.findPrimaryRoleIDs) != 0 {
		t.Fatalf("exact route finds = %#v, primary finds = %#v", store.findSessionIDs, store.findPrimaryRoleIDs)
	}
}

func TestSenderPubRoleUsesPrimarySession(t *testing.T) {
	connection := &senderConnectionFake{pubResult: PubResult{Seq: 5}}
	sender, store, _ := newSenderForTest(t, connection, SessionBinding{
		RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a",
	})

	result, err := sender.PubRole(context.Background(), 47, []byte("encoded-bon"))
	if err != nil {
		t.Fatalf("PubRole returned error: %v", err)
	}
	if result.Seq != 5 || connection.pubSessionID != 11 || string(connection.pubPayload) != "encoded-bon" {
		t.Fatalf("publish = %#v, sessionID = %d, payload = %q", result, connection.pubSessionID, connection.pubPayload)
	}
	if len(store.findPrimaryRoleIDs) != 1 || store.findPrimaryRoleIDs[0] != 47 || len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 {
		t.Fatalf("primary finds = %#v, exact finds = %#v", store.findPrimaryRoleIDs, store.findSessionIDs)
	}
}

func TestSenderJoinTopicsUsesOwnerSessionConnectionHeader(t *testing.T) {
	connection := &senderConnectionFake{}
	sender, store, _ := newSenderForTest(t, connection, SessionBinding{
		RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a",
	})

	if err := sender.JoinTopics(context.Background(), 11, "guild:1", "region:2"); err != nil {
		t.Fatalf("JoinTopics returned error: %v", err)
	}
	wantHeader := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 13}
	if connection.topicRequest.Header != wantHeader {
		t.Fatalf("topic header = %#v, want %#v", connection.topicRequest.Header, wantHeader)
	}
	if got := connection.topicRequest.Topics; len(got) != 2 || got[0] != "guild:1" || got[1] != "region:2" {
		t.Fatalf("topics = %#v", got)
	}
	if len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 || len(store.findPrimaryRoleIDs) != 0 {
		t.Fatalf("topic route finds = %#v, primary finds = %#v", store.findSessionIDs, store.findPrimaryRoleIDs)
	}
}

func TestSenderBroadcastTopicUsesOwnerSessionMsgQInstance(t *testing.T) {
	connection := &senderConnectionFake{broadcastResult: BroadcastResult{
		Attempted: 3, Delivered: 2, Skipped: 1,
	}}
	sender, store, _ := newSenderForTest(t, connection, SessionBinding{
		RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a",
	})
	request := BroadcastTopicRequest{Topic: "guild:1", Payload: []byte("encoded-bon")}

	result, err := sender.BroadcastTopic(context.Background(), 11, request)
	if err != nil {
		t.Fatalf("BroadcastTopic returned error: %v", err)
	}
	if result != connection.broadcastResult {
		t.Fatalf("result = %#v, want %#v", result, connection.broadcastResult)
	}
	if connection.broadcastRequest.Topic != request.Topic || string(connection.broadcastRequest.Payload) != "encoded-bon" {
		t.Fatalf("broadcast request = %#v", connection.broadcastRequest)
	}
	if len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 || len(store.findPrimaryRoleIDs) != 0 {
		t.Fatalf("broadcast route finds = %#v, primary finds = %#v", store.findSessionIDs, store.findPrimaryRoleIDs)
	}
}

func TestSenderPubRoleUsesFirstPrimaryReadAsBoundary(t *testing.T) {
	first := SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a"}
	second := SessionBinding{RoleID: 47, SessionID: 12, ConnectionID: 14, InstanceID: "msgq-b"}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{ID: "msgq-a", Endpoint: "msgq-a:9000", Services: []string{"msgq"}, Status: discovery.StatusRunning},
		{ID: "msgq-b", Endpoint: "msgq-b:9000", Services: []string{"msgq"}, Status: discovery.StatusRunning},
	}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	store := &senderStoreFake{
		bindings:          map[int64]SessionBinding{11: first, 12: second},
		primarySessionIDs: []int64{11, 12},
		primaryFound:      true,
	}
	firstConnection := &senderConnectionFake{pubResult: PubResult{Seq: 1}}
	secondConnection := &senderConnectionFake{pubResult: PubResult{Seq: 1}}
	dialer := &senderDialerFake{connections: map[string]Connection{
		"msgq-a:9000": firstConnection,
		"msgq-b:9000": secondConnection,
	}}
	router, err := NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	sender, err := NewSender(router, dialer)
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })

	if _, err := sender.PubRole(context.Background(), 47, []byte("first")); err != nil {
		t.Fatalf("first PubRole returned error: %v", err)
	}
	if _, err := sender.PubRole(context.Background(), 47, []byte("second")); err != nil {
		t.Fatalf("second PubRole returned error: %v", err)
	}
	if firstConnection.pubSessionID != 11 || secondConnection.pubSessionID != 12 {
		t.Fatalf("published session IDs = %d, %d", firstConnection.pubSessionID, secondConnection.pubSessionID)
	}
	if len(store.findPrimaryRoleIDs) != 2 || store.findPrimaryRoleIDs[0] != 47 || store.findPrimaryRoleIDs[1] != 47 {
		t.Fatalf("primary role reads = %#v", store.findPrimaryRoleIDs)
	}
	if len(store.findSessionIDs) != 2 || store.findSessionIDs[0] != 11 || store.findSessionIDs[1] != 12 {
		t.Fatalf("exact session reads = %#v", store.findSessionIDs)
	}
}

func TestSenderPubRoleUpdatesSessionOnReusedEndpoint(t *testing.T) {
	first := SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a"}
	second := SessionBinding{RoleID: 47, SessionID: 12, ConnectionID: 14, InstanceID: "msgq-a"}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID: "msgq-a", Endpoint: "msgq-a:9000", Services: []string{"msgq"}, Status: discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	store := &senderStoreFake{
		bindings:          map[int64]SessionBinding{11: first, 12: second},
		primarySessionIDs: []int64{11, 12},
		primaryFound:      true,
	}
	connection := &senderConnectionFake{pubResult: PubResult{Seq: 1}}
	dialer := &senderDialerFake{connection: connection}
	router, err := NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	sender, err := NewSender(router, dialer)
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })

	for _, payload := range [][]byte{[]byte("first"), []byte("second")} {
		if _, err := sender.PubRole(context.Background(), 47, payload); err != nil {
			t.Fatalf("PubRole returned error: %v", err)
		}
	}
	if len(connection.pubSessionIDs) != 2 || connection.pubSessionIDs[0] != 11 || connection.pubSessionIDs[1] != 12 {
		t.Fatalf("published session IDs = %#v", connection.pubSessionIDs)
	}
	if dialer.calls != 1 {
		t.Fatalf("dial calls = %d, want 1", dialer.calls)
	}
}

func TestSenderPubRoleDoesNotRereadPrimaryWhenExactBindingIsMissing(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID: "msgq-a", Endpoint: "msgq-a:9000", Services: []string{"msgq"}, Status: discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	store := &senderStoreFake{
		bindings: map[int64]SessionBinding{
			12: {RoleID: 47, SessionID: 12, ConnectionID: 14, InstanceID: "msgq-a"},
		},
		primarySessionIDs: []int64{11, 12},
		primaryFound:      true,
	}
	dialer := &senderDialerFake{connection: &senderConnectionFake{}}
	router, err := NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	sender, err := NewSender(router, dialer)
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}

	if _, err := sender.PubRole(context.Background(), 47, []byte("encoded-bon")); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("PubRole error = %v, want ErrSessionOffline", err)
	}
	if len(store.findPrimaryRoleIDs) != 1 || len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 || dialer.calls != 0 {
		t.Fatalf("primary reads = %#v, exact reads = %#v, dial calls = %d", store.findPrimaryRoleIDs, store.findSessionIDs, dialer.calls)
	}
}

func TestSenderDoesNotRetryFailedPublish(t *testing.T) {
	pubErr := errors.New("publish response lost")
	connection := &senderConnectionFake{pubErr: pubErr}
	sender, store, dialer := newSenderForTest(t, connection, SessionBinding{
		RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a",
	})

	if _, err := sender.PubRole(context.Background(), 47, []byte("encoded-bon")); !errors.Is(err, pubErr) {
		t.Fatalf("PubRole error = %v, want publish error", err)
	}
	if connection.pubCalls != 1 || dialer.calls != 1 || len(store.findPrimaryRoleIDs) != 1 || len(store.findSessionIDs) != 1 {
		t.Fatalf("pub calls = %d, dial calls = %d, primary reads = %d, exact reads = %d", connection.pubCalls, dialer.calls, len(store.findPrimaryRoleIDs), len(store.findSessionIDs))
	}
}

func TestSenderTopicOperationsRejectOfflineOwnerBeforeDial(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace(nil); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	router, err := NewSessionRouter(directory, &senderStoreFake{})
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	dialer := &senderDialerFake{connection: &senderConnectionFake{}}
	sender, err := NewSender(router, dialer)
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}

	if err := sender.JoinTopics(context.Background(), 11, "guild:1"); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("JoinTopics error = %v, want ErrSessionOffline", err)
	}
	if _, err := sender.BroadcastTopic(context.Background(), 11, BroadcastTopicRequest{Topic: "guild:1"}); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("BroadcastTopic error = %v, want ErrSessionOffline", err)
	}
	if dialer.calls != 0 {
		t.Fatalf("dial calls = %d, want 0", dialer.calls)
	}
}

func newSenderForTest(t *testing.T, connection Connection, binding SessionBinding) (*Sender, *senderStoreFake, *senderDialerFake) {
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
	store := &senderStoreFake{
		binding:          binding,
		found:            true,
		primarySessionID: binding.SessionID,
		primaryFound:     true,
	}
	router, err := NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	dialer := &senderDialerFake{connection: connection}
	sender, err := NewSender(router, dialer)
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	return sender, store, dialer
}

type senderStoreFake struct {
	roleTestStore
	binding            SessionBinding
	bindings           map[int64]SessionBinding
	found              bool
	primarySessionID   int64
	primarySessionIDs  []int64
	primaryFound       bool
	findSessionIDs     []int64
	findPrimaryRoleIDs []int64
}

func (senderStoreFake) Create(context.Context, SessionBinding) (bool, error) {
	return false, errors.New("unexpected Create")
}

func (senderStoreFake) Rebind(context.Context, SessionBinding, SessionBinding) (bool, error) {
	return false, errors.New("unexpected Rebind")
}

func (store *senderStoreFake) Find(_ context.Context, sessionID int64) (SessionBinding, bool, error) {
	store.findSessionIDs = append(store.findSessionIDs, sessionID)
	if store.bindings != nil {
		binding, found := store.bindings[sessionID]
		return binding, found, nil
	}
	return store.binding, store.found, nil
}

func (*senderStoreFake) Refresh(context.Context, SessionBinding) (bool, error) {
	return false, errors.New("unexpected Refresh")
}

func (senderStoreFake) Unbind(context.Context, SessionBinding) (bool, error) {
	return false, errors.New("unexpected Unbind")
}

func (senderStoreFake) BindPrimary(context.Context, int64, int64) error {
	return errors.New("unexpected BindPrimary")
}

func (store *senderStoreFake) FindPrimary(_ context.Context, roleID int64) (int64, bool, error) {
	store.findPrimaryRoleIDs = append(store.findPrimaryRoleIDs, roleID)
	if index := len(store.findPrimaryRoleIDs) - 1; index < len(store.primarySessionIDs) {
		return store.primarySessionIDs[index], store.primaryFound, nil
	}
	return store.primarySessionID, store.primaryFound, nil
}

func (senderStoreFake) RefreshPrimary(context.Context, int64, int64) (bool, error) {
	return false, errors.New("unexpected RefreshPrimary")
}

func (senderStoreFake) UnbindPrimary(context.Context, int64, int64) (bool, error) {
	return false, errors.New("unexpected UnbindPrimary")
}

type senderDialerFake struct {
	connection  Connection
	connections map[string]Connection
	calls       int
}

func (dialer *senderDialerFake) Dial(_ context.Context, instance discovery.Instance) (Connection, error) {
	dialer.calls++
	if dialer.connections != nil {
		return dialer.connections[instance.Endpoint], nil
	}
	return dialer.connection, nil
}

type senderConnectionFake struct {
	pubSessionID     int64
	pubSessionIDs    []int64
	pubPayload       []byte
	pubResult        PubResult
	pubErr           error
	pubCalls         int
	topicRequest     TopicRequest
	broadcastRequest BroadcastTopicRequest
	broadcastResult  BroadcastResult
}

func (connection *senderConnectionFake) PubSession(_ context.Context, sessionID int64, payload []byte) (PubResult, error) {
	connection.pubCalls++
	connection.pubSessionID = sessionID
	connection.pubSessionIDs = append(connection.pubSessionIDs, sessionID)
	connection.pubPayload = payload
	return connection.pubResult, connection.pubErr
}

func (connection *senderConnectionFake) JoinTopics(_ context.Context, request TopicRequest) error {
	connection.topicRequest = request
	return nil
}

func (connection *senderConnectionFake) BroadcastTopic(_ context.Context, request BroadcastTopicRequest) (BroadcastResult, error) {
	connection.broadcastRequest = request
	return connection.broadcastResult, nil
}

func (*senderConnectionFake) Close() error { return nil }
