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

func TestSenderTopicOperationsRejectOfflineOwnerBeforeDial(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace(nil); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	store := &senderStoreFake{}
	router, err := NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	dialer := &senderDialerFake{connection: &senderConnectionFake{}}
	roles, err := NewRoleRouter(directory, store.FindRole, 16)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(router, roles, dialer)
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
	roles, err := NewRoleRouter(directory, store.FindRole, 16)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(router, roles, dialer)
	if err != nil {
		t.Fatalf("NewSender returned error: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	return sender, store, dialer
}

type senderStoreFake struct {
	roleTestStore
	roleReads          int
	binding            SessionBinding
	bindings           map[int64]SessionBinding
	found              bool
	primarySessionID   int64
	primarySessionIDs  []int64
	primaryFound       bool
	findSessionIDs     []int64
	findPrimaryRoleIDs []int64
}

func (store *senderStoreFake) FindRole(ctx context.Context, roleID int64) (RoleBinding, bool, error) {
	store.roleReads++
	return store.roleTestStore.FindRole(ctx, roleID)
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
	roleBindings     []RoleBinding
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

func (connection *senderConnectionFake) PubRole(_ context.Context, binding RoleBinding, payload []byte) (BroadcastResult, error) {
	connection.roleBindings = append(connection.roleBindings, binding)
	connection.pubPayload = payload
	return connection.broadcastResult, connection.pubErr
}

func TestSenderRoleCacheInvalidatesOnlyDefinitiveRejectionWithoutReplay(t *testing.T) {
	for _, failure := range []error{ErrSessionChanged, context.DeadlineExceeded, ErrRouteUnavailable, ErrDownMsgOverflow} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx := context.Background()
			connection := &senderConnectionFake{broadcastResult: BroadcastResult{Attempted: 2, Delivered: 1, Overflowed: 1}}
			sender, store, _ := newSenderForTest(t, connection, SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a"})
			first := RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "first"}
			if _, err := store.ClaimRole(ctx, first); err != nil {
				t.Fatal(err)
			}
			if _, err := sender.PubRole(ctx, 47, []byte("warm")); err != nil {
				t.Fatal(err)
			}
			connection.pubErr = failure
			result, err := sender.PubRole(ctx, 47, []byte("ambiguous"))
			if !errors.Is(err, failure) || result != connection.broadcastResult || len(connection.roleBindings) != 2 || store.roleReads != 1 {
				t.Fatalf("publish result=%+v error=%v calls=%d reads=%d", result, err, len(connection.roleBindings), store.roleReads)
			}
			if len(store.findSessionIDs) != 0 || len(store.findPrimaryRoleIDs) != 0 {
				t.Fatal("role publish resolved a session")
			}
			if _, err := store.ReleaseRole(ctx, first); err != nil {
				t.Fatal(err)
			}
			second := first
			second.Generation = "second"
			if _, err := store.ClaimRole(ctx, second); err != nil {
				t.Fatal(err)
			}
			connection.pubErr = nil
			if _, err := sender.PubRole(ctx, 47, []byte("next")); err != nil {
				t.Fatal(err)
			}
			wantBinding, wantReads := first, 1
			if errors.Is(failure, ErrSessionChanged) {
				wantBinding, wantReads = second, 2
			}
			if len(connection.roleBindings) != 3 || connection.roleBindings[2] != wantBinding || store.roleReads != wantReads {
				t.Fatalf("next binding=%+v reads=%d", connection.roleBindings, store.roleReads)
			}
		})
	}
}
