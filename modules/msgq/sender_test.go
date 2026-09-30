package msgq

import (
	"context"
	"errors"
	"fmt"
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
	for range 100 {
		if _, err := sender.PubSession(context.Background(), 11, []byte("next")); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 {
		t.Fatalf("exact route finds = %#v", store.findSessionIDs)
	}
	// 恢复只更新物理连接，Topic 必须重新读取绑定，不能使用精确投递缓存中的旧身份。
	store.binding.ConnectionID = 14
	if err := sender.JoinTopics(context.Background(), 11, "guild:1"); err != nil {
		t.Fatal(err)
	}
	if connection.topicRequest.Header.ConnectionID != 14 {
		t.Fatal("Topic 使用了旧 ConnectionID")
	}
	directory := sender.router.directory.(*discovery.ServiceDirectory)
	if err := directory.Replace(nil); err != nil {
		t.Fatal(err)
	}
	before := connection.pubCalls
	if _, err := sender.PubSession(context.Background(), 11, []byte("departed")); !errors.Is(err, ErrRouteUnavailable) {
		t.Fatal(err)
	}
	if connection.pubCalls != before {
		t.Fatal("向已退出的 MsgQ 发送了载荷")
	}
}

func TestSenderStreamPublishesToExactSessionAndRoleOwnerWithoutResult(t *testing.T) {
	connection := &senderConnectionFake{}
	sender, store, _ := newSenderForTest(t, connection, SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a"})
	role := RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "generation-1"}
	if _, err := store.ClaimRole(context.Background(), role); err != nil {
		t.Fatal(err)
	}
	if err := sender.SendSession(context.Background(), 11, []byte("session")); err != nil {
		t.Fatal(err)
	}
	if err := sender.SendRole(context.Background(), 47, []byte("role")); err != nil {
		t.Fatal(err)
	}
	if connection.streamSession != 11 || connection.streamRole != role || connection.streamPayloads[0] != "session" || connection.streamPayloads[1] != "role" {
		t.Fatalf("stream publications session=%d role=%+v payloads=%v", connection.streamSession, connection.streamRole, connection.streamPayloads)
	}
	if connection.pubCalls != 0 || len(connection.roleBindings) != 0 {
		t.Fatal("stream publication unexpectedly used unary publish")
	}
}

func TestSenderDoesNotReplayRoleRejectionWithoutPayload(t *testing.T) {
	connection := &senderConnectionFake{}
	sender, store, _ := newSenderForTest(t, connection, SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a"})
	role := RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "generation-1"}
	if _, err := store.ClaimRole(context.Background(), role); err != nil {
		t.Fatal(err)
	}
	if err := sender.SendRole(context.Background(), 47, []byte("first")); err != nil {
		t.Fatal(err)
	}
	connection.rejectRole(context.Background(), role, nil)
	if len(connection.roleBindings) != 0 {
		t.Fatal("缺少原载荷的拒绝被重投")
	}
}

func TestSenderMailboxCacheInvalidationDoesNotRetryPayload(t *testing.T) {
	connection := &senderConnectionFake{pubResult: PubResult{Seq: 1}}
	sender, store, _ := newSenderForTest(t, connection, SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a"})
	ctx := context.Background()
	if _, err := sender.PubSession(ctx, 11, []byte("first")); err != nil {
		t.Fatal(err)
	}
	connection.pubErr = context.DeadlineExceeded
	if _, err := sender.PubSession(ctx, 11, []byte("unknown")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	connection.pubErr = ErrSessionOffline
	if _, err := sender.PubSession(ctx, 11, []byte("offline")); !errors.Is(err, ErrSessionOffline) {
		t.Fatal(err)
	}
	if connection.pubCalls != 3 || len(store.findSessionIDs) != 1 {
		t.Fatal("错误触发了重发或超时误清缓存")
	}
	store.found = false
	if _, err := sender.PubSession(ctx, 11, []byte("after")); !errors.Is(err, ErrSessionOffline) {
		t.Fatal(err)
	}
	if connection.pubCalls != 3 || len(store.findSessionIDs) != 2 {
		t.Fatal("明确离线后未重新读取绑定")
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
	if len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 {
		t.Fatalf("topic route finds = %#v", store.findSessionIDs)
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
	if len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 11 {
		t.Fatalf("broadcast route finds = %#v", store.findSessionIDs)
	}
}

func TestSenderTopicOperationsRejectOfflineOwnerBeforeDial(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace(nil); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	store := &senderStoreFake{}
	router, err := NewSessionRouter("msgq", directory, store, 128)
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	dialer := &senderDialerFake{connection: &senderConnectionFake{}}
	roles, err := NewRoleRouter("msgq", directory, store.FindRole, 16)
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
		binding: binding,
		found:   true,
	}
	router, err := NewSessionRouter("msgq", directory, store, 128)
	if err != nil {
		t.Fatalf("NewSessionRouter returned error: %v", err)
	}
	dialer := &senderDialerFake{connection: connection}
	roles, err := NewRoleRouter("msgq", directory, store.FindRole, 16)
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
	roleReads      int
	binding        SessionBinding
	bindings       map[int64]SessionBinding
	found          bool
	findSessionIDs []int64
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
	rolePublish      func(RoleBinding) (BroadcastResult, error)
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
	streamSession    int64
	streamRole       RoleBinding
	streamPayloads   []string
	rejectRole       func(context.Context, RoleBinding, []byte)
}

func (connection *senderConnectionFake) OnRoleRejection(reject func(context.Context, RoleBinding, []byte)) {
	connection.rejectRole = reject
}

func (connection *senderConnectionFake) SendSession(_ context.Context, sessionID int64, payload []byte) error {
	connection.streamSession = sessionID
	connection.streamPayloads = append(connection.streamPayloads, string(payload))
	return nil
}

func (connection *senderConnectionFake) SendRole(_ context.Context, binding RoleBinding, payload []byte) error {
	connection.streamRole = binding
	connection.streamPayloads = append(connection.streamPayloads, string(payload))
	return nil
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
	if connection.rolePublish != nil {
		return connection.rolePublish(binding)
	}
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
			if len(store.findSessionIDs) != 0 {
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

func TestSenderRoleBindingRefreshRetriesOnce(t *testing.T) {
	for _, finalErr := range []error{nil, ErrRoleBindingChanged, context.DeadlineExceeded} {
		t.Run(fmt.Sprint(finalErr), func(t *testing.T) {
			ctx := context.Background()
			connection := &senderConnectionFake{}
			sender, store, _ := newSenderForTest(t, connection, SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 13, InstanceID: "msgq-a"})
			first := RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "first"}
			if _, err := store.ClaimRole(ctx, first); err != nil {
				t.Fatal(err)
			}
			if _, err := sender.PubRole(ctx, 47, []byte("warm")); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReleaseRole(ctx, first); err != nil {
				t.Fatal(err)
			}
			second := first
			second.Generation = "second"
			if _, err := store.ClaimRole(ctx, second); err != nil {
				t.Fatal(err)
			}
			connection.rolePublish = func(binding RoleBinding) (BroadcastResult, error) {
				if binding == first {
					return BroadcastResult{}, mailboxClientError(mailboxStatusError(ErrRoleBindingChanged))
				}
				if binding != second {
					t.Fatalf("unexpected binding: %+v", binding)
				}
				if finalErr != nil {
					return BroadcastResult{}, finalErr
				}
				return BroadcastResult{Attempted: 1, Delivered: 1}, nil
			}
			result, err := sender.PubRole(ctx, 47, []byte("login-response"))
			if !errors.Is(err, finalErr) || len(connection.roleBindings) != 3 || store.roleReads != 2 || string(connection.pubPayload) != "login-response" {
				t.Fatalf("result=%+v err=%v calls=%d reads=%d", result, err, len(connection.roleBindings), store.roleReads)
			}
			if finalErr == nil && result.Delivered != 1 {
				t.Fatal(result)
			}
		})
	}
}
