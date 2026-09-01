package msgq

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type recordingSessionRedisClient struct {
	getValue string
	getFound bool
	getKeys  []string
	eval     []redisEvalResult
	calls    []redisEvalCall
}

type redisEvalResult struct {
	value string
	err   error
}

type redisEvalCall struct {
	script string
	keys   []string
	args   []any
}

func (client *recordingSessionRedisClient) Get(_ context.Context, key string) (string, bool, error) {
	client.getKeys = append(client.getKeys, key)
	return client.getValue, client.getFound, nil
}

func (client *recordingSessionRedisClient) Eval(_ context.Context, script string, keys []string, args ...any) (string, error) {
	client.calls = append(client.calls, redisEvalCall{
		script: script,
		keys:   append([]string(nil), keys...),
		args:   append([]any(nil), args...),
	})
	result := client.eval[0]
	client.eval = client.eval[1:]
	return result.value, result.err
}

func TestRedisSessionStoreRejectsSubMillisecondTTLBeforeIO(t *testing.T) {
	client := &recordingSessionRedisClient{}
	store, err := NewRedisSessionStore(client, SessionRedisOptions{Prefix: "tx:test", TTL: time.Nanosecond})
	if !errors.Is(err, ErrInvalidSessionOptions) {
		t.Fatalf("NewRedisSessionStore error = %v, want ErrInvalidSessionOptions", err)
	}
	if store != nil {
		t.Fatalf("NewRedisSessionStore = %#v, want nil", store)
	}
	if len(client.calls) != 0 {
		t.Fatalf("eval calls = %d, want no Redis I/O", len(client.calls))
	}
}

func TestRedisSessionStoreRefreshIntervalUsesHalfTTL(t *testing.T) {
	store, err := NewRedisSessionStore(&recordingSessionRedisClient{}, SessionRedisOptions{Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if got := store.leaseRefreshInterval(); got != 30*time.Second {
		t.Fatalf("lease refresh interval = %s, want 30s", got)
	}
}

func TestRedisSessionStoreUsesVersionedSessionIDKeyAndCompleteRecord(t *testing.T) {
	binding := SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-1"}
	client := &recordingSessionRedisClient{
		getValue: `{"roleId":470001,"sessionId":99,"connectionId":7,"instanceId":"msgq-1"}`,
		getFound: true,
		eval:     []redisEvalResult{{value: "1"}, {value: "1"}, {value: "1"}, {value: "1"}},
	}
	store, err := NewRedisSessionStore(client, SessionRedisOptions{Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if created, err := store.Create(context.Background(), binding); err != nil || !created {
		t.Fatalf("create = %t, %v", created, err)
	}
	found, exists, err := store.Find(context.Background(), binding.SessionID)
	if err != nil || !exists || found != binding {
		t.Fatalf("find = %#v, %t, %v", found, exists, err)
	}
	replacement := binding
	replacement.ConnectionID = 8
	if rebound, err := store.Rebind(context.Background(), binding, replacement); err != nil || !rebound {
		t.Fatalf("rebind = %t, %v", rebound, err)
	}
	if refreshed, err := store.Refresh(context.Background(), replacement); err != nil || !refreshed {
		t.Fatalf("refresh = %t, %v", refreshed, err)
	}
	if removed, err := store.Unbind(context.Background(), replacement); err != nil || !removed {
		t.Fatalf("unbind = %t, %v", removed, err)
	}

	wantKey := "tx:test:session:v2:{99}"
	if !reflect.DeepEqual(client.getKeys, []string{wantKey}) {
		t.Fatalf("get keys = %#v, want %q", client.getKeys, wantKey)
	}
	if len(client.calls) != 4 {
		t.Fatalf("eval calls = %d, want 4", len(client.calls))
	}
	for _, call := range client.calls {
		if !reflect.DeepEqual(call.keys, []string{wantKey}) {
			t.Fatalf("eval keys = %#v, want %q", call.keys, wantKey)
		}
	}
	var record map[string]any
	encoded, ok := client.calls[0].args[0].(string)
	if !ok || json.Unmarshal([]byte(encoded), &record) != nil {
		t.Fatalf("create record = %#v", client.calls[0].args[0])
	}
	wantRecord := map[string]any{"roleId": float64(470001), "sessionId": float64(99), "connectionId": float64(7), "instanceId": "msgq-1"}
	if !reflect.DeepEqual(record, wantRecord) {
		t.Fatalf("create record = %#v, want %#v", record, wantRecord)
	}
}

func TestRedisSessionStoreRejectsMismatchedSessionRecord(t *testing.T) {
	client := &recordingSessionRedisClient{
		getValue: `{"roleId":470001,"sessionId":100,"connectionId":7,"instanceId":"msgq-1"}`,
		getFound: true,
	}
	store, err := NewRedisSessionStore(client, SessionRedisOptions{Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, _, err := store.Find(context.Background(), 99); !errors.Is(err, ErrSessionStoreResponse) {
		t.Fatalf("find mismatched record error = %v, want %v", err, ErrSessionStoreResponse)
	}
}

func TestRedisSessionStoreRebindOnlyChangesConnection(t *testing.T) {
	client := &recordingSessionRedisClient{}
	store, err := NewRedisSessionStore(client, SessionRedisOptions{Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	expected := SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-1"}
	for name, replacement := range map[string]SessionBinding{
		"role":     {RoleID: 470002, SessionID: 99, ConnectionID: 8, InstanceID: "msgq-1"},
		"session":  {RoleID: 470001, SessionID: 100, ConnectionID: 8, InstanceID: "msgq-1"},
		"instance": {RoleID: 470001, SessionID: 99, ConnectionID: 8, InstanceID: "msgq-2"},
	} {
		t.Run(name, func(t *testing.T) {
			if rebound, err := store.Rebind(context.Background(), expected, replacement); !errors.Is(err, ErrInvalidSession) || rebound {
				t.Fatalf("rebind = %t, %v, want invalid session", rebound, err)
			}
		})
	}
	if len(client.calls) != 0 {
		t.Fatalf("invalid replacements performed %d Redis calls", len(client.calls))
	}
}

func TestRedisSessionStorePrimaryUsesRoleIDKeyAndConditionalOperations(t *testing.T) {
	client := &recordingSessionRedisClient{
		getValue: "99",
		getFound: true,
		eval:     []redisEvalResult{{value: "1"}, {value: "1"}, {value: "1"}},
	}
	store, err := NewRedisSessionStore(client, SessionRedisOptions{Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.BindPrimary(context.Background(), 470001, 99); err != nil {
		t.Fatalf("bind primary: %v", err)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), 470001); err != nil || !found || sessionID != 99 {
		t.Fatalf("find primary = %d, %t, %v", sessionID, found, err)
	}
	if refreshed, err := store.RefreshPrimary(context.Background(), 470001, 99); err != nil || !refreshed {
		t.Fatalf("refresh primary = %t, %v", refreshed, err)
	}
	if removed, err := store.UnbindPrimary(context.Background(), 470001, 99); err != nil || !removed {
		t.Fatalf("unbind primary = %t, %v", removed, err)
	}
	wantKey := "tx:test:primary:v1:{470001}"
	if !reflect.DeepEqual(client.getKeys, []string{wantKey}) {
		t.Fatalf("get keys = %#v, want %q", client.getKeys, wantKey)
	}
	for _, call := range client.calls {
		if !reflect.DeepEqual(call.keys, []string{wantKey}) {
			t.Fatalf("eval keys = %#v, want %q", call.keys, wantKey)
		}
	}
}

func TestRedisSessionStoreRejectsUnknownConditionalResponse(t *testing.T) {
	binding := SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-1"}
	client := &recordingSessionRedisClient{eval: []redisEvalResult{{value: "unexpected"}}}
	store, err := NewRedisSessionStore(client, SessionRedisOptions{Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, err := store.Create(context.Background(), binding); !errors.Is(err, ErrSessionStoreResponse) {
		t.Fatalf("create response error = %v, want %v", err, ErrSessionStoreResponse)
	}
}
