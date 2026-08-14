package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

func TestMsgQSessionRouterResolvesBoundInstance(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "msgq-2",
		Services: []string{"msgq"},
		Endpoint: "127.0.0.1:9202",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}

	store := &sessionStoreFake{
		binding: msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-2"},
		found:   true,
	}
	router, err := msgq.NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("new session router: %v", err)
	}

	target, err := router.Resolve(context.Background(), 470001, 0)
	if err != nil {
		t.Fatalf("resolve session: %v", err)
	}
	if target.Instance.ID != "msgq-2" || target.Binding.RoleID != 470001 || target.Binding.SessionID != 99 {
		t.Fatalf("target = %#v", target)
	}
	if store.refreshCalls != 1 || store.refreshed != store.binding {
		t.Fatalf("refresh calls = %d, binding = %#v", store.refreshCalls, store.refreshed)
	}
}

func TestMsgQSessionRouterRejectsStaleSessionWithoutRefreshing(t *testing.T) {
	directory := rpcSessionDirectory(t, "msgq-2")
	store := &sessionStoreFake{
		binding: msgq.SessionBinding{RoleID: 470001, SessionID: 100, ConnectionID: 7, InstanceID: "msgq-2"},
		found:   true,
	}
	router, err := msgq.NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("new session router: %v", err)
	}

	_, err = router.Resolve(context.Background(), 470001, 99)
	if !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("resolve error = %v, want session changed", err)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refresh calls = %d, want 0", store.refreshCalls)
	}
}

func TestMsgQSessionRouterDoesNotRefreshUnavailableInstance(t *testing.T) {
	store := &sessionStoreFake{
		binding: msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-missing"},
		found:   true,
	}
	router, err := msgq.NewSessionRouter(discovery.NewServiceDirectory(), store)
	if err != nil {
		t.Fatalf("new session router: %v", err)
	}

	_, err = router.Resolve(context.Background(), 470001, 0)
	if !errors.Is(err, msgq.ErrRouteUnavailable) {
		t.Fatalf("resolve error = %v, want route unavailable", err)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refresh calls = %d, want 0", store.refreshCalls)
	}
}

func TestMsgQSessionRouterRejectsNonMsgQInstance(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "game-1",
		Services: []string{"game"},
		Endpoint: "127.0.0.1:9201",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	store := &sessionStoreFake{
		binding: msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "game-1"},
		found:   true,
	}
	router, err := msgq.NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("new session router: %v", err)
	}

	_, err = router.Resolve(context.Background(), 470001, 0)
	if !errors.Is(err, msgq.ErrRouteUnavailable) {
		t.Fatalf("resolve error = %v, want route unavailable", err)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refresh calls = %d, want 0", store.refreshCalls)
	}
}

func TestMsgQRedisSessionStoreUsesRoleHashTagAndConditionalOperations(t *testing.T) {
	client := &sessionRedisClientFake{
		getValue: `{"instanceId":"msgq-2","sessionId":99,"connectionId":7}`,
		evalResults: []string{
			`{"instanceId":"msgq-1","sessionId":98,"connectionId":6}`,
			"1",
			"1",
		},
	}
	store, err := msgq.NewRedisSessionStore(client, msgq.SessionRedisOptions{
		Prefix: "tx:test:rpc",
		TTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new redis session store: %v", err)
	}
	binding := msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-2"}

	previous, replaced, err := store.Bind(context.Background(), binding)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if !replaced || previous.SessionID != 98 || previous.InstanceID != "msgq-1" {
		t.Fatalf("bind previous = %#v, replaced = %t", previous, replaced)
	}
	found, exists, err := store.Find(context.Background(), 470001)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !exists || found != binding {
		t.Fatalf("find binding = %#v, exists = %t", found, exists)
	}
	if refreshed, err := store.Refresh(context.Background(), binding); err != nil || !refreshed {
		t.Fatalf("refresh = %t, %v", refreshed, err)
	}
	if removed, err := store.Unbind(context.Background(), binding); err != nil || !removed {
		t.Fatalf("unbind = %t, %v", removed, err)
	}

	if len(client.evalKeys) != 3 {
		t.Fatalf("eval keys = %d, want 3", len(client.evalKeys))
	}
	for _, keys := range client.evalKeys {
		if len(keys) != 1 || keys[0] != "tx:test:rpc:session:{470001}" {
			t.Fatalf("redis keys = %#v", keys)
		}
	}
}

func TestMsgQRoutedRedisSessionStoreResolvesEveryOperationByRoleID(t *testing.T) {
	client := &sessionRedisClientFake{
		getValue: `{"instanceId":"msgq-2","sessionId":99,"connectionId":7}`,
		evalResults: []string{
			"",
			"1",
			"1",
		},
	}
	resolver := &sessionRedisResolverFake{client: client}
	store, err := msgq.NewRoutedRedisSessionStore(resolver, msgq.SessionRedisOptions{
		Prefix: "tx:test:routed-session",
		TTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new routed redis session store: %v", err)
	}
	binding := msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-2"}
	if _, _, err := store.Bind(context.Background(), binding); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if _, found, err := store.Find(context.Background(), binding.RoleID); err != nil || !found {
		t.Fatalf("find found = %t, err = %v", found, err)
	}
	if refreshed, err := store.Refresh(context.Background(), binding); err != nil || !refreshed {
		t.Fatalf("refresh = %t, err = %v", refreshed, err)
	}
	if removed, err := store.Unbind(context.Background(), binding); err != nil || !removed {
		t.Fatalf("unbind = %t, err = %v", removed, err)
	}
	if len(resolver.roleIDs) != 4 {
		t.Fatalf("resolver role IDs = %#v", resolver.roleIDs)
	}
	for _, roleID := range resolver.roleIDs {
		if roleID != binding.RoleID {
			t.Fatalf("resolver role ID = %d, want %d", roleID, binding.RoleID)
		}
	}
}

func TestMsgQRedisSessionStoreIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run Redis session binding integration test")
	}
	ctx := context.Background()
	raw := redis.NewClient(&redis.Options{Addr: endpoint})
	defer raw.Close()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}

	prefix := fmt.Sprintf("tx:test:rpc-session:%d", time.Now().UnixNano())
	key := prefix + ":session:{470001}"
	defer raw.Del(ctx, key)
	store, err := msgq.NewRedisSessionStore(goredis.NewClient(raw), msgq.SessionRedisOptions{
		Prefix: prefix,
		TTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new redis session store: %v", err)
	}
	first := msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-1"}
	second := msgq.SessionBinding{RoleID: 470001, SessionID: 100, ConnectionID: 8, InstanceID: "msgq-2"}

	if previous, replaced, err := store.Bind(ctx, first); err != nil || replaced || previous != (msgq.SessionBinding{}) {
		t.Fatalf("first bind previous = %#v, replaced = %t, err = %v", previous, replaced, err)
	}
	if previous, replaced, err := store.Bind(ctx, second); err != nil || !replaced || previous != first {
		t.Fatalf("second bind previous = %#v, replaced = %t, err = %v", previous, replaced, err)
	}
	if refreshed, err := store.Refresh(ctx, first); err != nil || refreshed {
		t.Fatalf("refresh old binding = %t, %v", refreshed, err)
	}
	if refreshed, err := store.Refresh(ctx, second); err != nil || !refreshed {
		t.Fatalf("refresh current binding = %t, %v", refreshed, err)
	}
	if ttl, err := raw.PTTL(ctx, key).Result(); err != nil || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("binding ttl = %s, %v", ttl, err)
	}
	if removed, err := store.Unbind(ctx, first); err != nil || removed {
		t.Fatalf("unbind old binding = %t, %v", removed, err)
	}
	if removed, err := store.Unbind(ctx, second); err != nil || !removed {
		t.Fatalf("unbind current binding = %t, %v", removed, err)
	}
	if _, exists, err := store.Find(ctx, second.RoleID); err != nil || exists {
		t.Fatalf("find after unbind exists = %t, err = %v", exists, err)
	}
}

func rpcSessionDirectory(t *testing.T, instanceID string) *discovery.ServiceDirectory {
	t.Helper()
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       instanceID,
		Services: []string{"msgq"},
		Endpoint: "127.0.0.1:9202",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	return directory
}

type sessionStoreFake struct {
	binding      msgq.SessionBinding
	found        bool
	findErr      error
	refreshErr   error
	refreshCalls int
	refreshed    msgq.SessionBinding
}

type sessionRedisResolverFake struct {
	client  msgq.SessionRedisClient
	roleIDs []int64
}

func (r *sessionRedisResolverFake) ResolveSessionRedis(_ context.Context, roleID int64) (msgq.SessionRedisClient, error) {
	r.roleIDs = append(r.roleIDs, roleID)
	return r.client, nil
}

func (s *sessionStoreFake) Bind(context.Context, msgq.SessionBinding) (msgq.SessionBinding, bool, error) {
	return msgq.SessionBinding{}, false, errors.New("unexpected bind")
}

func (s *sessionStoreFake) Find(context.Context, int64) (msgq.SessionBinding, bool, error) {
	return s.binding, s.found, s.findErr
}

func (s *sessionStoreFake) Refresh(_ context.Context, binding msgq.SessionBinding) (bool, error) {
	s.refreshCalls++
	s.refreshed = binding
	return true, s.refreshErr
}

func (s *sessionStoreFake) Unbind(context.Context, msgq.SessionBinding) (bool, error) {
	return false, errors.New("unexpected unbind")
}

type sessionRedisClientFake struct {
	getValue    string
	evalResults []string
	evalKeys    [][]string
}

func (c *sessionRedisClientFake) Get(context.Context, string) (string, bool, error) {
	return c.getValue, true, nil
}

func (c *sessionRedisClientFake) Eval(_ context.Context, _ string, keys []string, _ ...any) (string, error) {
	c.evalKeys = append(c.evalKeys, append([]string(nil), keys...))
	result := c.evalResults[0]
	c.evalResults = c.evalResults[1:]
	return result, nil
}
