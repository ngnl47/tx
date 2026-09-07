package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

func TestMsgQSessionRouterResolvesExactSessionWithoutRefreshing(t *testing.T) {
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

	target, err := router.ResolveSession(context.Background(), 99)
	if err != nil {
		t.Fatalf("resolve session: %v", err)
	}
	if target.Instance.ID != "msgq-2" || target.Binding.RoleID != 470001 || target.Binding.SessionID != 99 {
		t.Fatalf("target = %#v", target)
	}
	if len(store.findSessionIDs) != 1 || store.findSessionIDs[0] != 99 {
		t.Fatalf("find session IDs = %#v, want [99]", store.findSessionIDs)
	}
	if !slices.Equal(store.operations, []string{"Find"}) {
		t.Fatalf("store operations = %#v, want [Find]", store.operations)
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

	_, err = router.ResolveSession(context.Background(), 99)
	if !errors.Is(err, msgq.ErrRouteUnavailable) {
		t.Fatalf("resolve error = %v, want route unavailable", err)
	}
	if !slices.Equal(store.operations, []string{"Find"}) {
		t.Fatalf("store operations = %#v, want [Find]", store.operations)
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

	_, err = router.ResolveSession(context.Background(), 99)
	if !errors.Is(err, msgq.ErrRouteUnavailable) {
		t.Fatalf("resolve error = %v, want route unavailable", err)
	}
	if !slices.Equal(store.operations, []string{"Find"}) {
		t.Fatalf("store operations = %#v, want [Find]", store.operations)
	}
}

func TestMsgQSessionRouterRejectsMismatchedSessionWithoutRefreshing(t *testing.T) {
	store := &sessionStoreFake{
		binding: msgq.SessionBinding{RoleID: 470001, SessionID: 100, ConnectionID: 7, InstanceID: "msgq-2"},
		found:   true,
	}
	router, err := msgq.NewSessionRouter(rpcSessionDirectory(t, "msgq-2"), store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.ResolveSession(context.Background(), 99); !errors.Is(err, msgq.ErrSessionOffline) {
		t.Fatalf("mismatched session: %v", err)
	}
	if !slices.Equal(store.operations, []string{"Find"}) {
		t.Fatalf("store operations = %#v", store.operations)
	}
}

func TestMsgQRedisSessionStoreUsesSessionHashTagAndConditionalOperations(t *testing.T) {
	client := &sessionRedisClientFake{
		getValue: `{"roleId":470001,"sessionId":99,"connectionId":7,"instanceId":"msgq-2"}`,
		evalResults: []string{
			"1",
			"1",
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

	created, err := store.Create(context.Background(), binding)
	if err != nil || !created {
		t.Fatalf("create = %t, %v", created, err)
	}
	found, exists, err := store.Find(context.Background(), binding.SessionID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !exists || found != binding {
		t.Fatalf("find binding = %#v, exists = %t", found, exists)
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

	if len(client.evalKeys) != 4 {
		t.Fatalf("eval keys = %d, want 4", len(client.evalKeys))
	}
	for _, keys := range client.evalKeys {
		if len(keys) != 1 || keys[0] != "tx:test:rpc:session:v2:{99}" {
			t.Fatalf("redis keys = %#v", keys)
		}
	}
}

func TestMsgQRoutedRedisSessionStoreResolvesExactOperationsBySessionID(t *testing.T) {
	client := &sessionRedisClientFake{
		getValue: `{"roleId":470001,"sessionId":99,"connectionId":7,"instanceId":"msgq-2"}`,
		evalResults: []string{
			"1",
			"1",
			"1",
			"1",
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
	if created, err := store.Create(context.Background(), binding); err != nil || !created {
		t.Fatalf("create = %t, %v", created, err)
	}
	if _, found, err := store.Find(context.Background(), binding.SessionID); err != nil || !found {
		t.Fatalf("find found = %t, err = %v", found, err)
	}
	if refreshed, err := store.Refresh(context.Background(), binding); err != nil || !refreshed {
		t.Fatalf("refresh = %t, err = %v", refreshed, err)
	}
	if removed, err := store.Unbind(context.Background(), binding); err != nil || !removed {
		t.Fatalf("unbind = %t, err = %v", removed, err)
	}
	if len(resolver.roleIDs) != 4 {
		t.Fatalf("resolver shard keys = %#v", resolver.roleIDs)
	}
	for _, sessionID := range resolver.roleIDs {
		if sessionID != binding.SessionID {
			t.Fatalf("resolver shard key = %d, want SessionID %d", sessionID, binding.SessionID)
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
	firstKey := prefix + ":session:v2:{99}"
	secondKey := prefix + ":session:v2:{100}"
	defer raw.Del(ctx, firstKey, secondKey)
	store, err := msgq.NewRedisSessionStore(goredis.NewClient(raw), msgq.SessionRedisOptions{
		Prefix: prefix,
		TTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new redis session store: %v", err)
	}
	first := msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-1"}
	second := msgq.SessionBinding{RoleID: 470001, SessionID: 100, ConnectionID: 8, InstanceID: "msgq-2"}

	if created, err := store.Create(ctx, first); err != nil || !created {
		t.Fatalf("create first = %t, %v", created, err)
	}
	firstTTL, err := raw.PTTL(ctx, firstKey).Result()
	if err != nil || firstTTL <= 0 {
		t.Fatalf("first ttl = %s, %v", firstTTL, err)
	}
	if created, err := store.Create(ctx, first); err != nil || created {
		t.Fatalf("create conflict = %t, %v", created, err)
	}
	if found, exists, err := store.Find(ctx, first.SessionID); err != nil || !exists || found != first {
		t.Fatalf("binding after conflict = %#v, %t, %v", found, exists, err)
	}
	if ttl, err := raw.PTTL(ctx, firstKey).Result(); err != nil || ttl > firstTTL {
		t.Fatalf("conflict ttl = %s, %v, want <= %s", ttl, err, firstTTL)
	}
	if created, err := store.Create(ctx, second); err != nil || !created {
		t.Fatalf("create second = %t, %v", created, err)
	}
	rebound := first
	rebound.ConnectionID = 9
	if changed, err := store.Rebind(ctx, first, rebound); err != nil || !changed {
		t.Fatalf("rebind first = %t, %v", changed, err)
	}
	reboundTTL, err := raw.PTTL(ctx, firstKey).Result()
	if err != nil || reboundTTL <= 0 {
		t.Fatalf("rebound ttl = %s, %v", reboundTTL, err)
	}
	if changed, err := store.Rebind(ctx, first, second); !errors.Is(err, msgq.ErrInvalidSession) || changed {
		t.Fatalf("cross-session rebind = %t, %v", changed, err)
	}
	if refreshed, err := store.Refresh(ctx, first); err != nil || refreshed {
		t.Fatalf("refresh stale first = %t, %v", refreshed, err)
	}
	if removed, err := store.Unbind(ctx, first); err != nil || removed {
		t.Fatalf("unbind stale first = %t, %v", removed, err)
	}
	if ttl, err := raw.PTTL(ctx, firstKey).Result(); err != nil || ttl <= 0 || ttl > reboundTTL {
		t.Fatalf("ttl after stale operations = %s, %v, want <= %s", ttl, err, reboundTTL)
	}
	if found, exists, err := store.Find(ctx, rebound.SessionID); err != nil || !exists || found != rebound {
		t.Fatalf("find rebound = %#v, %t, %v", found, exists, err)
	}
	if found, exists, err := store.Find(ctx, second.SessionID); err != nil || !exists || found != second {
		t.Fatalf("find second = %#v, %t, %v", found, exists, err)
	}
}

func TestMsgQRedisSessionStoreConcurrentRebindIntegration(t *testing.T) {
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
	prefix := fmt.Sprintf("tx:test:rpc-session-race:%d", time.Now().UnixNano())
	key := prefix + ":session:v2:{99}"
	defer raw.Del(ctx, key)
	store, err := msgq.NewRedisSessionStore(goredis.NewClient(raw), msgq.SessionRedisOptions{Prefix: prefix, TTL: time.Minute})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	initial := msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-1"}
	if created, err := store.Create(ctx, initial); err != nil || !created {
		t.Fatalf("create initial = %t, %v", created, err)
	}
	candidates := []msgq.SessionBinding{initial, initial}
	candidates[0].ConnectionID = 8
	candidates[1].ConnectionID = 9
	start := make(chan struct{})
	type result struct {
		index   int
		rebound bool
		err     error
	}
	results := make(chan result, len(candidates))
	for index, candidate := range candidates {
		go func() {
			<-start
			rebound, err := store.Rebind(ctx, initial, candidate)
			results <- result{index: index, rebound: rebound, err: err}
		}()
	}
	close(start)
	winner := -1
	for range candidates {
		result := <-results
		if result.err != nil {
			t.Fatalf("rebind %d: %v", result.index, result.err)
		}
		if result.rebound {
			if winner >= 0 {
				t.Fatalf("multiple rebind winners: %d and %d", winner, result.index)
			}
			winner = result.index
		}
	}
	if winner < 0 {
		t.Fatal("concurrent rebind had no winner")
	}
	current, found, err := store.Find(ctx, initial.SessionID)
	if err != nil || !found || current != candidates[winner] {
		t.Fatalf("current = %#v, %t, %v, want winner %#v", current, found, err, candidates[winner])
	}
	loser := 1 - winner
	if refreshed, err := store.Refresh(ctx, candidates[loser]); err != nil || refreshed {
		t.Fatalf("refresh loser = %t, %v", refreshed, err)
	}
	if refreshed, err := store.Refresh(ctx, candidates[winner]); err != nil || !refreshed {
		t.Fatalf("refresh winner = %t, %v", refreshed, err)
	}
	if removed, err := store.Unbind(ctx, candidates[winner]); err != nil || !removed {
		t.Fatalf("unbind winner = %t, %v", removed, err)
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
	roleTestStore
	operations     []string
	binding        msgq.SessionBinding
	found          bool
	findErr        error
	findSessionIDs []int64
	refreshErr     error
	refreshCalls   int
	refreshed      msgq.SessionBinding
}

type sessionRedisResolverFake struct {
	client  msgq.SessionRedisClient
	roleIDs []int64
}

func (r *sessionRedisResolverFake) ResolveSessionRedis(_ context.Context, roleID int64) (msgq.SessionRedisClient, error) {
	r.roleIDs = append(r.roleIDs, roleID)
	return r.client, nil
}

func (s *sessionStoreFake) Create(context.Context, msgq.SessionBinding) (bool, error) {
	s.operations = append(s.operations, "Create")
	return false, errors.New("unexpected create")
}

func (s *sessionStoreFake) Rebind(context.Context, msgq.SessionBinding, msgq.SessionBinding) (bool, error) {
	s.operations = append(s.operations, "Rebind")
	return false, errors.New("unexpected rebind")
}

func (s *sessionStoreFake) Find(_ context.Context, sessionID int64) (msgq.SessionBinding, bool, error) {
	s.operations = append(s.operations, "Find")
	s.findSessionIDs = append(s.findSessionIDs, sessionID)
	return s.binding, s.found, s.findErr
}

func (s *sessionStoreFake) Refresh(_ context.Context, binding msgq.SessionBinding) (bool, error) {
	s.operations = append(s.operations, "Refresh")
	s.refreshCalls++
	s.refreshed = binding
	return true, s.refreshErr
}

func (s *sessionStoreFake) Unbind(context.Context, msgq.SessionBinding) (bool, error) {
	s.operations = append(s.operations, "Unbind")
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
