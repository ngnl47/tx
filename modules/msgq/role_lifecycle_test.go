package msgq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

func TestRoleMailboxRedisOwnershipLifecycle(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT for real Redis verification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw := redis.NewClient(&redis.Options{Addr: endpoint})
	defer raw.Close()
	commands := &redisCommandCounter{}
	raw.AddHook(commands)
	prefix := fmt.Sprintf("tx:test:role-lifecycle:%d", time.Now().UnixNano())
	defer raw.Del(context.Background(), prefix+":role:{47}", prefix+":session:v2:{11}", prefix+":session:v2:{12}", prefix+":session:v2:{13}")
	store, err := NewRedisSessionStore(goredis.NewClient(raw), SessionRedisOptions{Prefix: prefix, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewServer(ServerOptions{InstanceID: "msgq-b", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	first := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	second := SessionHeader{RoleID: 47, SessionID: 12, ConnectionID: 2}
	third := SessionHeader{RoleID: 47, SessionID: 13, ConnectionID: 3}
	if err := a.Init(ctx, InitRequest{Header: first}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PubSession(ctx, first.SessionID, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	if err := b.Init(ctx, InitRequest{Header: second}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("split owner accepted: %v", err)
	}
	if err := a.Init(ctx, InitRequest{Header: second}); err != nil {
		t.Fatal(err)
	}
	first.ConnectionID = 4
	if err := a.Init(ctx, InitRequest{Header: first, Restore: true}); err != nil {
		t.Fatalf("peer login prevented first session restore: %v", err)
	}
	if err := a.Touch(ctx, TouchRequest{Header: second}); err != nil {
		t.Fatalf("first restore affected peer activity: %v", err)
	}
	message, err := a.Fetch(ctx, FetchRequest{Header: first})
	if err != nil || message.Seq != 1 || string(message.Payload) != "pending" {
		t.Fatalf("restored queue = %+v, %v", message, err)
	}
	if err := a.Confirm(ctx, ConfirmRequest{Header: first, Ack: 1}); err != nil {
		t.Fatal(err)
	}
	owner, found, err := store.FindRole(ctx, 47)
	if err != nil || !found || owner.InstanceID != "msgq-a" {
		t.Fatalf("owner = %+v, %t, %v", owner, found, err)
	}
	if result, err := a.PubRole(ctx, owner, []byte("shared")); err != nil || result.Delivered != 2 {
		t.Fatalf("shared delivery = %+v, %v", result, err)
	}
	for _, item := range []struct {
		header SessionHeader
		seq    int64
	}{{first, 2}, {second, 1}} {
		message, err := a.Fetch(ctx, FetchRequest{Header: item.header})
		if err != nil || message.Seq != item.seq || string(message.Payload) != "shared" {
			t.Fatalf("independent mailbox = %+v, %v", message, err)
		}
		if err := a.Confirm(ctx, ConfirmRequest{Header: item.header, Ack: item.seq}); err != nil {
			t.Fatal(err)
		}
	}
	// 真实 Redis 客户端统计命令，不使用全局 MONITOR 或扫描；续期窗口内的热链路不得回源。
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "msgq-a", Endpoint: "localhost:9001", Services: []string{"msgq"}, Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	router, err := NewRoleRouter("msgq", directory, store.FindRole, 128)
	if err != nil {
		t.Fatal(err)
	}
	before := commands.count.Load()
	if _, err := router.Resolve(ctx, 47); err != nil {
		t.Fatal(err)
	}
	if delta := commands.count.Load() - before; delta != 1 {
		t.Fatalf("角色冷查询 Redis 命令数 = %d，期望 1", delta)
	}
	before = commands.count.Load()
	for seq := int64(1); seq <= 100; seq++ {
		target, err := router.Resolve(ctx, 47)
		if err != nil {
			t.Fatal(err)
		}
		if result, err := a.PubRole(ctx, target.Binding, []byte("hot")); err != nil || result.Delivered != 2 {
			t.Fatalf("热链路分发 = %+v, %v", result, err)
		}
		for _, item := range []struct {
			header SessionHeader
			ack    int64
		}{{first, seq + 2}, {second, seq + 1}} {
			if message, err := a.Fetch(ctx, FetchRequest{Header: item.header}); err != nil || message.Seq != item.ack || string(message.Payload) != "hot" {
				t.Fatalf("热邮箱消息 = %+v, %v", message, err)
			}
			if err := a.Confirm(ctx, ConfirmRequest{Header: item.header, Ack: item.ack}); err != nil {
				t.Fatal(err)
			}
			if err := a.Confirm(ctx, ConfirmRequest{Header: item.header, Ack: item.ack}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if delta := commands.count.Load() - before; delta != 0 {
		t.Fatalf("100 次角色下行及双邮箱 Fetch/Rec/Confirm 产生 %d 次 Redis 命令", delta)
	}
	t.Log("真实 Redis：角色冷查询 1 次；100 次热角色下行及双邮箱 Fetch/Rec/Confirm 额外命令 0 次")
	// 推进本地续期时间而不等待墙钟，验证续期仍然有效且不会退化为逐消息写入。
	mailbox, _ := a.lookup(first.SessionID)
	mailbox.mu.Lock()
	mailbox.nextLeaseRefresh = time.Now().Add(-time.Second)
	mailbox.mu.Unlock()
	before = commands.count.Load()
	for range 100 {
		if err := a.Touch(ctx, TouchRequest{Header: first}); err != nil {
			t.Fatal(err)
		}
	}
	if delta := commands.count.Load() - before; delta != 1 {
		t.Fatalf("到期后 100 次活动的续期命令数 = %d，期望 1", delta)
	}
	t.Log("真实 Redis：Session 续期到期后 100 次活动仅执行 1 次续期命令")
	if err := a.TerminateSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := a.TerminateDeadSessions(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if current, found, err := store.FindRole(ctx, 47); err != nil || !found || current != owner {
		t.Fatalf("peer lost ownership: %+v, %t, %v", current, found, err)
	}
	if err := a.TerminateSession(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := a.TerminateDeadSessions(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.FindRole(ctx, 47); err != nil || found {
		t.Fatalf("empty owner retained: %t, %v", found, err)
	}
	if err := b.Init(ctx, InitRequest{Header: third}); err != nil {
		t.Fatal(err)
	}
	if err := a.Init(ctx, InitRequest{Header: first, Restore: true}); !errors.Is(err, ErrSessionChanged) && !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("old restore = %v", err)
	}
	if current, found, err := store.FindRole(ctx, 47); err != nil || !found || current.InstanceID != "msgq-b" || current.Generation == owner.Generation {
		t.Fatalf("new owner = %+v, %t, %v", current, found, err)
	}
	if err := b.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.FindRole(ctx, 47); err != nil || found {
		t.Fatalf("stopped owner retained: %t, %v", found, err)
	}
}

// redisCommandCounter 只统计该用例客户端实际执行的命令，不改变执行结果。
type redisCommandCounter struct{ count atomic.Int64 }

func (c *redisCommandCounter) DialHook(next redis.DialHook) redis.DialHook {
	return next
}
func (c *redisCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { c.count.Add(1); return next(ctx, cmd) }
}
func (c *redisCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		c.count.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

func TestExpiredRoleLeaseRejectsTrafficAndCannotReleaseNewOwner(t *testing.T) {
	ctx := context.Background()
	store := &testSessionStore{}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if err := server.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	lease := server.roleLeases[47]
	lease.expires = time.Now().Add(-time.Second)
	server.roleLeases[47] = lease
	server.mu.Unlock()
	replacement := RoleBinding{RoleID: 47, InstanceID: "msgq-b", Generation: "replacement"}
	store.roleMu.Lock()
	store.owners[47] = replacement
	store.roleMu.Unlock()
	if _, err := server.PubSession(ctx, 11, []byte("stale")); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("stale publish = %v", err)
	}
	if _, err := server.BroadcastRole(ctx, 47, []byte("stale")); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("stale role broadcast = %v", err)
	}
	if _, err := server.PubRole(ctx, lease.binding, []byte("stale")); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("stale versioned publish = %v", err)
	}
	if err := server.Touch(ctx, TouchRequest{Header: header}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("stale connection activity = %v", err)
	}
	if _, err := server.TerminateDeadSessions(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if server.SessionCount() != 0 {
		t.Fatal("old role mailboxes remain active")
	}
	if owner, found, err := store.FindRole(ctx, 47); err != nil || !found || owner != replacement {
		t.Fatalf("new owner deleted: %+v, %t, %v", owner, found, err)
	}
}

func TestRoleLeaseRefreshIsNotPerMessage(t *testing.T) {
	ctx := context.Background()
	store := &testSessionStore{}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 128})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if err := server.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if _, err := server.PubSession(ctx, 11, []byte("update")); err != nil {
			t.Fatal(err)
		}
	}
	if store.roleRefreshes != 1 {
		t.Fatalf("role refreshed on hot path: %d", store.roleRefreshes)
	}
	server.mu.Lock()
	lease := server.roleLeases[47]
	lease.refreshAt = time.Now().Add(-time.Second)
	server.roleLeases[47] = lease
	server.mu.Unlock()
	if _, err := server.TerminateDeadSessions(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if store.roleRefreshes != 2 {
		t.Fatalf("scheduled role refresh count = %d", store.roleRefreshes)
	}
}
