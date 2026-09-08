package msgq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestSlowRenewalKeepsOtherMailboxesUsable 验证慢续期不阻塞精确下行和角色共享下行。
func TestSlowRenewalKeepsOtherMailboxesUsable(t *testing.T) {
	store := &blockedRenewalStore{entered: make(chan struct{}), release: make(chan struct{})}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 8})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	headers := []SessionHeader{
		{RoleID: 47, SessionID: 11, ConnectionID: 1},
		{RoleID: 47, SessionID: 12, ConnectionID: 2},
		{RoleID: 48, SessionID: 13, ConnectionID: 3},
	}
	for _, header := range headers {
		if _, err := server.Init(ctx, InitRequest{Header: header}); err != nil {
			t.Fatal(err)
		}
	}
	touchDone := make(chan error, 1)
	go func() { touchDone <- server.Touch(ctx, TouchRequest{Header: headers[0]}) }()
	waitMailboxSignal(t, store.entered)
	defer func() {
		close(store.release)
		if err := waitMailboxResult(t, touchDone); err != nil {
			t.Error(err)
		}
	}()

	// 精确投递到同角色的另一邮箱、另一角色邮箱都必须能继续。
	for _, header := range headers[1:] {
		done := make(chan error, 1)
		go func() { _, err := server.PubSession(ctx, header.SessionID, []byte("independent")); done <- err }()
		if err := waitMailboxResult(t, done); err != nil {
			t.Fatal(err)
		}
	}
	first, _ := server.lookup(11)
	peer, _ := server.lookup(12)
	sharedDone := make(chan error, 1)
	// 固定快照顺序，排除 map 遍历顺序对诊断的影响；使用生产共享分发实现。
	go func() {
		result, err := server.broadcastSessions(ctx, []*mailboxSession{first, peer}, []byte("shared"))
		if err == nil && result.Delivered != 2 {
			err = ErrInvalidMailboxResponse
		}
		sharedDone <- err
	}()
	select {
	case err := <-sharedDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		store.release <- struct{}{}
		_ = waitMailboxResult(t, sharedDone)
		t.Fatal("共享下行被续期网络等待阻塞")
	}
}

func TestTerminationDuringRenewalCannotReactivateMailbox(t *testing.T) {
	store := &blockedRenewalStore{entered: make(chan struct{}), release: make(chan struct{})}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 8})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Touch(context.Background(), TouchRequest{Header: header}) }()
	waitMailboxSignal(t, store.entered)
	// 等待同一次续期的调用可以独立取消，不取消已经在途的请求。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := server.Touch(ctx, TouchRequest{Header: header}); !errors.Is(err, context.DeadlineExceeded) {
		close(store.release)
		t.Fatalf("renewal waiter cancellation = %v", err)
	}
	terminated := make(chan error, 1)
	go func() { terminated <- server.TerminateSession(context.Background(), header) }()
	err = waitMailboxResult(t, terminated)
	close(store.release)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitMailboxResult(t, done); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("old renewal = %v", err)
	}
	if server.SessionCount() != 0 {
		t.Fatal("旧续期重新激活了已终止邮箱")
	}
}

type blockedRenewalStore struct {
	testSessionStore
	entered chan struct{}
	release chan struct{}
}

// TestSlowRoleRenewalKeepsDeliveryUsable 明确维护队列等待与业务下行的隔离边界。
func TestSlowRoleRenewalKeepsDeliveryUsable(t *testing.T) {
	store := &blockedRoleRenewalStore{entered: make(chan struct{}), release: make(chan struct{})}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 8})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := int64(1); i <= 2; i++ {
		if _, err := server.Init(ctx, InitRequest{Header: SessionHeader{RoleID: i, SessionID: i, ConnectionID: i}}); err != nil {
			t.Fatal(err)
		}
	}
	// 仅把本地续期时刻置为到期，不改变生产 TTL 或等待完整租期。
	for id, lease := range server.roleLeases {
		lease.refreshAt = time.Now().Add(-time.Second)
		server.roleLeases[id] = lease
	}
	store.block.Store(true)
	done := make(chan error, 1)
	go func() { done <- server.maintainRoles(ctx, time.Now()) }()
	waitMailboxSignal(t, store.entered)
	defer func() {
		close(store.release)
		if err := waitMailboxResult(t, done); err != nil {
			t.Error(err)
		}
		if got := store.calls.Load(); got != 2 {
			t.Errorf("renewed roles = %d, want 2", got)
		}
	}()
	for id := int64(1); id <= 2; id++ {
		published := make(chan error, 1)
		go func() { _, err := server.PubSession(ctx, id, []byte("during-renewal")); published <- err }()
		if err := waitMailboxResult(t, published); err != nil {
			t.Fatal(err)
		}
	}
	if got := store.calls.Load(); got != 1 {
		t.Fatalf("maintenance unexpectedly advanced while first renewal blocked: %d", got)
	}
}

type blockedRoleRenewalStore struct {
	testSessionStore
	block            atomic.Bool
	calls            atomic.Int32
	entered, release chan struct{}
}

func (s *blockedRoleRenewalStore) RefreshRole(ctx context.Context, binding RoleBinding) (bool, error) {
	if s.block.Load() && s.calls.Add(1) == 1 {
		close(s.entered)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-s.release:
		}
	}
	return s.roleTestStore.RefreshRole(ctx, binding)
}

func (s *blockedRenewalStore) Refresh(ctx context.Context, binding SessionBinding) (bool, error) {
	if binding.SessionID == 11 {
		close(s.entered)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-s.release:
		}
	}
	return true, nil
}
