package msgq

import (
	"context"
	"testing"
	"time"
)

// TestSlowRenewalKeepsOtherMailboxesUsable 验证慢续期不占用其他邮箱锁。
// 共享分发的等待时间仅记录为诊断，不把当前阻塞行为固定为接口契约。
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
	started := time.Now()
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
		t.Log("共享分发在续期完成前已返回")
	case <-time.After(20 * time.Millisecond):
		t.Logf("续期受阻时，共享分发至少等待 %s；其他邮箱精确下行已完成", time.Since(started))
		// 先释放存储再等待分发，不遗留测试 goroutine。
		store.release <- struct{}{}
		if err := waitMailboxResult(t, sharedDone); err != nil {
			t.Fatal(err)
		}
	}
}

type blockedRenewalStore struct {
	testSessionStore
	entered chan struct{}
	release chan struct{}
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
