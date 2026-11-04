package msgq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type blockedRestoreAdmissionStore struct {
	admissionTestStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockedRestoreAdmissionStore) CompareAdmission(ctx context.Context, roleID int64, old, next AdmissionRecord) (bool, error) {
	if next.Header.ConnectionID == 2 && next.Attached {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return s.admissionTestStore.CompareAdmission(ctx, roleID, old, next)
}

func TestMailboxOldTerminateWaitsForRestoreHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	store := &blockedRestoreAdmissionStore{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	defer release()
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	client, _ := NewLocalSessionClient(mailbox)
	request := takeoverRequest(1, 1)
	old, err := client.OpenMailboxStream(ctx, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	request.Restore = true
	request.Header.ConnectionID = 2
	type openResult struct {
		session MailboxStreamSession
		err     error
	}
	opened := make(chan openResult, 1)
	go func() {
		session, err := client.OpenMailboxStream(ctx, request, nil)
		opened <- openResult{session: session, err: err}
	}()
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("恢复未进入许可更新")
	}
	terminated := make(chan error, 1)
	go func() { terminated <- old.Terminate(ctx) }()
	// 恢复持有角色准入时，让旧退出进入同一队列，随后才公布新 Header。
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		mailbox.admissionMu.Lock()
		users := mailbox.admissions[request.Header.RoleID].users
		mailbox.admissionMu.Unlock()
		if users == 2 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("旧退出未进入角色准入队列")
		}
	}
	release()
	current := <-opened
	if current.err != nil {
		t.Fatal(current.err)
	}
	defer current.session.Close()
	if err = <-terminated; !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("旧退出应拒绝新连接，实际为 %v", err)
	}
	record, err := mailbox.InspectSession(ctx, request.Header.RoleID)
	if err != nil || record.Header != request.Header || !record.Attached {
		t.Fatalf("旧退出改变了恢复后的许可: %+v, %v", record, err)
	}
	if _, err = mailbox.PubSession(ctx, request.Header.SessionID, []byte("restored")); err != nil {
		t.Fatal(err)
	}
	message, err := current.session.Recv()
	if err != nil || string(message.Payload) != "restored" {
		t.Fatalf("恢复后的下行不可用: %+v, %v", message, err)
	}
}
