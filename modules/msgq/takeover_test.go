package msgq

import (
	"context"
	"errors"
	"fmt"
	"github.com/ngnl47/tx/modules/discovery"
	"testing"
	"time"
)

func takeoverRequest(sid, order int64) InitRequest {
	return InitRequest{Header: SessionHeader{RoleID: 47, SessionID: sid, ConnectionID: sid}, Admission: &SessionAdmission{Policy: ReplaceExisting, TicketOrder: order, ExpiresAt: time.Now().Add(time.Minute)}}
}
func TestMailboxTakeoverWaitsForOldIngressClosure(t *testing.T) {
	mailbox := admissionServer(t)
	client, _ := NewLocalSessionClient(mailbox)
	entered, allow := make(chan struct{}), make(chan struct{})
	old, err := client.OpenMailboxStream(context.Background(), takeoverRequest(90, 1), func() { close(entered); <-allow })
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := client.OpenMailboxStream(context.Background(), takeoverRequest(20, 2), func() {})
		result <- err
	}()
	<-entered
	select {
	case err := <-result:
		t.Fatalf("closed before old ingress: %v", err)
	default:
	}
	close(allow)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if old.Context().Err() == nil {
		t.Fatal("old session still open")
	}
	_, err = client.OpenMailboxStream(context.Background(), takeoverRequest(100, 1), func() {})
	if !errors.Is(err, ErrSessionAdmission) {
		t.Fatalf("stale ticket: %v", err)
	}
}
func TestMailboxDetachRestoresButLogoutCannotFreshReplay(t *testing.T) {
	mailbox := admissionServer(t)
	client, _ := NewLocalSessionClient(mailbox)
	request := takeoverRequest(8, 3)
	session, err := client.OpenMailboxStream(context.Background(), request, func() {})
	if err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	request.Restore = true
	request.Header.ConnectionID++
	session, err = client.OpenMailboxStream(context.Background(), request, func() {})
	if err != nil {
		t.Fatal(err)
	}
	if err = session.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	request.Restore = false
	request.Header.ConnectionID++
	if _, err = client.OpenMailboxStream(context.Background(), request, func() {}); !errors.Is(err, ErrSessionAdmission) {
		t.Fatalf("replay: %v", err)
	}
}

func TestMailboxRemoteTakeoverAndLogoutUseExistingStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	mailbox := admissionServer(t)
	first := startMailboxTestGRPC(t, mailbox)
	second := startMailboxTestGRPC(t, mailbox)
	entered, allow := make(chan struct{}), make(chan struct{})
	old, err := first.OpenMailboxStream(ctx, takeoverRequest(50, 1), func() { close(entered); <-allow })
	if err != nil {
		t.Fatal(err)
	}
	rejected := takeoverRequest(40, 2)
	rejected.Admission.Policy = RejectExisting
	if _, err = second.OpenMailboxStream(ctx, rejected, func() {}); !errors.Is(err, ErrSessionOccupied) {
		t.Fatalf("nonforce: %v", err)
	}
	current, err := mailbox.InspectSession(ctx, 47)
	if err != nil || current.SessionID != 50 || current.TicketOrder != 1 {
		t.Fatalf("nonforce changed admission: %+v %v", current, err)
	}
	second = startMailboxTestGRPC(t, mailbox)
	admitted := make(chan MailboxStreamSession, 1)
	failure := make(chan error, 1)
	go func() {
		session, err := second.OpenMailboxStream(ctx, takeoverRequest(30, 3), func() { t.Error("current Logout must not revoke its socket") })
		admitted <- session
		failure <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-failure:
		t.Fatalf("admitted before close: %v", err)
	default:
	}
	other := takeoverRequest(60, 1)
	other.Header.RoleID = 111
	peer, err := second.OpenMailboxStream(ctx, other, func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	close(allow)
	session := <-admitted
	if err = <-failure; err != nil {
		t.Fatal(err)
	}
	if old.Context().Err() == nil {
		t.Fatal("old session not revoked")
	}
	if err = session.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	current, err = mailbox.InspectSession(ctx, 47)
	if err != nil || current.SessionID != 0 || current.TicketOrder != 3 || current.Attached {
		t.Fatalf("logout record: %+v %v", current, err)
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMailboxStopWaitsForDetachAndCanRetry(t *testing.T) {
	mailbox := admissionServer(t)
	client, _ := NewLocalSessionClient(mailbox)
	allow := make(chan struct{})
	entered := make(chan struct{})
	_, err := client.OpenMailboxStream(t.Context(), takeoverRequest(1, 1), func() { close(entered); <-allow })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err = mailbox.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop: %v", err)
	}
	<-entered
	if _, found, err := mailbox.store.FindRole(t.Context(), 47); err != nil || !found {
		t.Fatalf("owner released before closure: %v %v", found, err)
	}
	close(allow)
	if err = mailbox.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := mailbox.store.FindRole(t.Context(), 47); err != nil || found {
		t.Fatalf("owner retained: %v %v", found, err)
	}
}

func TestMailboxLostStreamRequiresExplicitDetach(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	mailbox := admissionServer(t)
	oldClient := startMailboxTestGRPC(t, mailbox)
	old, err := oldClient.OpenMailboxStream(ctx, takeoverRequest(1, 1), func() {})
	if err != nil {
		t.Fatal(err)
	}
	oldClient.mailbox.fail(errors.New("transport disconnected"))
	nextClient := startMailboxTestGRPC(t, mailbox)
	if _, err = nextClient.OpenMailboxStream(ctx, takeoverRequest(2, 2), func() {}); err == nil {
		t.Fatal("lost stream was treated as closed ingress")
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	thirdClient := startMailboxTestGRPC(t, mailbox)
	session, err := thirdClient.OpenMailboxStream(ctx, takeoverRequest(3, 3), func() {})
	if err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
}

type failedReadyStore struct {
	admissionTestStore
	failOnce bool
}

func (s *failedReadyStore) CompareAdmission(ctx context.Context, roleID int64, old, next AdmissionRecord) (bool, error) {
	changed, err := s.admissionTestStore.CompareAdmission(ctx, roleID, old, next)
	if changed && next.Ready && next.Attached && s.failOnce {
		s.failOnce = false
		return false, errors.New("lost admission write response")
	}
	return changed, err
}
func TestMailboxFailedAdmissionWriteStillClosesRegisteredEndpoint(t *testing.T) {
	store := &failedReadyStore{failOnce: true}
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	client, _ := NewLocalSessionClient(mailbox)
	closed := false
	if _, err = client.OpenMailboxStream(t.Context(), takeoverRequest(1, 1), func() { closed = true }); err == nil {
		t.Fatal("write failure hidden")
	}
	if closed {
		t.Fatal("Open 失败抢先关闭了返回错误所需的 socket")
	}
	record, err := mailbox.InspectSession(t.Context(), 47)
	if err != nil || record.Attached {
		t.Fatalf("uncleared attachment: %+v %v", record, err)
	}
	if mailbox.SessionCount() != 0 {
		t.Fatal("failed Open retained an unconsumable mailbox")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err = mailbox.Stop(ctx); err != nil {
		t.Fatalf("failed admission blocks stop: %v", err)
	}
}

func TestMailboxOldConnectionCannotTerminateRestoredSession(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprint(remote), func(t *testing.T) {
			mailbox := admissionServer(t)
			var client SessionClient
			if remote {
				client = startMailboxTestGRPC(t, mailbox)
			} else {
				client, _ = NewLocalSessionClient(mailbox)
			}
			request := takeoverRequest(1, 1)
			old, err := client.OpenMailboxStream(t.Context(), request, func() {})
			if err != nil {
				t.Fatal(err)
			}
			if err = old.Close(); err != nil {
				t.Fatal(err)
			}
			request.Restore = true
			request.Header.ConnectionID = 2
			request.Admission.ExpiresAt = time.Now().Add(-time.Hour)
			current, err := client.OpenMailboxStream(t.Context(), request, func() {})
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			if err = old.Terminate(t.Context()); !errors.Is(err, ErrSessionChanged) {
				t.Fatalf("stale Terminate: %v", err)
			}
			if err = current.Renew(t.Context()); err != nil {
				t.Fatal(err)
			}
			if remote {
				stream := client.(*grpcConnection).mailbox
				stream.mu.Lock()
				remaining := len(stream.sessions)
				stream.mu.Unlock()
				if remaining != 1 {
					t.Fatalf("旧退出请求遗留流接入: %d", remaining)
				}
			}
		})
	}
}

// 旧物理连接已经关闭但其流接入尚未收尾时，恢复应交给 MsgQ 仲裁。
func TestMailboxRemoteRestoreBeforeOldClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	mailbox := admissionServer(t)
	client := startMailboxTestGRPC(t, mailbox)
	entered, allow := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-allow:
		default:
			close(allow)
		}
	}()
	request := takeoverRequest(11, 1)
	old, err := client.OpenMailboxStream(ctx, request, func() { close(entered); <-allow })
	if err != nil {
		t.Fatal(err)
	}
	request.Restore = true
	request.Header.ConnectionID = 2
	type result struct {
		session MailboxStreamSession
		err     error
	}
	opened := make(chan result, 1)
	go func() {
		session, err := client.OpenMailboxStream(ctx, request, func() {})
		opened <- result{session, err}
	}()
	select {
	case <-entered:
	case got := <-opened:
		t.Fatalf("恢复未经过旧入口关闭协调: %v", got.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case got := <-opened:
		t.Fatalf("旧入口未关闭就完成恢复: %v", got.err)
	default:
	}
	close(allow)
	var current MailboxStreamSession
	select {
	case got := <-opened:
		if got.err != nil {
			t.Fatal(got.err)
		}
		current = got.session
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer current.Close()
	if err = old.Terminate(ctx); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("旧连接终止影响新接入: %v", err)
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = mailbox.PubSession(ctx, 11, []byte("restored")); err != nil {
		t.Fatal(err)
	}
	message, err := current.Recv()
	if err != nil || string(message.Payload) != "restored" {
		t.Fatalf("旧接入收尾破坏新接入下行: %+v %v", message, err)
	}
	if err = current.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOfflineLogoutDoesNotAllocateOwnerOrDial(t *testing.T) {
	store := &admissionTestStore{records: map[int64]AdmissionRecord{47: {SessionID: 1, TicketOrder: 7, Ready: true, Header: SessionHeader{RoleID: 47, SessionID: 1, ConnectionID: 1}}}}
	directory := discovery.NewServiceDirectory()
	dialer := &rotatingSessionDialer{}
	router, err := NewSessionClientRouter("msgq", directory, store, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	if err = router.TerminateLogicalSession(t.Context(), 47, 1); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.FindRole(t.Context(), 47); err != nil || found {
		t.Fatalf("allocated owner: %v %v", found, err)
	}
	if dialer.calls.Load() != 0 {
		t.Fatal("offline logout dialed a MsgQ")
	}
	record, err := store.LoadAdmission(t.Context(), 47)
	if err != nil || record.SessionID != 0 || record.TicketOrder != 7 {
		t.Fatalf("watermark: %+v %v", record, err)
	}
}

func TestMailboxStopNotifiesOtherRolesBeforeWaitingAndRetainsAllOwnersOnFailure(t *testing.T) {
	mailbox := admissionServer(t)
	client, _ := NewLocalSessionClient(mailbox)
	notified := make(chan int, 2)
	allow := make(chan struct{})
	for i := 1; i <= 2; i++ {
		request := takeoverRequest(int64(i), 1)
		request.Header.RoleID = int64(i)
		_, err := client.OpenMailboxStream(t.Context(), request, func() {
			notified <- i
			if i == 1 {
				<-allow
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- mailbox.Stop(ctx) }()
	seen := map[int]bool{}
	for range 2 {
		select {
		case id := <-notified:
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatal("关闭等待阻止了其他角色收到撤销")
		}
	}
	if !seen[1] || !seen[2] {
		t.Fatal(seen)
	}
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	for id := int64(1); id <= 2; id++ {
		if _, found, err := mailbox.store.FindRole(t.Context(), id); err != nil || !found {
			t.Fatalf("partial stop released owner %d: %v %v", id, found, err)
		}
	}
	close(allow)
	if err := mailbox.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFailedRestoreAdmissionKeepsUnacknowledgedMailbox(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprint(remote), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			store := &failedReadyStore{}
			mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
			if err != nil {
				t.Fatal(err)
			}
			var client SessionClient
			if remote {
				client = startMailboxTestGRPC(t, mailbox)
			} else {
				client, _ = NewLocalSessionClient(mailbox)
			}
			request := takeoverRequest(1, 1)
			original, err := client.OpenMailboxStream(ctx, request, func() {})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = mailbox.PubSession(ctx, 1, []byte("keep across failed restore")); err != nil {
				t.Fatal(err)
			}
			if err = original.Close(); err != nil {
				t.Fatal(err)
			}
			store.failOnce = true
			request.Restore = true
			request.Header.ConnectionID = 2
			if _, err = client.OpenMailboxStream(ctx, request, func() {}); err == nil {
				t.Fatal("restore CAS failure hidden")
			}

			if mailbox.SessionCount() != 1 {
				t.Fatal("failed restore deleted original mailbox")
			}
			request.Header.ConnectionID = 3
			current, err := client.OpenMailboxStream(ctx, request, func() {})
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			message, err := current.Recv()
			if err != nil || message.Seq != 1 || string(message.Payload) != "keep across failed restore" {
				t.Fatalf("restored downlink: %+v %v", message, err)
			}
		})
	}
}

func TestConcurrentNonForceAdmissionKeepsOneSession(t *testing.T) {
	mailbox := admissionServer(t)
	client, err := NewLocalSessionClient(mailbox)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		sid int64
		err error
	}
	barrier := make(chan struct{})
	results := make(chan outcome, 2)
	for sid := int64(1); sid <= 2; sid++ {
		go func() {
			<-barrier
			request := takeoverRequest(sid, sid)
			request.Admission.Policy = RejectExisting
			_, err := client.OpenMailboxStream(t.Context(), request, func() {})
			results <- outcome{sid: sid, err: err}
		}()
	}
	close(barrier)
	var winner int64
	for range 2 {
		result := <-results
		if result.err == nil {
			if winner != 0 {
				t.Fatal("非强制并发接入同时成功")
			}
			winner = result.sid
		} else if !errors.Is(result.err, ErrSessionOccupied) && !errors.Is(result.err, ErrSessionAdmission) {
			t.Fatal(result.err)
		}
	}
	current, err := mailbox.InspectSession(t.Context(), 47)
	if err != nil || winner == 0 || current.SessionID != winner || current.TicketOrder != winner || !current.Ready || !current.Attached || mailbox.SessionCount() != 1 {
		t.Fatalf("非强制接入结果 winner=%d admission=%+v err=%v", winner, current, err)
	}
	if err := mailbox.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}
