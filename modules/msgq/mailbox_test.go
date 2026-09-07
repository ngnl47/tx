package msgq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	txlog "github.com/ngnl47/tx/log"
)

func TestMailboxLogsStateTransitionsAtDebug(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.Rec(context.Background(), RecRequest{Header: header, Seq: 1}); err != nil {
		t.Fatalf("rec: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("downMsg")); err != nil {
		t.Fatalf("pub: %v", err)
	}
	downMsg, err := server.Fetch(context.Background(), FetchRequest{Header: header})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if err := server.Confirm(context.Background(), ConfirmRequest{Header: header, Ack: downMsg.Seq}); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	records := decodeJSONLogs(t, output.String())
	for _, message := range []string{
		"msgq session initialized",
		"msgq upMsg accepted",
		"msgq downMsg published",
		"msgq downMsg confirmed",
	} {
		record := findJSONLog(t, records, message)
		if record["level"] != "DEBUG" {
			t.Fatalf("%q level = %v, want DEBUG", message, record["level"])
		}
		if record["roleID"] != float64(header.RoleID) {
			t.Fatalf("%q roleID = %v, want %d", message, record["roleID"], header.RoleID)
		}
		if record["sessionID"] != float64(header.SessionID) || record["connectionID"] != float64(header.ConnectionID) {
			t.Fatalf("%q session identity = %v/%v, want %d/%d", message, record["sessionID"], record["connectionID"], header.SessionID, header.ConnectionID)
		}
	}
}

func TestMailboxRestoreLogCorrelatesPreviousConnection(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: first}); err != nil {
		t.Fatalf("init first connection: %v", err)
	}
	restored := SessionHeader{RoleID: first.RoleID, SessionID: first.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: restored, Restore: true}); err != nil {
		t.Fatalf("restore connection: %v", err)
	}

	for _, record := range decodeJSONLogs(t, output.String()) {
		if record["msg"] != "msgq session initialized" || record["connectionID"] != float64(restored.ConnectionID) {
			continue
		}
		if record["roleID"] != float64(restored.RoleID) || record["sessionID"] != float64(restored.SessionID) || record["previousConnectionID"] != float64(first.ConnectionID) || record["restore"] != true {
			t.Fatalf("restore log = %#v", record)
		}
		return
	}
	t.Fatalf("logs = %s, want restored session identity", output.String())
}

func TestMailboxRejectsStaleConnectionWithCurrentIdentityLog(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	stale := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: stale}); err != nil {
		t.Fatalf("init first connection: %v", err)
	}
	current := SessionHeader{RoleID: stale.RoleID, SessionID: stale.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: current, Restore: true}); err != nil {
		t.Fatalf("restore connection: %v", err)
	}
	if err := server.Touch(context.Background(), TouchRequest{Header: stale}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("touch stale connection error = %v, want %v", err, ErrSessionChanged)
	}
	if err := server.TerminateSession(context.Background(), stale); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("close stale connection error = %v, want %v", err, ErrSessionChanged)
	}

	rejected := 0
	for _, record := range decodeJSONLogs(t, output.String()) {
		if record["msg"] != "msgq stale connection rejected" {
			continue
		}
		rejected++
		if record["roleID"] != float64(stale.RoleID) || record["sessionID"] != float64(stale.SessionID) || record["connectionID"] != float64(stale.ConnectionID) || record["currentSessionID"] != float64(current.SessionID) || record["currentConnectionID"] != float64(current.ConnectionID) {
			t.Fatalf("stale connection log = %#v", record)
		}
	}
	if rejected != 2 {
		t.Fatalf("stale connection rejection logs = %d, want 2; logs = %s", rejected, output.String())
	}
}

func TestDefaultOfflineTimeoutRetainsWeakNetworkRecoveryWindow(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	base := time.Now()
	if removed, err := server.TerminateDeadSessions(context.Background(), base.Add(61*time.Second)); err != nil || removed != 0 {
		t.Fatalf("reap after 61s = %d, %v, want retained session", removed, err)
	}
	if removed, err := server.TerminateDeadSessions(context.Background(), base.Add(181*time.Second)); err != nil || removed != 1 {
		t.Fatalf("reap after 181s = %d, %v, want one expired session", removed, err)
	}
}

func TestTerminateSessionPreservesPrimaryProjection(t *testing.T) {
	store := newStagedSessionStore()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}

	if err := server.TerminateSession(context.Background(), header); err != nil {
		t.Fatalf("terminate session: %v", err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
	if _, found, err := store.Find(context.Background(), header.SessionID); err != nil || found {
		t.Fatalf("exact binding after terminate = %t, %v", found, err)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), header.RoleID); err != nil || !found || sessionID != header.SessionID {
		t.Fatalf("primary after terminate = %d, %t, %v, want %d", sessionID, found, err, header.SessionID)
	}
}

func TestTerminateDeadSessionsPreservesPrimaryProjection(t *testing.T) {
	store := newStagedSessionStore()
	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              store,
		MaxBacklogMessages: 4,
		OfflineTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}

	terminated, err := server.TerminateDeadSessions(context.Background(), time.Now().Add(2*time.Second))
	if err != nil || terminated != 1 {
		t.Fatalf("terminate dead sessions = %d, %v, want 1", terminated, err)
	}
	if _, found, err := store.Find(context.Background(), header.SessionID); err != nil || found {
		t.Fatalf("exact binding after dead termination = %t, %v", found, err)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), header.RoleID); err != nil || !found || sessionID != header.SessionID {
		t.Fatalf("primary after dead termination = %d, %t, %v, want %d", sessionID, found, err, header.SessionID)
	}
}

func TestMailboxDeadTerminationLogCorrelatesSessionAndConnection(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              &testSessionStore{},
		MaxBacklogMessages: 4,
		OfflineTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if removed, err := server.TerminateDeadSessions(context.Background(), time.Now().Add(2*time.Second)); err != nil || removed != 1 {
		t.Fatalf("terminate dead sessions = %d, %v", removed, err)
	}
	record := findJSONLog(t, decodeJSONLogs(t, output.String()), "msgq dead session terminated")
	if record["roleID"] != float64(header.RoleID) || record["sessionID"] != float64(header.SessionID) || record["connectionID"] != float64(header.ConnectionID) {
		t.Fatalf("dead termination log = %#v", record)
	}
}

func TestMailboxOverflowLogCorrelatesSessionAndConnection(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 1})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("first")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("overflow")); !errors.Is(err, ErrDownMsgOverflow) {
		t.Fatalf("publish overflow error = %v, want %v", err, ErrDownMsgOverflow)
	}
	record := findJSONLog(t, decodeJSONLogs(t, output.String()), "msgq session evicted")
	if record["roleID"] != float64(header.RoleID) || record["sessionID"] != float64(header.SessionID) || record["connectionID"] != float64(header.ConnectionID) || record["reason"] != "backlog_overflow" {
		t.Fatalf("eviction log = %#v", record)
	}
}

func TestBroadcastTopicSharesImmutablePayload(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	second := SessionHeader{RoleID: 2, SessionID: 2, ConnectionID: 2}
	for _, header := range []SessionHeader{first, second} {
		if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
			t.Fatalf("init %#v: %v", header, err)
		}
		if err := server.JoinTopics(context.Background(), TopicRequest{Header: header, Topics: []string{"guild:1"}}); err != nil {
			t.Fatalf("join topic: %v", err)
		}
	}
	payload := []byte("already-encoded-bon")
	if _, err := server.BroadcastTopic(context.Background(), BroadcastTopicRequest{Topic: "guild:1", Payload: payload}); err != nil {
		t.Fatalf("broadcast topic: %v", err)
	}
	firstSession, _ := server.lookup(first.SessionID)
	secondSession, _ := server.lookup(second.SessionID)
	firstSession.mu.Lock()
	defer firstSession.mu.Unlock()
	secondSession.mu.Lock()
	defer secondSession.mu.Unlock()
	firstPayload := firstSession.downMsgs.entries[firstSession.downMsgs.head].payload
	secondPayload := secondSession.downMsgs.entries[secondSession.downMsgs.head].payload
	if &firstPayload[0] != &payload[0] || &secondPayload[0] != &payload[0] {
		t.Fatal("broadcast payload was copied instead of shared")
	}
}

func TestDownMsgQueueReleasesAcknowledgedStorage(t *testing.T) {
	var queue downMsgQueue
	payload := []byte("payload")
	for range 128 {
		queue.enqueue(payload)
	}
	if &queue.entries[0].payload[0] != &payload[0] {
		t.Fatal("queue payload was copied")
	}
	entries := queue.dequeueSome(96)
	if len(entries) != 96 || queue.sentSeq != 96 {
		t.Fatalf("dequeue = %d entries, sent seq = %d", len(entries), queue.sentSeq)
	}
	if err := queue.confirm(96); err != nil {
		t.Fatalf("confirm first batch: %v", err)
	}
	if queue.head != 0 || len(queue.entries) != 32 || cap(queue.entries) > 64 || queue.sent != 0 {
		t.Fatalf("queue was not compacted: head=%d len=%d cap=%d sent=%d", queue.head, len(queue.entries), cap(queue.entries), queue.sent)
	}
	entries = queue.dequeueSome(0)
	if len(entries) != 32 || queue.sentSeq != 128 {
		t.Fatalf("dequeue remainder = %d entries, sent seq = %d", len(entries), queue.sentSeq)
	}
	if err := queue.confirm(128); err != nil {
		t.Fatalf("confirm all: %v", err)
	}
	if queue.entries != nil || queue.head != 0 || queue.sent != 0 || queue.bytes != 0 {
		t.Fatalf("queue retained acknowledged storage: %#v", queue)
	}
}

func TestSlowSessionWithoutAcknowledgementHitsByteLimit(t *testing.T) {
	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              &testSessionStore{},
		MaxBacklogMessages: 4,
		MaxBacklogBytes:    8,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for _, message := range [][]byte{[]byte("one!"), []byte("two!")} {
		if _, err := server.PubSession(context.Background(), header.SessionID, message); err != nil {
			t.Fatalf("publish %q: %v", message, err)
		}
		if _, err := server.Fetch(context.Background(), FetchRequest{Header: header}); err != nil {
			t.Fatalf("fetch %q: %v", message, err)
		}
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("more")); !errors.Is(err, ErrDownMsgOverflow) {
		t.Fatalf("publish after unacknowledged backlog error = %v, want %v", err, ErrDownMsgOverflow)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after overflow = %d, want 0", server.SessionCount())
	}
}

func TestAcknowledgementReleasesDownMsgByteBudget(t *testing.T) {
	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              &testSessionStore{},
		MaxBacklogMessages: 2,
		MaxBacklogBytes:    4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("full")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	message, err := server.Fetch(context.Background(), FetchRequest{Header: header})
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if err := server.Confirm(context.Background(), ConfirmRequest{Header: header, Ack: message.Seq}); err != nil {
		t.Fatalf("confirm first: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("next")); err != nil {
		t.Fatalf("publish after acknowledgement: %v", err)
	}
}

func TestInitWithoutRestoreAlwaysCreatesFreshSession(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: first}); err != nil {
		t.Fatalf("init first: %v", err)
	}
	if _, err := server.Rec(context.Background(), RecRequest{Header: first, Seq: 1}); err != nil {
		t.Fatalf("rec first: %v", err)
	}
	if _, err := server.PubSession(context.Background(), first.SessionID, []byte("pending")); err != nil {
		t.Fatalf("publish first: %v", err)
	}

	second := SessionHeader{RoleID: 1, SessionID: 8, ConnectionID: 2}
	result, err := server.Init(context.Background(), InitRequest{Header: second})
	if err != nil {
		t.Fatalf("init fresh: %v", err)
	}
	if result.UpMsgSeq != 0 {
		t.Fatalf("fresh upMsg seq = %d, want 0", result.UpMsgSeq)
	}
	session, found := server.lookup(second.SessionID)
	if !found {
		t.Fatal("fresh session is missing")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.connectionID != second.ConnectionID || session.upMsgSeq != 0 || session.downMsgs.len() != 0 {
		t.Fatalf("fresh session retained old state: %#v", session)
	}
}

func TestInitRestoreRequiresExistingMatchingSession(t *testing.T) {
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	missing := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: missing, Restore: true}); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("restore missing error = %v, want %v", err, ErrSessionOffline)
	}
	if _, found := server.lookup(missing.SessionID); found {
		t.Fatal("restore missing installed an empty session")
	}

	current := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 2}
	if _, err := server.Init(context.Background(), InitRequest{Header: current}); err != nil {
		t.Fatalf("init current: %v", err)
	}
	mismatch := SessionHeader{RoleID: 1, SessionID: 8, ConnectionID: 3}
	if _, err := server.Init(context.Background(), InitRequest{Header: mismatch, Restore: true}); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("restore mismatched error = %v, want %v", err, ErrSessionOffline)
	}
}

func TestSlowSessionExpiresDespiteClientActivity(t *testing.T) {
	server, err := NewServer(ServerOptions{
		InstanceID:         "msgq-1",
		Store:              &testSessionStore{},
		MaxBacklogMessages: 2,
		OfflineTimeout:     time.Minute,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 1, SessionID: 1, ConnectionID: 1}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("pending")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := server.Fetch(context.Background(), FetchRequest{Header: header}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	session, found := server.lookup(header.SessionID)
	if !found {
		t.Fatal("session is missing")
	}
	session.mu.Lock()
	session.lastActivity = time.Now()
	session.downMsgs.entries[session.downMsgs.head].createdAt = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	removed, err := server.TerminateDeadSessions(context.Background(), time.Now())
	if err != nil || removed != 1 {
		t.Fatalf("reap = %d, %v", removed, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after stale unacknowledged message = %d, want 0", server.SessionCount())
	}
}

func TestMailboxTouchFirstSerializesRefreshWithRestore(t *testing.T) {
	store := newStagedSessionStore()
	store.refreshEntered = make(chan struct{})
	store.continueRefresh = make(chan struct{})
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	newHeader := SessionHeader{RoleID: oldHeader.RoleID, SessionID: oldHeader.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: oldHeader}); err != nil {
		t.Fatalf("init old connection: %v", err)
	}
	touchDone := make(chan error, 1)
	touchStarted := make(chan struct{})
	go func() {
		close(touchStarted)
		touchDone <- server.Touch(context.Background(), TouchRequest{Header: oldHeader})
	}()
	waitMailboxSignal(t, touchStarted)
	waitMailboxSignal(t, store.refreshEntered)

	restoreDone := make(chan error, 1)
	restoreAtSessionLock := make(chan struct{})
	server.beforeRestoreSessionLock = func() {
		close(restoreAtSessionLock)
	}
	go func() {
		_, err := server.Init(context.Background(), InitRequest{Header: newHeader, Restore: true})
		restoreDone <- err
	}()
	waitMailboxSignal(t, restoreAtSessionLock)
	close(store.continueRefresh)

	if err := waitMailboxResult(t, touchDone); err != nil {
		t.Fatalf("touch old connection: %v", err)
	}
	if err := waitMailboxResult(t, restoreDone); err != nil {
		t.Fatalf("restore new connection: %v", err)
	}
	if server.SessionCount() != 1 {
		t.Fatalf("session count = %d, want restored session", server.SessionCount())
	}
	if err := server.Touch(context.Background(), TouchRequest{Header: newHeader}); err != nil {
		t.Fatalf("touch restored connection: %v", err)
	}
}

func TestMailboxSessionActivityCoalescesStoreRefresh(t *testing.T) {
	store := newLeaseCountingSessionStore(time.Hour)
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}
	session, found := server.lookup(header.SessionID)
	if !found {
		t.Fatal("session is missing")
	}
	if err := server.Touch(context.Background(), TouchRequest{Header: header}); err != nil {
		t.Fatalf("touch before refresh threshold: %v", err)
	}
	if store.refreshCalls != 0 || store.refreshPrimaryCalls != 0 {
		t.Fatalf("refresh calls before threshold = exact %d, primary %d, want 0/0", store.refreshCalls, store.refreshPrimaryCalls)
	}
	session.mu.Lock()
	session.nextLeaseRefresh = time.Now().Add(-time.Second)
	session.mu.Unlock()

	if err := server.Touch(context.Background(), TouchRequest{Header: header}); err != nil {
		t.Fatalf("first touch: %v", err)
	}
	if err := server.Touch(context.Background(), TouchRequest{Header: header}); err != nil {
		t.Fatalf("second touch: %v", err)
	}
	if store.refreshCalls != 1 || store.refreshPrimaryCalls != 1 {
		t.Fatalf("refresh calls = exact %d, primary %d, want 1/1", store.refreshCalls, store.refreshPrimaryCalls)
	}
}

func TestMailboxRestoreFirstRejectsOldTouchWithoutRemovingNewConnection(t *testing.T) {
	store := newStagedSessionStore()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	newHeader := SessionHeader{RoleID: oldHeader.RoleID, SessionID: oldHeader.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: oldHeader}); err != nil {
		t.Fatalf("init old connection: %v", err)
	}
	restoreStarted := make(chan struct{})
	continueRestore := make(chan struct{})
	restoreDone := make(chan error, 1)
	go func() {
		close(restoreStarted)
		<-continueRestore
		_, err := server.Init(context.Background(), InitRequest{Header: newHeader, Restore: true})
		restoreDone <- err
	}()
	waitMailboxSignal(t, restoreStarted)
	close(continueRestore)
	if err := waitMailboxResult(t, restoreDone); err != nil {
		t.Fatalf("restore new connection: %v", err)
	}
	if err := server.Touch(context.Background(), TouchRequest{Header: oldHeader}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("touch old connection error = %v, want %v", err, ErrSessionChanged)
	}
	if _, err := server.PubSession(context.Background(), newHeader.SessionID, []byte("new-connection")); err != nil {
		t.Fatalf("publish new connection: %v", err)
	}
	if err := server.Touch(context.Background(), TouchRequest{Header: newHeader}); err != nil {
		t.Fatalf("touch new connection: %v", err)
	}
	message, err := server.Fetch(context.Background(), FetchRequest{Header: newHeader})
	if err != nil || string(message.Payload) != "new-connection" {
		t.Fatalf("fetch new connection = %#v, %v", message, err)
	}
	if err := server.Confirm(context.Background(), ConfirmRequest{Header: newHeader, Ack: message.Seq}); err != nil {
		t.Fatalf("confirm new connection: %v", err)
	}
}

func TestMailboxFreshReservationMakesPubWaitForCreate(t *testing.T) {
	store := newStagedSessionStore()
	store.createEntered = make(chan struct{})
	store.continueCreate = make(chan struct{})
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	initDone := make(chan error, 1)
	go func() {
		_, err := server.Init(context.Background(), InitRequest{Header: header})
		initDone <- err
	}()
	waitMailboxSignal(t, store.createEntered)
	session, found := server.lookup(header.SessionID)
	if !found {
		t.Fatal("fresh reservation is missing while Create is blocked")
	}
	if session.mu.TryLock() {
		session.mu.Unlock()
		t.Fatal("fresh reservation was published without holding session.mu")
	}
	pubStarted := make(chan struct{})
	pubDone := make(chan error, 1)
	go func() {
		close(pubStarted)
		_, err := server.PubSession(context.Background(), header.SessionID, []byte("during-init"))
		pubDone <- err
	}()
	waitMailboxSignal(t, pubStarted)
	close(store.continueCreate)
	if err := waitMailboxResult(t, initDone); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := waitMailboxResult(t, pubDone); err != nil {
		t.Fatalf("publish during init: %v", err)
	}
	message, err := server.Fetch(context.Background(), FetchRequest{Header: header})
	if err != nil || string(message.Payload) != "during-init" {
		t.Fatalf("fetch = %#v, %v", message, err)
	}
}

func TestMailboxFreshRejectsLocalSessionWhenStoreBindingIsMissing(t *testing.T) {
	store := newStagedSessionStore()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("kept")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := server.JoinTopics(context.Background(), TopicRequest{Header: header, Topics: []string{"guild:1"}}); err != nil {
		t.Fatalf("join topic: %v", err)
	}
	original, _ := server.lookup(header.SessionID)
	store.dropBinding()
	conflict := SessionHeader{RoleID: 470002, SessionID: header.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: conflict}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("fresh conflict error = %v, want %v", err, ErrSessionChanged)
	}
	current, found := server.lookup(header.SessionID)
	if !found || current != original {
		t.Fatalf("local session = %#v, %t, want original %#v", current, found, original)
	}
	current.mu.Lock()
	if !current.active || current.downMsgs.len() != 1 {
		current.mu.Unlock()
		t.Fatalf("original session state changed: active=%t pending=%d", current.active, current.downMsgs.len())
	}
	if _, found := current.topics["guild:1"]; !found {
		current.mu.Unlock()
		t.Fatal("original topic was removed")
	}
	current.mu.Unlock()
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("still-addressable")); err != nil {
		t.Fatalf("publish original after conflict: %v", err)
	}
	if store.createCalls != 1 {
		t.Fatalf("Create calls = %d, want no Redis Create for local conflict", store.createCalls)
	}
}

func TestMailboxPrimaryBindFailureRollsBackExactBinding(t *testing.T) {
	exactErr := errors.New("exact rollback failed")
	primaryErr := errors.New("primary rollback failed")
	store := &primaryFailureSessionStore{
		err:          errors.New("primary unavailable"),
		exactErr:     exactErr,
		primaryErr:   primaryErr,
		exactRemoved: false,
	}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header, Primary: true}); !errors.Is(err, store.err) {
		t.Fatalf("init error = %v, want %v", err, store.err)
	} else if !errors.Is(err, exactErr) || !errors.Is(err, primaryErr) || !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("init rollback error = %v, want exact/primary errors and %v", err, ErrSessionChanged)
	}
	want := SessionBinding{RoleID: header.RoleID, SessionID: header.SessionID, ConnectionID: header.ConnectionID, InstanceID: "msgq-1"}
	if store.created != want || store.unbound != want || store.primaryUnboundRoleID != header.RoleID || store.primaryUnboundSessionID != header.SessionID {
		t.Fatalf("rollback = created %#v, unbound %#v, primary %d/%d, want %#v", store.created, store.unbound, store.primaryUnboundRoleID, store.primaryUnboundSessionID, want)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
}

func TestMailboxPrimaryBindFailureCompensatesAfterRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelingPrimarySessionStore{stagedSessionStore: newStagedSessionStore(), cancel: cancel}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(ctx, InitRequest{Header: header, Primary: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("init error = %v, want %v", err, context.Canceled)
	}
	if binding, found, err := store.Find(context.Background(), header.SessionID); err != nil || found {
		t.Fatalf("exact binding after compensation = %#v, %t, %v", binding, found, err)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), header.RoleID); err != nil || found {
		t.Fatalf("primary after compensation = %d, %t, %v", sessionID, found, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
}

func TestMailboxPrimaryRestoreMissingExactRemovesDanglingState(t *testing.T) {
	store := newStagedSessionStore()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: oldHeader, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}
	store.dropBinding()
	newHeader := SessionHeader{RoleID: oldHeader.RoleID, SessionID: oldHeader.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: newHeader, Restore: true, Primary: true}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("restore error = %v, want %v", err, ErrSessionChanged)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), oldHeader.RoleID); err != nil || found {
		t.Fatalf("dangling primary = %d, %t, %v", sessionID, found, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
	if _, err := server.PubSession(context.Background(), oldHeader.SessionID, []byte("stale")); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("publish removed session error = %v, want %v", err, ErrSessionOffline)
	}
}

func TestMailboxPrimaryRestorePreservesReplacementPrimary(t *testing.T) {
	store := newStagedSessionStore()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: oldHeader, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}
	const replacementSessionID = 100
	if err := store.BindPrimary(context.Background(), oldHeader.RoleID, replacementSessionID); err != nil {
		t.Fatalf("replace primary: %v", err)
	}
	newHeader := SessionHeader{RoleID: oldHeader.RoleID, SessionID: oldHeader.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: newHeader, Restore: true, Primary: true}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("restore error = %v, want %v", err, ErrSessionChanged)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), oldHeader.RoleID); err != nil || !found || sessionID != replacementSessionID {
		t.Fatalf("replacement primary = %d, %t, %v, want %d", sessionID, found, err, replacementSessionID)
	}
	if binding, found, err := store.Find(context.Background(), oldHeader.SessionID); err != nil || found {
		t.Fatalf("old exact binding after failed restore = %#v, %t, %v", binding, found, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
}

func TestMailboxPrimaryRestoreDoesNotDeleteChangedExactBinding(t *testing.T) {
	store := newStagedSessionStore()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: oldHeader, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}
	changed := SessionBinding{RoleID: 470002, SessionID: oldHeader.SessionID, ConnectionID: 17, InstanceID: "msgq-2"}
	store.setBinding(changed)
	newHeader := SessionHeader{RoleID: oldHeader.RoleID, SessionID: oldHeader.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: newHeader, Restore: true, Primary: true}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("restore error = %v, want %v", err, ErrSessionChanged)
	}
	if binding, found, err := store.Find(context.Background(), oldHeader.SessionID); err != nil || !found || binding != changed {
		t.Fatalf("changed exact binding = %#v, %t, %v, want %#v", binding, found, err, changed)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), oldHeader.RoleID); err != nil || found {
		t.Fatalf("stale primary = %d, %t, %v", sessionID, found, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
}

func TestMailboxPrimaryRestoreRefreshErrorCompensatesReboundState(t *testing.T) {
	refreshErr := errors.New("primary refresh failed")
	store := &refreshPrimaryErrorSessionStore{stagedSessionStore: newStagedSessionStore(), err: refreshErr}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: oldHeader, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}
	newHeader := SessionHeader{RoleID: oldHeader.RoleID, SessionID: oldHeader.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), InitRequest{Header: newHeader, Restore: true, Primary: true}); !errors.Is(err, refreshErr) {
		t.Fatalf("restore error = %v, want %v", err, refreshErr)
	}
	if binding, found, err := store.Find(context.Background(), oldHeader.SessionID); err != nil || found {
		t.Fatalf("exact binding after compensation = %#v, %t, %v", binding, found, err)
	}
	if sessionID, found, err := store.FindPrimary(context.Background(), oldHeader.RoleID); err != nil || found {
		t.Fatalf("primary after compensation = %d, %t, %v", sessionID, found, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
}

func TestMailboxTouchPrimaryUnbindErrorKeepsSessionChangedIdentity(t *testing.T) {
	storeErr := errors.New("exact unbind failed")
	store := &touchPrimaryUnbindFailureStore{stagedSessionStore: newStagedSessionStore(), err: storeErr}
	server, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), InitRequest{Header: header, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}
	if err := server.Touch(context.Background(), TouchRequest{Header: header}); !errors.Is(err, ErrSessionChanged) || !errors.Is(err, storeErr) {
		t.Fatalf("touch error = %v, want %v and %v", err, ErrSessionChanged, storeErr)
	}
}

type primaryFailureSessionStore struct {
	testSessionStore
	err                     error
	exactErr                error
	primaryErr              error
	exactRemoved            bool
	created                 SessionBinding
	unbound                 SessionBinding
	primaryUnboundRoleID    int64
	primaryUnboundSessionID int64
}

func (s *primaryFailureSessionStore) Create(_ context.Context, binding SessionBinding) (bool, error) {
	s.created = binding
	return true, nil
}

func (s *primaryFailureSessionStore) BindPrimary(context.Context, int64, int64) error {
	return s.err
}

func (s *primaryFailureSessionStore) Unbind(_ context.Context, binding SessionBinding) (bool, error) {
	s.unbound = binding
	return s.exactRemoved, s.exactErr
}

func (s *primaryFailureSessionStore) UnbindPrimary(_ context.Context, roleID, sessionID int64) (bool, error) {
	s.primaryUnboundRoleID = roleID
	s.primaryUnboundSessionID = sessionID
	return false, s.primaryErr
}

type cancelingPrimarySessionStore struct {
	*stagedSessionStore
	cancel context.CancelFunc
}

func (s *cancelingPrimarySessionStore) BindPrimary(ctx context.Context, roleID, sessionID int64) error {
	if err := s.stagedSessionStore.BindPrimary(context.Background(), roleID, sessionID); err != nil {
		return err
	}
	s.cancel()
	return ctx.Err()
}

func (s *cancelingPrimarySessionStore) Unbind(ctx context.Context, binding SessionBinding) (bool, error) {
	if err := validCompensationContext(ctx); err != nil {
		return false, err
	}
	return s.stagedSessionStore.Unbind(ctx, binding)
}

func (s *cancelingPrimarySessionStore) UnbindPrimary(ctx context.Context, roleID, sessionID int64) (bool, error) {
	if err := validCompensationContext(ctx); err != nil {
		return false, err
	}
	return s.stagedSessionStore.UnbindPrimary(ctx, roleID, sessionID)
}

func validCompensationContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, found := ctx.Deadline(); !found {
		return errors.New("compensation context has no deadline")
	}
	return nil
}

type refreshPrimaryErrorSessionStore struct {
	*stagedSessionStore
	err error
}

func (s *refreshPrimaryErrorSessionStore) RefreshPrimary(context.Context, int64, int64) (bool, error) {
	return false, s.err
}

type stagedSessionStore struct {
	roleTestStore
	mu              sync.Mutex
	binding         SessionBinding
	found           bool
	primary         map[int64]int64
	createCalls     int
	createEntered   chan struct{}
	continueCreate  chan struct{}
	refreshEntered  chan struct{}
	continueRefresh chan struct{}
	createOnce      sync.Once
	refreshOnce     sync.Once
}

type leaseCountingSessionStore struct {
	*stagedSessionStore
	interval            time.Duration
	refreshCalls        int
	refreshPrimaryCalls int
}

func newLeaseCountingSessionStore(interval time.Duration) *leaseCountingSessionStore {
	return &leaseCountingSessionStore{stagedSessionStore: newStagedSessionStore(), interval: interval}
}

func (s *leaseCountingSessionStore) leaseRefreshInterval() time.Duration {
	return s.interval
}

func (s *leaseCountingSessionStore) Refresh(ctx context.Context, binding SessionBinding) (bool, error) {
	s.refreshCalls++
	return s.stagedSessionStore.Refresh(ctx, binding)
}

func (s *leaseCountingSessionStore) RefreshPrimary(ctx context.Context, roleID, sessionID int64) (bool, error) {
	s.refreshPrimaryCalls++
	return s.stagedSessionStore.RefreshPrimary(ctx, roleID, sessionID)
}

func newStagedSessionStore() *stagedSessionStore {
	return &stagedSessionStore{primary: make(map[int64]int64)}
}

func (s *stagedSessionStore) Create(_ context.Context, binding SessionBinding) (bool, error) {
	s.mu.Lock()
	s.createCalls++
	s.mu.Unlock()
	if s.createEntered != nil {
		s.createOnce.Do(func() { close(s.createEntered) })
		<-s.continueCreate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.found {
		return false, nil
	}
	s.binding = binding
	s.found = true
	return true, nil
}

func (s *stagedSessionStore) Rebind(_ context.Context, expected, replacement SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.found || s.binding != expected {
		return false, nil
	}
	s.binding = replacement
	return true, nil
}

func (s *stagedSessionStore) Find(context.Context, int64) (SessionBinding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binding, s.found, nil
}

func (s *stagedSessionStore) Refresh(_ context.Context, binding SessionBinding) (bool, error) {
	if s.refreshEntered != nil {
		s.refreshOnce.Do(func() {
			close(s.refreshEntered)
			<-s.continueRefresh
		})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.found && s.binding == binding, nil
}

func (s *stagedSessionStore) Unbind(_ context.Context, binding SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.found || s.binding != binding {
		return false, nil
	}
	s.found = false
	return true, nil
}

func (s *stagedSessionStore) BindPrimary(_ context.Context, roleID, sessionID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.primary[roleID] = sessionID
	return nil
}

func (s *stagedSessionStore) FindPrimary(_ context.Context, roleID int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessionID, found := s.primary[roleID]
	return sessionID, found, nil
}

func (s *stagedSessionStore) RefreshPrimary(_ context.Context, roleID, sessionID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.primary[roleID]
	return found && current == sessionID, nil
}

func (s *stagedSessionStore) UnbindPrimary(_ context.Context, roleID, sessionID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.primary[roleID]
	if !found || current != sessionID {
		return false, nil
	}
	delete(s.primary, roleID)
	return true, nil
}

func (s *stagedSessionStore) dropBinding() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.binding = SessionBinding{}
	s.found = false
}

func (s *stagedSessionStore) setBinding(binding SessionBinding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.binding = binding
	s.found = true
}

func waitMailboxSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for mailbox test stage")
	}
}

func waitMailboxResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for mailbox operation")
		return nil
	}
}

type touchPrimaryUnbindFailureStore struct {
	*stagedSessionStore
	err error
}

func (s *touchPrimaryUnbindFailureStore) RefreshPrimary(context.Context, int64, int64) (bool, error) {
	return false, nil
}

func (s *touchPrimaryUnbindFailureStore) Unbind(context.Context, SessionBinding) (bool, error) {
	return false, s.err
}

type testSessionStore struct{ roleTestStore }

func (*testSessionStore) Create(context.Context, SessionBinding) (bool, error) {
	return true, nil
}

func (*testSessionStore) Rebind(context.Context, SessionBinding, SessionBinding) (bool, error) {
	return true, nil
}

func (*testSessionStore) Find(context.Context, int64) (SessionBinding, bool, error) {
	return SessionBinding{}, false, nil
}

func (*testSessionStore) Refresh(context.Context, SessionBinding) (bool, error) {
	return true, nil
}

func (*testSessionStore) Unbind(context.Context, SessionBinding) (bool, error) {
	return true, nil
}

func (*testSessionStore) BindPrimary(context.Context, int64, int64) error { return nil }

func (*testSessionStore) FindPrimary(context.Context, int64) (int64, bool, error) {
	return 0, false, nil
}

func (*testSessionStore) RefreshPrimary(context.Context, int64, int64) (bool, error) {
	return true, nil
}

func (*testSessionStore) UnbindPrimary(context.Context, int64, int64) (bool, error) {
	return true, nil
}

func decodeJSONLogs(t *testing.T, output string) []map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	records := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func findJSONLog(t *testing.T, records []map[string]any, message string) map[string]any {
	t.Helper()
	for _, record := range records {
		if record["msg"] == message {
			return record
		}
	}
	t.Fatalf("logs = %#v, want message %q", records, message)
	return nil
}
