package msgq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
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
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("downMsg")}); err != nil {
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
	if err := server.Close(context.Background(), stale); !errors.Is(err, ErrSessionChanged) {
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
	if removed, err := server.Reap(context.Background(), base.Add(61*time.Second)); err != nil || removed != 0 {
		t.Fatalf("reap after 61s = %d, %v, want retained session", removed, err)
	}
	if removed, err := server.Reap(context.Background(), base.Add(181*time.Second)); err != nil || removed != 1 {
		t.Fatalf("reap after 181s = %d, %v, want one expired session", removed, err)
	}
}

func TestMailboxReapLogCorrelatesSessionAndConnection(t *testing.T) {
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
	if removed, err := server.Reap(context.Background(), time.Now().Add(2*time.Second)); err != nil || removed != 1 {
		t.Fatalf("reap = %d, %v", removed, err)
	}
	record := findJSONLog(t, decodeJSONLogs(t, output.String()), "msgq session reaped")
	if record["roleID"] != float64(header.RoleID) || record["sessionID"] != float64(header.SessionID) || record["connectionID"] != float64(header.ConnectionID) {
		t.Fatalf("reap log = %#v", record)
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
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("first")}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("overflow")}); !errors.Is(err, ErrDownMsgOverflow) {
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
	second := SessionHeader{RoleID: 2, SessionID: 1, ConnectionID: 2}
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
	firstSession, _ := server.lookup(first.RoleID)
	secondSession, _ := server.lookup(second.RoleID)
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
		if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: message}); err != nil {
			t.Fatalf("publish %q: %v", message, err)
		}
		if _, err := server.Fetch(context.Background(), FetchRequest{Header: header}); err != nil {
			t.Fatalf("fetch %q: %v", message, err)
		}
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("more")}); !errors.Is(err, ErrDownMsgOverflow) {
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
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("full")}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	message, err := server.Fetch(context.Background(), FetchRequest{Header: header})
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if err := server.Confirm(context.Background(), ConfirmRequest{Header: header, Ack: message.Seq}); err != nil {
		t.Fatalf("confirm first: %v", err)
	}
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("next")}); err != nil {
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
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: first.RoleID}, Payload: []byte("pending")}); err != nil {
		t.Fatalf("publish first: %v", err)
	}

	second := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 2}
	result, err := server.Init(context.Background(), InitRequest{Header: second})
	if err != nil {
		t.Fatalf("init fresh: %v", err)
	}
	if result.UpMsgSeq != 0 {
		t.Fatalf("fresh upMsg seq = %d, want 0", result.UpMsgSeq)
	}
	session, found := server.lookup(second.RoleID)
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
	if _, found := server.lookup(missing.RoleID); found {
		t.Fatal("restore missing installed an empty session")
	}

	current := SessionHeader{RoleID: 1, SessionID: 7, ConnectionID: 2}
	if _, err := server.Init(context.Background(), InitRequest{Header: current}); err != nil {
		t.Fatalf("init current: %v", err)
	}
	mismatch := SessionHeader{RoleID: 1, SessionID: 8, ConnectionID: 3}
	if _, err := server.Init(context.Background(), InitRequest{Header: mismatch, Restore: true}); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("restore mismatched error = %v, want %v", err, ErrSessionChanged)
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
	if _, err := server.Pub(context.Background(), PubRequest{Header: SessionHeader{RoleID: header.RoleID}, Payload: []byte("pending")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := server.Fetch(context.Background(), FetchRequest{Header: header}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	session, found := server.lookup(header.RoleID)
	if !found {
		t.Fatal("session is missing")
	}
	session.mu.Lock()
	session.lastActivity = time.Now()
	session.downMsgs.entries[session.downMsgs.head].createdAt = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	removed, err := server.Reap(context.Background(), time.Now())
	if err != nil || removed != 1 {
		t.Fatalf("reap = %d, %v", removed, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after stale unacknowledged message = %d, want 0", server.SessionCount())
	}
}

type testSessionStore struct{}

func (*testSessionStore) Bind(context.Context, SessionBinding) (SessionBinding, bool, error) {
	return SessionBinding{}, false, nil
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
