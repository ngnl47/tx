package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

func TestMsgQServerRestoresUnconfirmedDownMsgMessages(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              store,
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer server.TerminateSession(context.Background(), msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8})

	first := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	init, err := server.Init(context.Background(), msgq.InitRequest{Header: first})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if init.UpMsgSeq != 0 {
		t.Fatalf("upMsg seq = %d, want 0", init.UpMsgSeq)
	}
	if _, err := server.PubSession(context.Background(), first.SessionID, []byte("first")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := server.PubSession(context.Background(), first.SessionID, []byte("second")); err != nil {
		t.Fatalf("publish second: %v", err)
	}

	firstMessage, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: first})
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if firstMessage.Seq != 1 || string(firstMessage.Payload) != "first" {
		t.Fatalf("first fetch = %#v", firstMessage)
	}

	secondConn := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{
		Header:  secondConn,
		Restore: true,
		Ack:     1,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	secondMessage, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: secondConn})
	if err != nil {
		t.Fatalf("fetch restored: %v", err)
	}
	if secondMessage.Seq != 2 || string(secondMessage.Payload) != "second" {
		t.Fatalf("restored fetch = %#v", secondMessage)
	}
	if err := server.Confirm(context.Background(), msgq.ConfirmRequest{Header: secondConn, Ack: 2}); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = server.Fetch(ctx, msgq.FetchRequest{Header: secondConn})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty fetch error = %v, want deadline exceeded", err)
	}
}

func TestMsgQServerSerializesUpMsgAndRejectsStaleConnection(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 2,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	current := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: current}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer server.TerminateSession(context.Background(), current)

	received, err := server.Rec(context.Background(), msgq.RecRequest{Header: current, Seq: 1})
	if err != nil || received.UpMsgSeq != 1 {
		t.Fatalf("rec = %#v, %v", received, err)
	}
	if _, err := server.Rec(context.Background(), msgq.RecRequest{Header: current, Seq: 3}); !errors.Is(err, msgq.ErrUpMsgSequence) {
		t.Fatalf("over seq error = %v, want upMsg sequence", err)
	}
	if _, err := server.Rec(context.Background(), msgq.RecRequest{
		Header: msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7},
		Seq:    2,
	}); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("stale rec error = %v, want session changed", err)
	}
}

func TestMsgQServerBoundsDownMsgQueue(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 1,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("one")); err != nil {
		t.Fatalf("publish one: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("two")); !errors.Is(err, msgq.ErrDownMsgOverflow) {
		t.Fatalf("publish overflow error = %v, want downMsg overflow", err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after overflow = %d, want 0", server.SessionCount())
	}
}

func TestMsgQServerBoundsDownMsgQueueBytes(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 4,
		MaxBacklogBytes:    3,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("123")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("4")); !errors.Is(err, msgq.ErrDownMsgOverflow) {
		t.Fatalf("publish byte overflow error = %v, want downMsg overflow", err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after byte overflow = %d, want 0", server.SessionCount())
	}
}

func TestMsgQServerRejectsAcknowledgementBeyondSentDownMsg(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-1", Store: newMailboxSessionStore(), MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := server.PubSession(context.Background(), header.SessionID, []byte("one")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := server.Confirm(context.Background(), msgq.ConfirmRequest{Header: header, Ack: 1}); !errors.Is(err, msgq.ErrDownMsgAcknowledgement) {
		t.Fatalf("confirm before fetch error = %v, want invalid acknowledgement", err)
	}
	message, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: header})
	if err != nil || message.Seq != 1 {
		t.Fatalf("fetch = %#v, %v", message, err)
	}
	if err := server.Confirm(context.Background(), msgq.ConfirmRequest{Header: header, Ack: 1}); err != nil {
		t.Fatalf("confirm sent message: %v", err)
	}
}

func TestMsgQServerFetchSomeKeepsUnconfirmedSequence(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer server.TerminateSession(context.Background(), header)
	for _, message := range []string{"one", "two", "three"} {
		if _, err := server.PubSession(context.Background(), header.SessionID, []byte(message)); err != nil {
			t.Fatalf("publish %q: %v", message, err)
		}
	}

	batch, err := server.FetchSome(context.Background(), msgq.FetchSomeRequest{Header: header, Max: 2})
	if err != nil {
		t.Fatalf("fetch some: %v", err)
	}
	if len(batch.Messages) != 2 || batch.Messages[0].Seq != 1 || batch.Messages[1].Seq != 2 || string(batch.Messages[1].Payload) != "two" {
		t.Fatalf("batch = %#v", batch.Messages)
	}
	last, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: header})
	if err != nil || last.Seq != 3 || string(last.Payload) != "three" {
		t.Fatalf("last = %#v, %v", last, err)
	}
}

func TestMsgQServerBroadcastTopicReportsPartialOverflow(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 1,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	second := msgq.SessionHeader{RoleID: 470002, SessionID: 100, ConnectionID: 8}
	for _, header := range []msgq.SessionHeader{first, second} {
		if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
			t.Fatalf("init %#v: %v", header, err)
		}
		defer server.TerminateSession(context.Background(), header)
		if err := server.JoinTopics(context.Background(), msgq.TopicRequest{Header: header, Topics: []string{"guild:9"}}); err != nil {
			t.Fatalf("join topic: %v", err)
		}
	}

	firstBroadcast, err := server.BroadcastTopic(context.Background(), msgq.BroadcastTopicRequest{
		Topic:   "guild:9",
		Payload: []byte("first"),
	})
	if err != nil || firstBroadcast.Attempted != 2 || firstBroadcast.Delivered != 2 || firstBroadcast.Overflowed != 0 {
		t.Fatalf("first broadcast = %#v, %v", firstBroadcast, err)
	}
	secondBroadcast, err := server.BroadcastTopic(context.Background(), msgq.BroadcastTopicRequest{
		Topic:   "guild:9",
		Payload: []byte("second"),
	})
	if !errors.Is(err, msgq.ErrDownMsgOverflow) || secondBroadcast.Attempted != 2 || secondBroadcast.Delivered != 0 || secondBroadcast.Overflowed != 2 {
		t.Fatalf("second broadcast = %#v, %v", secondBroadcast, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count after topic overflow = %d, want 0", server.SessionCount())
	}
}

func TestMsgQServerRejectsOldConnectionCloseAfterRestore(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              store,
		MaxBacklogMessages: 2,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldHeader := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	newHeader := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: oldHeader}); err != nil {
		t.Fatalf("init old connection: %v", err)
	}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: newHeader, Restore: true}); err != nil {
		t.Fatalf("restore new connection: %v", err)
	}
	if err := server.TerminateSession(context.Background(), oldHeader); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("old close error = %v, want session changed", err)
	}
	if server.SessionCount() != 1 {
		t.Fatalf("session count = %d, want 1", server.SessionCount())
	}
	binding, found, err := store.Find(context.Background(), newHeader.SessionID)
	if err != nil || !found || binding.SessionID != 99 || binding.ConnectionID != 8 || binding.InstanceID != "msgq-1" {
		t.Fatalf("binding after old close = %#v, found = %t, err = %v", binding, found, err)
	}
	if err := server.TerminateSession(context.Background(), newHeader); err != nil {
		t.Fatalf("close new connection: %v", err)
	}
}

func TestMsgQServerKeepsSameRoleSessionsIsolated(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              store,
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	second := msgq.SessionHeader{RoleID: 470001, SessionID: 100, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: first}); err != nil {
		t.Fatalf("init first: %v", err)
	}
	if _, err := server.PubSession(context.Background(), first.SessionID, []byte("first-only")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: second}); err != nil {
		t.Fatalf("init second: %v", err)
	}
	if _, err := server.PubSession(context.Background(), second.SessionID, []byte("second-only")); err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if server.SessionCount() != 2 {
		t.Fatalf("session count = %d, want 2", server.SessionCount())
	}
	firstMessage, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: first})
	if err != nil || string(firstMessage.Payload) != "first-only" {
		t.Fatalf("first fetch = %#v, %v", firstMessage, err)
	}
	secondMessage, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: second})
	if err != nil || string(secondMessage.Payload) != "second-only" {
		t.Fatalf("second fetch = %#v, %v", secondMessage, err)
	}
	if err := server.TerminateSession(context.Background(), first); err != nil {
		t.Fatalf("close first: %v", err)
	}
	if server.SessionCount() != 1 {
		t.Fatalf("session count after first close = %d, want 1", server.SessionCount())
	}
	if _, found, err := store.Find(context.Background(), second.SessionID); err != nil || !found {
		t.Fatalf("second binding after first close = %t, %v", found, err)
	}
}

func TestMsgQServerInitializesSameRoleSessionsConcurrently(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	headers := []msgq.SessionHeader{
		{RoleID: 470001, SessionID: 99, ConnectionID: 7},
		{RoleID: 470001, SessionID: 100, ConnectionID: 8},
	}
	start := make(chan struct{})
	results := make(chan error, len(headers))
	for _, header := range headers {
		go func() {
			<-start
			_, err := server.Init(context.Background(), msgq.InitRequest{Header: header})
			results <- err
		}()
	}
	close(start)
	for range headers {
		if err := <-results; err != nil {
			t.Fatalf("concurrent init: %v", err)
		}
	}
	if server.SessionCount() != len(headers) {
		t.Fatalf("session count = %d, want %d", server.SessionCount(), len(headers))
	}
	for _, header := range headers {
		if err := server.Touch(context.Background(), msgq.TouchRequest{Header: header}); err != nil {
			t.Fatalf("touch session %d: %v", header.SessionID, err)
		}
	}
}

func TestMsgQServerFreshSessionDoesNotInheritMailboxState(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              newMailboxSessionStore(),
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	second := msgq.SessionHeader{RoleID: 470001, SessionID: 100, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: first}); err != nil {
		t.Fatalf("init first: %v", err)
	}
	if _, err := server.Rec(context.Background(), msgq.RecRequest{Header: first, Seq: 1}); err != nil {
		t.Fatalf("rec first: %v", err)
	}
	if _, err := server.PubSession(context.Background(), first.SessionID, []byte("old-pending")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	result, err := server.Init(context.Background(), msgq.InitRequest{Header: second})
	if err != nil {
		t.Fatalf("init second: %v", err)
	}
	if result.UpMsgSeq != 0 {
		t.Fatalf("fresh upMsg seq = %d, want 0", result.UpMsgSeq)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := server.Fetch(ctx, msgq.FetchRequest{Header: second}); !errors.Is(err, context.Canceled) {
		t.Fatalf("fresh fetch error = %v, want canceled empty queue", err)
	}
	oldMessage, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: first})
	if err != nil || string(oldMessage.Payload) != "old-pending" {
		t.Fatalf("old fetch = %#v, %v", oldMessage, err)
	}
}

func TestMsgQServerFreshInitRejectsExistingSessionID(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	current := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: current}); err != nil {
		t.Fatalf("init current: %v", err)
	}
	conflict := msgq.SessionHeader{RoleID: 470002, SessionID: current.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: conflict}); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("init conflict error = %v, want %v", err, msgq.ErrSessionChanged)
	}
	if server.SessionCount() != 1 {
		t.Fatalf("session count = %d, want 1", server.SessionCount())
	}
	if err := server.Touch(context.Background(), msgq.TouchRequest{Header: current}); err != nil {
		t.Fatalf("touch current after conflict: %v", err)
	}
}

func TestMsgQServerBorrowSessionDoesNotReplacePrimary(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	primary := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	borrow := msgq.SessionHeader{RoleID: 470001, SessionID: 100, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: primary, Primary: true}); err != nil {
		t.Fatalf("init primary: %v", err)
	}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: borrow}); err != nil {
		t.Fatalf("init borrow: %v", err)
	}
	if current, found, err := store.FindPrimary(context.Background(), primary.RoleID); err != nil || !found || current != primary.SessionID {
		t.Fatalf("primary after borrow = %d, %t, %v", current, found, err)
	}
	if _, err := server.PubSession(context.Background(), borrow.SessionID, []byte("borrow-target")); err != nil {
		t.Fatalf("publish borrow: %v", err)
	}
	message, err := server.Fetch(context.Background(), msgq.FetchRequest{Header: borrow})
	if err != nil || string(message.Payload) != "borrow-target" {
		t.Fatalf("borrow fetch = %#v, %v", message, err)
	}
}

func TestMsgQServerRestoreRequiresStablePrimaryProperty(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	first := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: first, Primary: true}); err != nil {
		t.Fatalf("init first: %v", err)
	}
	restored := msgq.SessionHeader{RoleID: first.RoleID, SessionID: first.SessionID, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: restored, Restore: true}); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("restore with changed primary property error = %v, want %v", err, msgq.ErrSessionChanged)
	}
	replacement := msgq.SessionHeader{RoleID: first.RoleID, SessionID: 100, ConnectionID: 9}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: replacement, Primary: true}); err != nil {
		t.Fatalf("init replacement primary: %v", err)
	}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: restored, Restore: true, Primary: true}); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("restore replaced primary error = %v, want %v", err, msgq.ErrSessionChanged)
	}
}

func TestMsgQServerPrimaryLifecycleUsesConditionalProjection(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	oldPrimary := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	newPrimary := msgq.SessionHeader{RoleID: 470001, SessionID: 100, ConnectionID: 8}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: oldPrimary, Primary: true}); err != nil {
		t.Fatalf("init old primary: %v", err)
	}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: newPrimary, Primary: true}); err != nil {
		t.Fatalf("init new primary: %v", err)
	}
	if err := server.Touch(context.Background(), msgq.TouchRequest{Header: oldPrimary}); !errors.Is(err, msgq.ErrSessionChanged) {
		t.Fatalf("touch replaced primary error = %v, want %v", err, msgq.ErrSessionChanged)
	}
	if _, found, err := store.Find(context.Background(), oldPrimary.SessionID); err != nil || found {
		t.Fatalf("old exact binding after stale touch = %t, %v", found, err)
	}
	if primary, found, err := store.FindPrimary(context.Background(), oldPrimary.RoleID); err != nil || !found || primary != newPrimary.SessionID {
		t.Fatalf("primary after stale touch = %d, %t, %v", primary, found, err)
	}
	if err := server.TerminateSession(context.Background(), newPrimary); err != nil {
		t.Fatalf("terminate new primary: %v", err)
	}
	if primary, found, err := store.FindPrimary(context.Background(), newPrimary.RoleID); err != nil || !found || primary != newPrimary.SessionID {
		t.Fatalf("primary after terminate = %d, %t, %v, want %d", primary, found, err, newPrimary.SessionID)
	}
}

func TestMsgQServerDeadTerminationAndOverflowPreservePrimaryProjection(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*testing.T, *msgq.Server, msgq.SessionHeader)
	}{
		{
			name: "dead termination",
			run: func(t *testing.T, server *msgq.Server, _ msgq.SessionHeader) {
				t.Helper()
				if removed, err := server.TerminateDeadSessions(context.Background(), time.Now().Add(2*time.Second)); err != nil || removed != 1 {
					t.Fatalf("terminate dead sessions = %d, %v", removed, err)
				}
			},
		},
		{
			name: "overflow",
			run: func(t *testing.T, server *msgq.Server, header msgq.SessionHeader) {
				t.Helper()
				if _, err := server.PubSession(context.Background(), header.SessionID, []byte("first")); err != nil {
					t.Fatalf("publish first: %v", err)
				}
				if _, err := server.PubSession(context.Background(), header.SessionID, []byte("overflow")); !errors.Is(err, msgq.ErrDownMsgOverflow) {
					t.Fatalf("publish overflow error = %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newMailboxSessionStore()
			server, err := msgq.NewServer(msgq.ServerOptions{
				InstanceID:         "msgq-1",
				Store:              store,
				MaxBacklogMessages: 1,
				OfflineTimeout:     time.Second,
			})
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
			if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header, Primary: true}); err != nil {
				t.Fatalf("init primary: %v", err)
			}
			test.run(t, server, header)
			if primary, found, err := store.FindPrimary(context.Background(), header.RoleID); err != nil || !found || primary != header.SessionID {
				t.Fatalf("primary after %s = %d, %t, %v, want %d", test.name, primary, found, err, header.SessionID)
			}
			if _, found, err := store.Find(context.Background(), header.SessionID); err != nil || found {
				t.Fatalf("binding after %s = %t, %v", test.name, found, err)
			}
		})
	}
}

func TestMsgQServerRedisIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run msgq mailbox Redis integration test")
	}
	ctx := context.Background()
	raw := redis.NewClient(&redis.Options{Addr: endpoint})
	defer raw.Close()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}
	prefix := fmt.Sprintf("tx:test:msgq-mailbox:%d", time.Now().UnixNano())
	key := prefix + ":session:v2:{99}"
	primaryKey := prefix + ":primary:v1:{470001}"
	defer raw.Del(ctx, key, primaryKey)

	bindings, err := msgq.NewRedisSessionStore(goredis.NewClient(raw), msgq.SessionRedisOptions{
		Prefix: prefix,
		TTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new redis session store: %v", err)
	}
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:         "msgq-1",
		Store:              bindings,
		MaxBacklogMessages: 2,
		OfflineTimeout:     time.Minute,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(ctx, msgq.InitRequest{Header: header, Primary: true}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, found, err := bindings.Find(ctx, header.SessionID); err != nil || !found {
		t.Fatalf("find owner after init = %t, %v", found, err)
	}
	if primary, found, err := bindings.FindPrimary(ctx, header.RoleID); err != nil || !found || primary != header.SessionID {
		t.Fatalf("find primary after init = %d, %t, %v", primary, found, err)
	}
	if _, err := server.PubSession(ctx, header.SessionID, []byte("notify")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	message, err := server.Fetch(ctx, msgq.FetchRequest{Header: header})
	if err != nil || message.Seq != 1 || string(message.Payload) != "notify" {
		t.Fatalf("fetch = %#v, %v", message, err)
	}
	if err := server.Confirm(ctx, msgq.ConfirmRequest{Header: header, Ack: message.Seq}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if removed, err := server.TerminateDeadSessions(ctx, time.Now().Add(2*time.Minute)); err != nil || removed != 1 {
		t.Fatalf("terminate dead sessions = %d, %v", removed, err)
	}
	if _, found, err := bindings.Find(ctx, header.SessionID); err != nil || found {
		t.Fatalf("find owner after dead termination = %t, %v", found, err)
	}
	if primary, found, err := bindings.FindPrimary(ctx, header.RoleID); err != nil || !found || primary != header.SessionID {
		t.Fatalf("find primary after dead termination = %d, %t, %v, want %d", primary, found, err, header.SessionID)
	}
}

func TestMsgQServerTerminateDeadSessionsRemovesOfflineSessionAndBinding(t *testing.T) {
	store := newMailboxSessionStore()
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:                   "msgq-1",
		Store:                        store,
		MaxBacklogMessages:           2,
		OfflineTimeout:               time.Second,
		TerminateDeadSessionInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	removed, err := server.TerminateDeadSessions(context.Background(), time.Now().Add(2*time.Second))
	if err != nil || removed != 1 {
		t.Fatalf("terminate dead sessions = %d, %v", removed, err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("session count = %d, want 0", server.SessionCount())
	}
	if _, found, err := store.Find(context.Background(), header.SessionID); err != nil || found {
		t.Fatalf("binding after dead termination = found %t, err %v", found, err)
	}
}

func TestMsgQServerStartUsesLifecycleWorker(t *testing.T) {
	server, err := msgq.NewServer(msgq.ServerOptions{
		InstanceID:                   "msgq-1",
		Store:                        newMailboxSessionStore(),
		MaxBacklogMessages:           2,
		OfflineTimeout:               time.Nanosecond,
		TerminateDeadSessionInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	header := msgq.SessionHeader{RoleID: 470001, SessionID: 99, ConnectionID: 7}
	if _, err := server.Init(context.Background(), msgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("init: %v", err)
	}
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline := time.After(time.Second)
	for server.SessionCount() != 0 {
		select {
		case <-deadline:
			t.Fatal("dead session was not terminated by lifecycle worker")
		case <-time.After(time.Millisecond):
		}
	}
	ctx.Cancel()
	ctx.WaitDoneAndWaitGroupWait()
}

type mailboxSessionStore struct {
	mu       sync.Mutex
	bindings map[int64]msgq.SessionBinding
	primary  map[int64]int64
}

func newMailboxSessionStore() *mailboxSessionStore {
	return &mailboxSessionStore{
		bindings: make(map[int64]msgq.SessionBinding),
		primary:  make(map[int64]int64),
	}
}

func (s *mailboxSessionStore) Create(_ context.Context, binding msgq.SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.bindings[binding.SessionID]; exists {
		return false, nil
	}
	s.bindings[binding.SessionID] = binding
	return true, nil
}

func (s *mailboxSessionStore) Rebind(_ context.Context, expected, replacement msgq.SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.bindings[expected.SessionID]
	if !found || current != expected {
		return false, nil
	}
	s.bindings[replacement.SessionID] = replacement
	return true, nil
}

func (s *mailboxSessionStore) Find(_ context.Context, sessionID int64) (msgq.SessionBinding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, found := s.bindings[sessionID]
	return binding, found, nil
}

func (s *mailboxSessionStore) Refresh(_ context.Context, binding msgq.SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.bindings[binding.SessionID]
	return found && current == binding, nil
}

func (s *mailboxSessionStore) Unbind(_ context.Context, binding msgq.SessionBinding) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.bindings[binding.SessionID]
	if !found || current != binding {
		return false, nil
	}
	delete(s.bindings, binding.SessionID)
	return true, nil
}

func (s *mailboxSessionStore) BindPrimary(_ context.Context, roleID, sessionID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.primary[roleID] = sessionID
	return nil
}

func (s *mailboxSessionStore) FindPrimary(_ context.Context, roleID int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessionID, found := s.primary[roleID]
	return sessionID, found, nil
}

func (s *mailboxSessionStore) RefreshPrimary(_ context.Context, roleID, sessionID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.primary[roleID]
	return found && current == sessionID, nil
}

func (s *mailboxSessionStore) UnbindPrimary(_ context.Context, roleID, sessionID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.primary[roleID]
	if !found || current != sessionID {
		return false, nil
	}
	delete(s.primary, roleID)
	return true, nil
}
