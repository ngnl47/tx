package msgq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRoleBroadcastKeepsIndependentMailboxesAcrossRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	a := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	b := SessionHeader{RoleID: 47, SessionID: 12, ConnectionID: 2}
	other := SessionHeader{RoleID: 48, SessionID: 13, ConnectionID: 3}
	for _, header := range []SessionHeader{a, b, other} {
		if err := server.Init(ctx, InitRequest{Header: header}); err != nil {
			t.Fatal(err)
		}
	}
	payload := []byte("shared-state")
	result, err := server.BroadcastRole(ctx, 47, payload)
	if err != nil || result.Delivered != 2 || result.Attempted != 2 {
		t.Fatalf("role broadcast = %+v, %v", result, err)
	}
	for _, header := range []SessionHeader{a, b} {
		message, err := server.Fetch(ctx, FetchRequest{Header: header})
		if err != nil || message.Seq != 1 || &message.Payload[0] != &payload[0] {
			t.Fatalf("shared mailbox = %+v, %v", message, err)
		}
	}
	if err := server.Confirm(ctx, ConfirmRequest{Header: a, Ack: 1}); err != nil {
		t.Fatal(err)
	}
	b.ConnectionID = 4
	if err := server.Init(ctx, InitRequest{Header: b, Restore: true}); err != nil {
		t.Fatal(err)
	}
	message, err := server.Fetch(ctx, FetchRequest{Header: b})
	if err != nil || message.Seq != 1 || string(message.Payload) != "shared-state" {
		t.Fatalf("restore after peer Ack = %+v, %v", message, err)
	}
	result, err = server.BroadcastRole(ctx, 47, []byte("next"))
	if err != nil || result.Delivered != 2 {
		t.Fatalf("restore duplicated subscription: %+v, %v", result, err)
	}
	if err := server.TerminateSession(ctx, a); err != nil {
		t.Fatal(err)
	}
	result, err = server.BroadcastRole(ctx, 47, []byte("remaining"))
	if err != nil || result.Attempted != 1 || result.Delivered != 1 {
		t.Fatalf("terminated subscription = %+v, %v", result, err)
	}
	untouched, _ := server.lookup(other.SessionID)
	untouched.mu.Lock()
	defer untouched.mu.Unlock()
	if untouched.downMsgs.len() != 0 {
		t.Fatal("another role received payload")
	}
}

func TestRoleBroadcastIsolatesOverflowAndRemovesSubscription(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	a := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	b := SessionHeader{RoleID: 47, SessionID: 12, ConnectionID: 2}
	for _, header := range []SessionHeader{a, b} {
		if err := server.Init(ctx, InitRequest{Header: header}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := server.PubSession(ctx, a.SessionID, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	result, err := server.BroadcastRole(ctx, 47, []byte("update"))
	if !errors.Is(err, ErrDownMsgOverflow) || result.Delivered != 1 || result.Overflowed != 1 {
		t.Fatalf("partial = %+v, %v", result, err)
	}
	if _, found := server.lookup(a.SessionID); found {
		t.Fatal("overflow mailbox retained")
	}
	if count, err := server.TerminateDeadSessions(ctx, time.Now().Add(181*time.Second)); err != nil || count != 1 {
		t.Fatalf("dead mailbox cleanup = %d, %v", count, err)
	}
	result, err = server.BroadcastRole(ctx, 47, []byte("nobody"))
	if err != nil || result.Attempted != 0 {
		t.Fatalf("empty role = %+v, %v", result, err)
	}
	if len(server.roles) != 0 {
		t.Fatal("empty role index retained")
	}
}

func TestRoleBroadcastDoesNotExposeInitializingOrRejectedMailbox(t *testing.T) {
	store := newStagedSessionStore()
	store.found = true
	store.createEntered = make(chan struct{})
	store.continueCreate = make(chan struct{})
	server, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		err := server.Init(context.Background(), InitRequest{Header: SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}})
		finished <- err
	}()
	waitMailboxSignal(t, store.createEntered)
	result, err := server.BroadcastRole(context.Background(), 47, []byte("during-init"))
	close(store.continueCreate)
	if err != nil || result.Attempted != 0 {
		t.Fatalf("initializing mailbox received data: %+v, %v", result, err)
	}
	if err := waitMailboxResult(t, finished); !errors.Is(err, ErrSessionChanged) {
		t.Fatalf("rejected init = %v", err)
	}
	if len(server.roles) != 0 {
		t.Fatal("rejected mailbox left role index")
	}
}
