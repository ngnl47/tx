package msgq

import (
	"context"
	"testing"
)

func TestStopReleasesRoleAndRejectsReinitialization(t *testing.T) {
	ctx := context.Background()
	store := &testSessionStore{}
	old, err := NewServer(ServerOptions{InstanceID: "old", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if _, err := old.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	if err := old.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Init(ctx, InitRequest{Header: header}); err == nil {
		t.Fatal("stopped mailbox accepted Init")
	}
	next, err := NewServer(ServerOptions{InstanceID: "next", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	header.SessionID = 12
	if _, err := next.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatalf("role remained bound after Stop: %v", err)
	}
	if err := old.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := next.Touch(ctx, TouchRequest{Header: header}); err != nil {
		t.Fatalf("repeated Stop affected new owner: %v", err)
	}
}
