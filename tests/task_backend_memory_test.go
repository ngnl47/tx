package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/storage/task"
	"github.com/ngnl47/tx/storage/task/memory"
)

func TestMemoryDelayTaskBackendClaimsDueTasksInOrder(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)

	tasks := []task.DelayTask{
		{Type: "role", ID: "2", ShardKey: "b", Action: "save", DueAt: now.Add(time.Second)},
		{Type: "role", ID: "1", ShardKey: "a", Action: "save", DueAt: now.Add(-time.Second)},
	}
	for _, item := range tasks {
		if err := backend.Enqueue(ctx, item); err != nil {
			t.Fatalf("Enqueue returned error: %v", err)
		}
	}

	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "1" {
		t.Fatalf("leases = %#v", leases)
	}
	if err := backend.Complete(ctx, leases[0]); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	leases, err = backend.ClaimDue(ctx, now.Add(2*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "2" {
		t.Fatalf("leases after complete = %#v", leases)
	}
}

func TestMemoryDelayTaskBackendRetryReschedulesLease(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)

	err := backend.Enqueue(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v", leases)
	}
	if err := backend.Retry(ctx, leases[0], now.Add(time.Minute), context.Canceled); err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}

	leases, err = backend.ClaimDue(ctx, now.Add(30*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases before retry due = %#v", leases)
	}
	leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Attempts != 2 {
		t.Fatalf("leases after retry due = %#v", leases)
	}
}

func TestMemoryDelayTaskBackendUsesIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: "1", ShardKey: "a", KeySuffix: "save", Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	item.DueAt = now.Add(time.Hour)
	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	leases, err := backend.ClaimDue(ctx, now.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v", leases)
	}
}

func TestMemoryDelayTaskBackendKeepsColonDelimitedLocatorsDistinct(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	first := task.DelayTask{Type: "data.GD:Role", ID: "1", ShardKey: "a", Action: "save", DueAt: now}
	second := task.DelayTask{Type: "data.GD", ID: "Role:1", ShardKey: "a", Action: "save", DueAt: now}

	if first.IdempotencyKey() == second.IdempotencyKey() {
		t.Fatalf("idempotency keys collide: %q", first.IdempotencyKey())
	}
	for _, item := range []task.DelayTask{first, second} {
		if err := backend.Enqueue(ctx, item); err != nil {
			t.Fatalf("Enqueue returned error: %v", err)
		}
	}

	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 2 {
		t.Fatalf("leases = %#v, want two distinct tasks", leases)
	}
}

func TestMemoryDelayTaskBackendRejectsIncompleteTask(t *testing.T) {
	backend := memory.NewBackend()
	for _, item := range []task.DelayTask{
		{ID: "1", Action: "save"},
		{Type: "role", Action: "save"},
		{Type: "role", ID: "1"},
		{Type: "role", ID: "", Action: "save"},
	} {
		if err := backend.Enqueue(context.Background(), item); err == nil {
			t.Fatalf("Enqueue(%#v) error = nil, want invalid task error", item)
		}
	}
}
