package tests

import (
	"context"
	"errors"
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

func TestMemoryDelayTaskBackendRequeuesEnqueueDuringLease(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("ClaimDue leases = %#v, error = %v, want one", leases, err)
	}

	item.DueAt = now.Add(time.Minute)
	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue during lease returned error: %v", err)
	}
	if err := backend.Complete(ctx, leases[0]); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	if leases, err = backend.ClaimDue(ctx, now.Add(30*time.Second), 1); err != nil || len(leases) != 0 {
		t.Fatalf("ClaimDue before next due leases = %#v, error = %v, want none", leases, err)
	}
	if leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 1); err != nil || len(leases) != 1 || leases[0].Attempts != 1 {
		t.Fatalf("ClaimDue after next due leases = %#v, error = %v, want one", leases, err)
	}
}

func TestMemoryDelayTaskBackendMergesEnqueueIntoRetryDuringLease(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("ClaimDue leases = %#v, error = %v, want one", leases, err)
	}

	item.DueAt = now.Add(time.Minute)
	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue during lease returned error: %v", err)
	}
	retryAt := now.Add(2 * time.Minute)
	if err := backend.Retry(ctx, leases[0], retryAt, context.Canceled); err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}

	if leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 1); err != nil || len(leases) != 0 {
		t.Fatalf("ClaimDue before retry due leases = %#v, error = %v, want none", leases, err)
	}
	if leases, err = backend.ClaimDue(ctx, retryAt, 1); err != nil || len(leases) != 1 || leases[0].Attempts != 2 {
		t.Fatalf("ClaimDue after retry due leases = %#v, error = %v, want attempt two", leases, err)
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

func TestMemoryDelayTaskBackendRejectsInvalidLease(t *testing.T) {
	backend := memory.NewBackend()
	for _, lease := range []task.DelayTaskLease{
		{},
		{Task: task.DelayTask{Type: "role", ID: "1", Action: "save"}},
		{Task: task.DelayTask{Type: "role", ID: "1", Action: "save"}, Token: "lease", Attempts: 0},
	} {
		if err := backend.Complete(context.Background(), lease); !errors.Is(err, task.ErrInvalidDelayTaskLease) {
			t.Fatalf("Complete(%#v) error = %v, want ErrInvalidDelayTaskLease", lease, err)
		}
		if err := backend.Retry(context.Background(), lease, time.Now(), context.Canceled); !errors.Is(err, task.ErrInvalidDelayTaskLease) {
			t.Fatalf("Retry(%#v) error = %v, want ErrInvalidDelayTaskLease", lease, err)
		}
	}
}

func TestMemoryDelayTaskBackendRejectsNilReceiver(t *testing.T) {
	var backend *memory.Backend
	item := task.DelayTask{Type: "role", ID: "1", Action: "save"}
	lease := task.DelayTaskLease{Task: item, Token: "lease", Attempts: 1}

	if err := backend.Enqueue(context.Background(), item); !errors.Is(err, memory.ErrBackendNotConfigured) {
		t.Fatalf("Enqueue error = %v, want ErrBackendNotConfigured", err)
	}
	if _, err := backend.ClaimDue(context.Background(), time.Now(), 1); !errors.Is(err, memory.ErrBackendNotConfigured) {
		t.Fatalf("ClaimDue error = %v, want ErrBackendNotConfigured", err)
	}
	if err := backend.Complete(context.Background(), lease); !errors.Is(err, memory.ErrBackendNotConfigured) {
		t.Fatalf("Complete error = %v, want ErrBackendNotConfigured", err)
	}
	if err := backend.Retry(context.Background(), lease, time.Now(), context.Canceled); !errors.Is(err, memory.ErrBackendNotConfigured) {
		t.Fatalf("Retry error = %v, want ErrBackendNotConfigured", err)
	}
}
