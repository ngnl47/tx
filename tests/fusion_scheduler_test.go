package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/storage/fusion"
	"github.com/ngnl47/tx/storage/task"
	"github.com/ngnl47/tx/storage/task/memory"
)

func TestFusionSchedulerEnqueuesThroughDelayTaskBackend(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	scheduler := fusion.NewScheduler(backend)
	due := time.Unix(100, 0)

	err := scheduler.Schedule(ctx, task.DelayTask{
		Type:     "role",
		ID:       "1",
		ShardKey: "a",
		Action:   "save",
		DueAt:    due,
	})
	if err != nil {
		t.Fatalf("Schedule returned error: %v", err)
	}

	leases, err := backend.ClaimDue(ctx, due, 1)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "1" {
		t.Fatalf("leases = %#v", leases)
	}
}

func TestFusionSchedulerRunOnceCompletesSuccessfulTask(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	handled := make([]string, 0, 1)
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		BatchSize: 10,
		Handler: fusion.DelayTaskHandlerFunc(func(_ context.Context, item task.DelayTask) error {
			handled = append(handled, item.ID)
			return nil
		}),
	})

	if err := scheduler.Schedule(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Schedule returned error: %v", err)
	}
	count, err := scheduler.RunOnce(ctx, now)
	if err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if count != 1 || len(handled) != 1 || handled[0] != "1" {
		t.Fatalf("handled count=%d items=%#v, want one role 1", count, handled)
	}
	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases after successful RunOnce = %#v, want empty", leases)
	}
}

func TestFusionSchedulerRunOnceRetriesFailedTask(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		BatchSize:  10,
		RetryDelay: time.Minute,
		Handler: fusion.DelayTaskHandlerFunc(func(context.Context, task.DelayTask) error {
			return errors.New("temporary")
		}),
	})

	if err := scheduler.Schedule(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Schedule returned error: %v", err)
	}
	if _, err := scheduler.RunOnce(ctx, now); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now.Add(30*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases before retry delay = %#v, want empty", leases)
	}
	leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Attempts != 2 {
		t.Fatalf("leases after retry delay = %#v, want attempts 2", leases)
	}
}

func TestFusionSchedulerStartUsesLifecycleWaitGroup(t *testing.T) {
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		BatchSize:    10,
		PollInterval: time.Millisecond,
		Now: func() time.Time {
			return now
		},
		Handler: fusion.DelayTaskHandlerFunc(func(context.Context, task.DelayTask) error {
			close(started)
			<-release
			return nil
		}),
	})

	if err := scheduler.Schedule(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Schedule returned error: %v", err)
	}
	scheduler.Start(ctx)
	<-started

	go func() {
		ctx.Cancel()
		ctx.WaitDoneAndWaitGroupWait()
		close(done)
	}()
	assertNotClosed(t, done, "scheduler lifecycle wait returned before active handler completed")

	close(release)
	assertClosed(t, done, "scheduler lifecycle wait did not return after active handler completed")
}
