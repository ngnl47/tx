package tests

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
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

func TestFusionTaskQueueEnqueuesDelayTaskBackend(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	queue := fusion.NewTaskQueueWithOptions(backend, fusion.TaskQueueOptions{Now: func() time.Time {
		return now
	}})
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	err := queue.Enqueue(ctx, fusion.DelayedOperation{
		Kind:     fusion.DelayedSave,
		Type:     meta.TypeRef{Package: "data", Name: "GDRole"},
		Key:      storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001), ShardKey: int64(7)},
		Changes:  set,
		RunAfter: time.Minute,
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	leases, err := backend.ClaimDue(ctx, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want one", leases)
	}
	got := leases[0].Task
	if got.Type != "data.GDRole" || got.ID != int64(1001) || got.ShardKey != int64(7) || got.Action != string(fusion.DelayedSave) {
		t.Fatalf("delay task = %#v", got)
	}
	if !got.DueAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("due at = %s, want %s", got.DueAt, now.Add(time.Minute))
	}
}

func TestFusionTaskQueueUsesRecordKeyTypeNameWhenTypeRefMissing(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	queue := fusion.NewTaskQueueWithOptions(backend, fusion.TaskQueueOptions{Now: func() time.Time {
		return now
	}})

	err := queue.Enqueue(ctx, fusion.DelayedOperation{
		Kind:     fusion.DelayedSave,
		Key:      storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001), ShardKey: int64(7)},
		RunAfter: time.Minute,
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	leases, err := backend.ClaimDue(ctx, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want one", leases)
	}
	if got := leases[0].Task; got.Type != "data.GDRole" || got.ID != int64(1001) || got.ShardKey != int64(7) {
		t.Fatalf("delay task = %#v, want type and key from RecordKey", got)
	}
}

func TestFusionTaskQueueRejectsNonSaveOperation(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	queue := fusion.NewTaskQueue(backend)

	err := queue.Enqueue(ctx, fusion.DelayedOperation{
		Kind: "delete",
		Type: meta.TypeRef{Package: "data", Name: "GDRole"},
		Key:  storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown delayed operation kind") {
		t.Fatalf("Enqueue error = %v, want unknown kind error", err)
	}
	leases, err := backend.ClaimDue(ctx, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases = %#v, want no queued task", leases)
	}
}

func TestFusionTaskQueueRejectsMissingRecordLocator(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	queue := fusion.NewTaskQueue(backend)

	for _, tt := range []struct {
		name string
		op   fusion.DelayedOperation
		want string
	}{
		{
			name: "missing type",
			op: fusion.DelayedOperation{
				Kind: fusion.DelayedSave,
				Key:  storage.RecordKey{ID: int64(1001)},
			},
			want: "missing delayed operation type",
		},
		{
			name: "missing id",
			op: fusion.DelayedOperation{
				Kind: fusion.DelayedSave,
				Type: meta.TypeRef{Package: "data", Name: "GDRole"},
			},
			want: "missing delayed operation record id",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := queue.Enqueue(ctx, tt.op)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Enqueue error = %v, want %q", err, tt.want)
			}
		})
	}

	leases, err := backend.ClaimDue(ctx, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases = %#v, want no queued task", leases)
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
			handled = append(handled, fmt.Sprint(item.ID))
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
	handler := &recordingSlogHandler{}
	txlog.SetDefault(slog.New(handler))
	defer txlog.SetDefault(nil)
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
	if len(handler.records) != 1 {
		t.Fatalf("log records = %#v, want one retry error", handler.records)
	}
	record := handler.records[0]
	if record.Level != slog.LevelError || record.Message != "delay task failed, retry scheduled" {
		t.Fatalf("log record = %#v", record)
	}
	if !recordHasStringAttr(record, "taskType", "role") ||
		!recordHasStringAttr(record, "taskID", "1") ||
		!recordHasStringAttr(record, "err", "temporary") {
		t.Fatalf("log attrs = %#v", recordAttrs(record))
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
