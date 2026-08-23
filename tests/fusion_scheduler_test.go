package tests

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

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

func TestFusionSchedulerScheduleRejectsMissingBackend(t *testing.T) {
	ctx := context.Background()

	if err := (*fusion.Scheduler)(nil).Schedule(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save"}); !errors.Is(err, fusion.ErrTaskBackendNotConfigured) {
		t.Fatalf("nil Scheduler Schedule error = %v, want ErrTaskBackendNotConfigured", err)
	}
	if err := fusion.NewScheduler(nil).Schedule(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save"}); !errors.Is(err, fusion.ErrTaskBackendNotConfigured) {
		t.Fatalf("nil backend Scheduler Schedule error = %v, want ErrTaskBackendNotConfigured", err)
	}
}

func TestFusionSchedulerStartRejectsTypedNilContext(t *testing.T) {
	var ctx *lifecycle.WaitGroupContext
	scheduler := fusion.NewScheduler(memory.NewBackend())

	if err := scheduler.Start(ctx); !errors.Is(err, fusion.ErrSchedulerContextNotConfigured) {
		t.Fatalf("Start error = %v, want ErrSchedulerContextNotConfigured", err)
	}
}

func TestFusionSchedulerStartRejectsMissingBackend(t *testing.T) {
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	if err := fusion.NewScheduler(nil).Start(ctx); !errors.Is(err, fusion.ErrTaskBackendNotConfigured) {
		t.Fatalf("Start error = %v, want ErrTaskBackendNotConfigured", err)
	}
}

func TestFusionDelayTaskComponentsRejectTypedNilBackend(t *testing.T) {
	ctx := context.Background()
	item := task.DelayTask{Type: "role", ID: "1", Action: "save"}
	var backend *memory.Backend

	queue := fusion.NewTaskQueue(backend)
	if err := queue.Enqueue(ctx, fusion.DelayedOperation{
		Kind: fusion.DelayedSave,
		Type: meta.TypeRef{Package: "data", Name: "GDRole"},
		Key:  storage.RecordKey{ID: "1"},
	}); !errors.Is(err, fusion.ErrTaskBackendNotConfigured) {
		t.Fatalf("typed nil backend TaskQueue error = %v, want ErrTaskBackendNotConfigured", err)
	}

	scheduler := fusion.NewScheduler(backend)
	if err := scheduler.Schedule(ctx, item); !errors.Is(err, fusion.ErrTaskBackendNotConfigured) {
		t.Fatalf("typed nil backend Schedule error = %v, want ErrTaskBackendNotConfigured", err)
	}
	if _, err := scheduler.RunOnce(ctx, time.Now()); !errors.Is(err, fusion.ErrTaskBackendNotConfigured) {
		t.Fatalf("typed nil backend RunOnce error = %v, want ErrTaskBackendNotConfigured", err)
	}
}

func TestFusionSchedulerRetriesTaskWhenHandlerIsMissing(t *testing.T) {
	backend := memory.NewBackend()
	scheduler := fusion.NewScheduler(backend)
	now := time.Unix(100, 0)
	if err := backend.Enqueue(context.Background(), task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if _, err := scheduler.RunOnce(context.Background(), now); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	leases, err := backend.ClaimDue(context.Background(), now.Add(time.Second), 1)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want one retried task", leases)
	}
}

func TestFusionSchedulerRetriesTaskWhenHandlerIsTypedNil(t *testing.T) {
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	var handler fusion.DelayTaskHandlerFunc
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		Handler: handler,
	})
	if err := backend.Enqueue(context.Background(), task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if _, err := scheduler.RunOnce(context.Background(), now); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	leases, err := backend.ClaimDue(context.Background(), now.Add(time.Second), 1)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "1" {
		t.Fatalf("leases = %#v, want one retried task", leases)
	}
}

func TestFusionTaskQueueEnqueuesDelayTaskBackend(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	queue := fusion.NewTaskQueueWithOptions(backend, fusion.TaskQueueOptions{Now: func() time.Time {
		return now
	}})
	err := queue.Enqueue(ctx, fusion.DelayedOperation{
		Kind:     fusion.DelayedSave,
		Type:     meta.TypeRef{Package: "data", Name: "GDRole"},
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

func TestFusionTaskQueueEnqueuesDelayedDelete(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	queue := fusion.NewTaskQueueWithOptions(backend, fusion.TaskQueueOptions{Now: func() time.Time {
		return now
	}})

	err := queue.Enqueue(ctx, fusion.DelayedOperation{
		Kind:     fusion.DelayedDelete,
		Key:      storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001), ShardKey: int64(7)},
		RunAfter: time.Hour,
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	leases, err := backend.ClaimDue(ctx, now.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want one", leases)
	}
	got := leases[0].Task
	if got.Type != "data.GDRole" || got.ID != int64(1001) || got.ShardKey != int64(7) || got.Action != string(fusion.DelayedDelete) {
		t.Fatalf("delay task = %#v", got)
	}
}

func TestFusionTaskQueueRejectsUnknownOperation(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	queue := fusion.NewTaskQueue(backend)

	err := queue.Enqueue(ctx, fusion.DelayedOperation{
		Kind: "compact",
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
	var typedNil *int64

	for _, tt := range []struct {
		name string
		op   fusion.DelayedOperation
	}{
		{
			name: "missing type",
			op: fusion.DelayedOperation{
				Kind: fusion.DelayedSave,
				Key:  storage.RecordKey{ID: int64(1001)},
			},
		},
		{
			name: "missing id",
			op: fusion.DelayedOperation{
				Kind: fusion.DelayedSave,
				Type: meta.TypeRef{Package: "data", Name: "GDRole"},
			},
		},
		{
			name: "typed nil id",
			op: fusion.DelayedOperation{
				Kind: fusion.DelayedSave,
				Type: meta.TypeRef{Package: "data", Name: "GDRole"},
				Key:  storage.RecordKey{ID: typedNil},
			},
		},
		{
			name: "blank string id",
			op: fusion.DelayedOperation{
				Kind: fusion.DelayedSave,
				Type: meta.TypeRef{Package: "data", Name: "GDRole"},
				Key:  storage.RecordKey{ID: "  "},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := queue.Enqueue(ctx, tt.op)
			if !errors.Is(err, task.ErrInvalidDelayTask) {
				t.Fatalf("Enqueue error = %v, want ErrInvalidDelayTask", err)
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
		BatchSize:         10,
		RetryInitialDelay: time.Minute,
		RetryMaxDelay:     time.Minute,
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
	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("log records = %#v, want one retry error", records)
	}
	record := records[0]
	if record.Level != slog.LevelError || record.Message != "delay task failed, retry scheduled" {
		t.Fatalf("log record = %#v", record)
	}
	if !recordHasStringAttr(record, "taskType", "role") ||
		!recordHasStringAttr(record, "taskID", "1") ||
		!recordHasStringAttr(record, "err", "temporary") {
		t.Fatalf("log attrs = %#v", recordAttrs(record))
	}
}

func TestFusionSchedulerProcessesPartialClaimBeforeReturningError(t *testing.T) {
	claimErr := errors.New("later bucket failed")
	backend := &partialDelayTaskBackend{
		leases: []task.DelayTaskLease{{
			Task:     task.DelayTask{Type: "role", ID: "1", Action: "save"},
			Token:    "lease-1",
			Attempts: 1,
		}},
		claimErr: claimErr,
	}
	handled := make([]string, 0, 1)
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		Handler: fusion.DelayTaskHandlerFunc(func(_ context.Context, item task.DelayTask) error {
			handled = append(handled, fmt.Sprint(item.ID))
			return nil
		}),
	})

	count, err := scheduler.RunOnce(context.Background(), time.Unix(100, 0))
	if count != 1 || !errors.Is(err, claimErr) {
		t.Fatalf("RunOnce count=%d error=%v, want one and claim error", count, err)
	}
	if fmt.Sprint(handled) != "[1]" || fmt.Sprint(backend.completed) != "[1]" {
		t.Fatalf("handled=%v completed=%v, want partial lease settled", handled, backend.completed)
	}
}

func TestFusionSchedulerSettlesRemainingLeasesAfterFinalizeError(t *testing.T) {
	completeErr := errors.New("complete failed")
	backend := &partialDelayTaskBackend{
		leases: []task.DelayTaskLease{
			{Task: task.DelayTask{Type: "role", ID: "1", Action: "save"}, Token: "lease-1", Attempts: 1},
			{Task: task.DelayTask{Type: "role", ID: "2", Action: "save"}, Token: "lease-2", Attempts: 1},
		},
		completeErrors: map[string]error{"1": completeErr},
	}
	handled := make([]string, 0, 2)
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		Handler: fusion.DelayTaskHandlerFunc(func(_ context.Context, item task.DelayTask) error {
			handled = append(handled, fmt.Sprint(item.ID))
			return nil
		}),
	})

	count, err := scheduler.RunOnce(context.Background(), time.Unix(100, 0))
	if count != 2 || !errors.Is(err, completeErr) {
		t.Fatalf("RunOnce count=%d error=%v, want two and complete error", count, err)
	}
	if fmt.Sprint(handled) != "[1 2]" || fmt.Sprint(backend.completed) != "[1 2]" {
		t.Fatalf("handled=%v completed=%v, want both leases settled", handled, backend.completed)
	}
}

func TestFusionSchedulerSettlesRemainingLeasesAfterRetryError(t *testing.T) {
	retryErr := errors.New("retry failed")
	handlerErr := errors.New("handler failed")
	backend := &partialDelayTaskBackend{
		leases: []task.DelayTaskLease{
			{Task: task.DelayTask{Type: "role", ID: "1", Action: "save"}, Token: "lease-1", Attempts: 1},
			{Task: task.DelayTask{Type: "role", ID: "2", Action: "save"}, Token: "lease-2", Attempts: 1},
		},
		retryErrors: map[string]error{"1": retryErr},
	}
	handled := make([]string, 0, 2)
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		Handler: fusion.DelayTaskHandlerFunc(func(_ context.Context, item task.DelayTask) error {
			id := fmt.Sprint(item.ID)
			handled = append(handled, id)
			if id == "1" {
				return handlerErr
			}
			return nil
		}),
	})

	count, err := scheduler.RunOnce(context.Background(), time.Unix(100, 0))
	if count != 2 || !errors.Is(err, handlerErr) || !errors.Is(err, retryErr) {
		t.Fatalf("RunOnce count=%d error=%v, want two with handler and retry errors", count, err)
	}
	if fmt.Sprint(handled) != "[1 2]" || fmt.Sprint(backend.retried) != "[1]" || fmt.Sprint(backend.completed) != "[2]" {
		t.Fatalf("handled=%v retried=%v completed=%v, want failed retry followed by second completion", handled, backend.retried, backend.completed)
	}
}

func TestFusionSchedulerRetriesWithCappedExponentialDelay(t *testing.T) {
	ctx := context.Background()
	backend := memory.NewBackend()
	now := time.Unix(100, 0)
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		RetryInitialDelay: time.Second,
		RetryMultiplier:   2,
		RetryMaxDelay:     2 * time.Second,
		Handler: fusion.DelayTaskHandlerFunc(func(context.Context, task.DelayTask) error {
			return errors.New("temporary")
		}),
	})

	if err := scheduler.Schedule(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Schedule returned error: %v", err)
	}
	if _, err := scheduler.RunOnce(ctx, now); err != nil {
		t.Fatalf("first RunOnce returned error: %v", err)
	}
	if _, err := scheduler.RunOnce(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("second RunOnce returned error: %v", err)
	}
	if leases, err := backend.ClaimDue(ctx, now.Add(2*time.Second), 1); err != nil || len(leases) != 0 {
		t.Fatalf("second retry leases before capped delay = %#v, %v; want none", leases, err)
	}
	if leases, err := backend.ClaimDue(ctx, now.Add(3*time.Second), 1); err != nil || len(leases) != 1 || leases[0].Attempts != 3 {
		t.Fatalf("second retry leases = %#v, %v; want attempt 3 at capped delay", leases, err)
	}
}

func TestFusionSchedulerStartLogsRunOnceError(t *testing.T) {
	backend := &failingDelayTaskBackend{
		err:     errors.New("claim failed"),
		claimed: make(chan struct{}),
	}
	handler := &recordingSlogHandler{}
	txlog.SetDefault(slog.New(handler))
	defer txlog.SetDefault(nil)

	ctx := lifecycle.NewWaitGroupContext(context.Background())
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		PollInterval: time.Hour,
	})

	scheduler.Start(ctx)
	assertClosed(t, backend.claimed, "scheduler did not attempt to claim due tasks")

	deadline := time.After(time.Second)
	var records []slog.Record
	for len(records) == 0 {
		select {
		case <-deadline:
			t.Fatal("scheduler did not log RunOnce error")
		case <-time.After(time.Millisecond):
		}
		records = handler.Records()
	}

	ctx.Cancel()
	ctx.WaitDoneAndWaitGroupWait()

	record := records[0]
	if record.Level != slog.LevelError || record.Message != "delay task scheduler failed" {
		t.Fatalf("log record = %#v, want scheduler error", record)
	}
	if !recordHasStringAttr(record, "err", "claim failed") {
		t.Fatalf("log attrs = %#v, want claim failed", recordAttrs(record))
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

type failingDelayTaskBackend struct {
	err     error
	claimed chan struct{}
	closed  bool
}

type partialDelayTaskBackend struct {
	leases         []task.DelayTaskLease
	claimErr       error
	completeErrors map[string]error
	retryErrors    map[string]error
	completed      []string
	retried        []string
}

func (b *partialDelayTaskBackend) Enqueue(context.Context, task.DelayTask) error {
	return nil
}

func (b *partialDelayTaskBackend) ClaimDue(context.Context, time.Time, int) ([]task.DelayTaskLease, error) {
	return b.leases, b.claimErr
}

func (b *partialDelayTaskBackend) Complete(_ context.Context, lease task.DelayTaskLease) error {
	id := fmt.Sprint(lease.Task.ID)
	b.completed = append(b.completed, id)
	return b.completeErrors[id]
}

func (b *partialDelayTaskBackend) Retry(_ context.Context, lease task.DelayTaskLease, _ time.Time, _ error) error {
	id := fmt.Sprint(lease.Task.ID)
	b.retried = append(b.retried, id)
	return b.retryErrors[id]
}

func (b *failingDelayTaskBackend) Enqueue(context.Context, task.DelayTask) error {
	return nil
}

func (b *failingDelayTaskBackend) ClaimDue(context.Context, time.Time, int) ([]task.DelayTaskLease, error) {
	if !b.closed {
		close(b.claimed)
		b.closed = true
	}
	return nil, b.err
}

func (b *failingDelayTaskBackend) Complete(context.Context, task.DelayTaskLease) error {
	return nil
}

func (b *failingDelayTaskBackend) Retry(context.Context, task.DelayTaskLease, time.Time, error) error {
	return nil
}
