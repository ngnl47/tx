package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/storage/fusion"
	"github.com/ngnl47/tx/storage/task"
	"github.com/ngnl47/tx/storage/task/memory"
)

// 只覆盖失效租约恢复的三个核心结果，不复制正常任务生命周期测试。
func TestFusionLostLeaseRecovery(t *testing.T) {
	for _, mode := range []string{"completed", "new-owner", "enqueue-fails"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(100, 0)
			backend := &recoveryBackend{Backend: memory.NewBackend()}
			item := task.DelayTask{Type: "role", ID: int64(1), Action: "save", DueAt: now}
			if err := backend.Enqueue(ctx, item); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("enqueue unavailable")
			var owner task.DelayTaskLease
			scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
				Now: func() time.Time { return now.Add(time.Minute) },
				Handler: fusion.DelayTaskHandlerFunc(func(ctx context.Context, _ task.DelayTask) error {
					if err := backend.Complete(ctx, backend.last); err != nil {
						t.Fatal(err)
					}
					if mode == "new-owner" {
						if err := backend.Enqueue(ctx, item); err != nil {
							t.Fatal(err)
						}
						leases, err := backend.ClaimDue(ctx, now, 1)
						if err != nil || len(leases) != 1 {
							t.Fatalf("new owner: %v %v", leases, err)
						}
						owner = leases[0]
					}
					if mode == "enqueue-fails" {
						backend.enqueueErr = failure
					}
					return errors.New("old handler failed")
				}),
			})
			_, err := scheduler.RunOnce(ctx, now)
			if mode == "enqueue-fails" {
				if !errors.Is(err, failure) {
					t.Fatalf("recovery error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "new-owner" {
				if leases, err := backend.ClaimDue(ctx, now.Add(time.Hour), 1); err != nil || len(leases) != 0 {
					t.Fatalf("new owner disturbed: %v %v", leases, err)
				}
				if err := backend.Complete(ctx, owner); err != nil {
					t.Fatal(err)
				}
			}
			leases, err := backend.ClaimDue(ctx, now.Add(time.Hour), 1)
			if err != nil || len(leases) != 1 {
				t.Fatalf("recovery missing: %v %v", leases, err)
			}
			if !leases[0].Task.DueAt.Equal(now.Add(time.Minute + time.Second)) {
				t.Fatalf("retry due = %v", leases[0].Task.DueAt)
			}
		})
	}
}

type recoveryBackend struct {
	*memory.Backend
	last       task.DelayTaskLease
	enqueueErr error
}

func (b *recoveryBackend) ClaimDue(ctx context.Context, now time.Time, limit int) ([]task.DelayTaskLease, error) {
	leases, err := b.Backend.ClaimDue(ctx, now, limit)
	if len(leases) > 0 {
		b.last = leases[0]
	}
	return leases, err
}

func (b *recoveryBackend) Enqueue(ctx context.Context, item task.DelayTask) error {
	if b.enqueueErr != nil {
		return b.enqueueErr
	}
	return b.Backend.Enqueue(ctx, item)
}
