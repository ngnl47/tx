package tests

import (
	"context"
	"testing"
	"time"

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
