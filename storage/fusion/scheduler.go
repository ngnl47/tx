package fusion

import (
	"context"

	"github.com/ngnl47/gobricks/storage/task"
)

type Scheduler struct {
	backend task.DelayTaskBackend
}

func NewScheduler(backend task.DelayTaskBackend) *Scheduler {
	return &Scheduler{backend: backend}
}

func (s *Scheduler) Schedule(ctx context.Context, item task.DelayTask) error {
	return s.backend.Enqueue(ctx, item)
}
