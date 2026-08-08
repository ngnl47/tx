package fusion

import (
	"context"
	"time"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/storage/task"
)

const (
	defaultSchedulerBatchSize    = 100
	defaultSchedulerPollInterval = time.Second
	defaultSchedulerRetryDelay   = time.Second
)

type DelayTaskHandler interface {
	HandleDelayTask(context.Context, task.DelayTask) error
}

type DelayTaskHandlerFunc func(context.Context, task.DelayTask) error

func (f DelayTaskHandlerFunc) HandleDelayTask(ctx context.Context, item task.DelayTask) error {
	return f(ctx, item)
}

type SchedulerOptions struct {
	BatchSize    int
	PollInterval time.Duration
	RetryDelay   time.Duration
	Handler      DelayTaskHandler
	Now          func() time.Time
}

type waitGroupContext interface {
	context.Context
	WaitGroupAdd(int)
	WaitGroupDone()
}

type Scheduler struct {
	backend      task.DelayTaskBackend
	batchSize    int
	pollInterval time.Duration
	retryDelay   time.Duration
	handler      DelayTaskHandler
	now          func() time.Time
}

func NewScheduler(backend task.DelayTaskBackend) *Scheduler {
	return NewSchedulerWithOptions(backend, SchedulerOptions{})
}

func NewSchedulerWithOptions(backend task.DelayTaskBackend, options SchedulerOptions) *Scheduler {
	batchSize := options.BatchSize
	if batchSize <= 0 {
		batchSize = defaultSchedulerBatchSize
	}
	pollInterval := options.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultSchedulerPollInterval
	}
	retryDelay := options.RetryDelay
	if retryDelay <= 0 {
		retryDelay = defaultSchedulerRetryDelay
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Scheduler{
		backend:      backend,
		batchSize:    batchSize,
		pollInterval: pollInterval,
		retryDelay:   retryDelay,
		handler:      options.Handler,
		now:          now,
	}
}

func (s *Scheduler) Schedule(ctx context.Context, item task.DelayTask) error {
	if s == nil || isNilFusionDependency(s.backend) {
		return ErrTaskBackendNotConfigured
	}
	return s.backend.Enqueue(ctx, item)
}

// Start 使用生命周期上下文启动后台消费循环。启动依赖缺失时返回错误，避免调用方
// 误以为延迟回写已经运行。
func (s *Scheduler) Start(ctx waitGroupContext) error {
	if s == nil || isNilFusionDependency(s.backend) {
		return ErrTaskBackendNotConfigured
	}
	if isNilFusionDependency(ctx) {
		return ErrSchedulerContextNotConfigured
	}
	ctx.WaitGroupAdd(1)
	go func() {
		defer ctx.WaitGroupDone()
		s.run(ctx)
	}()
	return nil
}

func (s *Scheduler) RunOnce(ctx context.Context, now time.Time) (int, error) {
	if s == nil || isNilFusionDependency(s.backend) {
		return 0, ErrTaskBackendNotConfigured
	}
	leases, err := s.backend.ClaimDue(ctx, now, s.batchSize)
	if err != nil {
		return 0, err
	}
	for _, lease := range leases {
		if err := s.handle(ctx, lease.Task); err != nil {
			if retryErr := s.backend.Retry(ctx, lease, now.Add(s.retryDelay), err); retryErr != nil {
				return len(leases), retryErr
			}
			txlog.ErrorContext(ctx, "delay task failed, retry scheduled",
				"taskType", lease.Task.Type,
				"taskID", lease.Task.ID,
				"action", lease.Task.Action,
				"attempts", lease.Attempts,
				"retryAt", now.Add(s.retryDelay),
				"err", err,
			)
			continue
		}
		if err := s.backend.Complete(ctx, lease); err != nil {
			return len(leases), err
		}
	}
	return len(leases), nil
}

func (s *Scheduler) run(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		if _, err := s.RunOnce(ctx, s.now()); err != nil {
			if ctx.Err() != nil {
				return
			}
			txlog.ErrorContext(ctx, "delay task scheduler failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) handle(ctx context.Context, item task.DelayTask) error {
	if isNilFusionDependency(s.handler) {
		return ErrTaskHandlerNotConfigured
	}
	return s.handler.HandleDelayTask(ctx, item)
}
