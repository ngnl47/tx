package fusion

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/storage/task"
)

const (
	defaultSchedulerBatchSize    = 100
	defaultSchedulerPollInterval = time.Second
	defaultRetryInitialDelay     = time.Second
	defaultRetryMultiplier       = 2
	defaultRetryMaxDelay         = time.Minute
	defaultRetryJitter           = 0.2
)

type DelayTaskHandler interface {
	HandleDelayTask(context.Context, task.DelayTask) error
}

type DelayTaskHandlerFunc func(context.Context, task.DelayTask) error

func (f DelayTaskHandlerFunc) HandleDelayTask(ctx context.Context, item task.DelayTask) error {
	return f(ctx, item)
}

type SchedulerOptions struct {
	BatchSize         int
	PollInterval      time.Duration
	RetryInitialDelay time.Duration
	RetryMultiplier   float64
	RetryMaxDelay     time.Duration
	RetryJitter       float64
	Handler           DelayTaskHandler
	Now               func() time.Time
	Random            func() float64
}

type waitGroupContext interface {
	context.Context
	Go(func(context.Context)) error
}

type Scheduler struct {
	backend           task.DelayTaskBackend
	batchSize         int
	pollInterval      time.Duration
	retryInitialDelay time.Duration
	retryMultiplier   float64
	retryMaxDelay     time.Duration
	retryJitter       float64
	handler           DelayTaskHandler
	now               func() time.Time
	random            func() float64
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
	retryInitialDelay := options.RetryInitialDelay
	if retryInitialDelay <= 0 {
		retryInitialDelay = defaultRetryInitialDelay
	}
	retryMultiplier := options.RetryMultiplier
	if retryMultiplier <= 1 {
		retryMultiplier = defaultRetryMultiplier
	}
	retryMaxDelay := options.RetryMaxDelay
	if retryMaxDelay <= 0 {
		retryMaxDelay = defaultRetryMaxDelay
	}
	if retryMaxDelay < retryInitialDelay {
		retryMaxDelay = retryInitialDelay
	}
	retryJitter := options.RetryJitter
	if retryJitter < 0 || retryJitter > 1 {
		retryJitter = defaultRetryJitter
	}
	random := options.Random
	if random == nil {
		random = rand.Float64
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Scheduler{
		backend:           backend,
		batchSize:         batchSize,
		pollInterval:      pollInterval,
		retryInitialDelay: retryInitialDelay,
		retryMultiplier:   retryMultiplier,
		retryMaxDelay:     retryMaxDelay,
		retryJitter:       retryJitter,
		handler:           options.Handler,
		now:               now,
		random:            random,
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
	return ctx.Go(func(context.Context) {
		s.run(ctx)
	})
}

func (s *Scheduler) RunOnce(ctx context.Context, now time.Time) (int, error) {
	if s == nil || isNilFusionDependency(s.backend) {
		return 0, ErrTaskBackendNotConfigured
	}
	leases, runErr := s.backend.ClaimDue(ctx, now, s.batchSize)
	for _, lease := range leases {
		if err := s.handle(ctx, lease.Task); err != nil {
			retryAt := now.Add(s.retryDelay(lease.Attempts))
			retryErr := s.backend.Retry(ctx, lease, retryAt, err)
			if errors.Is(retryErr, task.ErrLeaseLost) {
				// 旧任务可能已落库，不能把失效租约视为已重试。重新登记记录身份，
				// 由后续任务读取最新 Redis 文档；Enqueue 保留当前所有者及后续执行。
				retryAt = s.now().Add(s.retryDelay(lease.Attempts))
				item := lease.Task
				item.DueAt = retryAt
				retryErr = s.backend.Enqueue(ctx, item)
			}
			if retryErr != nil {
				runErr = errors.Join(runErr, fmt.Errorf(
					"settle delayed task type=%s id=%v action=%s: %w",
					lease.Task.Type,
					lease.Task.ID,
					lease.Task.Action,
					errors.Join(err, retryErr),
				))
				continue
			}
			txlog.ErrorContext(ctx, "delay task failed, retry scheduled",
				"taskType", lease.Task.Type,
				"taskID", lease.Task.ID,
				"action", lease.Task.Action,
				"attempts", lease.Attempts,
				"retryAt", retryAt,
				"err", err,
			)
			continue
		}
		if err := s.backend.Complete(ctx, lease); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf(
				"complete delayed task type=%s id=%v action=%s: %w",
				lease.Task.Type,
				lease.Task.ID,
				lease.Task.Action,
				err,
			))
		}
	}
	return len(leases), runErr
}

func (s *Scheduler) retryDelay(attempts int) time.Duration {
	delay := s.retryInitialDelay
	if attempts > 1 && delay < s.retryMaxDelay {
		multiplier := math.Pow(s.retryMultiplier, float64(attempts-1))
		if multiplier >= float64(s.retryMaxDelay)/float64(delay) {
			delay = s.retryMaxDelay
		} else {
			delay = time.Duration(float64(delay) * multiplier)
		}
	}
	if s.retryJitter == 0 {
		return delay
	}
	jitter := time.Duration((s.random()*2 - 1) * s.retryJitter * float64(delay))
	if delay+jitter <= 0 {
		return time.Nanosecond
	}
	if delay+jitter > s.retryMaxDelay {
		return s.retryMaxDelay
	}
	return delay + jitter
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
