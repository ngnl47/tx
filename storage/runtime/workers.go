package runtime

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/lifecycle"
)

var (
	errDispatcherMissing    = errors.New("storage runtime: fusion delay task dispatcher is not configured")
	errWorkerContextMissing = errors.New("storage runtime: worker lifecycle context is not configured")
)

// StartDelayTaskDispatcher 启动 Fusion 本地延迟任务登记器。
func (s *Runtime) StartDelayTaskDispatcher(ctx lifecycle.Tasks) error {
	if s == nil {
		return ErrNotConfigured
	}
	if ctx == nil {
		return errWorkerContextMissing
	}
	if s.delayDispatcher == nil {
		return errDispatcherMissing
	}
	return s.delayDispatcher.Start(ctx)
}

// DrainDelayTaskDispatcher 等待已接受的本地延迟任务登记完成。
func (s *Runtime) DrainDelayTaskDispatcher(ctx context.Context) error {
	if s == nil {
		return ErrNotConfigured
	}
	if s.delayDispatcher == nil {
		return errDispatcherMissing
	}
	return s.delayDispatcher.Drain(ctx)
}

// StartDelayScheduler 启动 Redis 延迟任务消费循环。
func (s *Runtime) StartDelayScheduler(ctx lifecycle.Tasks) error {
	if s == nil {
		return ErrNotConfigured
	}
	if ctx == nil {
		return errWorkerContextMissing
	}
	if s.delayWorker == nil {
		return ErrWorkerNotConfigured
	}
	return s.delayWorker.Start(ctx)
}
