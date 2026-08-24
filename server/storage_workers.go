package server

import (
	"context"
	"errors"
)

var (
	errStorageNotConfigured = errors.New("server: storage is not configured")
	errWorkerNotConfigured  = errors.New("server: delayed task worker is not configured")
	errDispatcherMissing    = errors.New("server: fusion delay task dispatcher is not configured")
	errWorkerContextMissing = errors.New("server: worker lifecycle context is not configured")
)

type workerContext interface {
	context.Context
	Go(func(context.Context)) error
}

func (s *storageRuntime) startDelayTaskDispatcher(ctx workerContext) error {
	if s == nil {
		return errStorageNotConfigured
	}
	if ctx == nil {
		return errWorkerContextMissing
	}
	if s.delayDispatcher == nil {
		return errDispatcherMissing
	}
	return s.delayDispatcher.Start(ctx)
}

func (s *storageRuntime) drainDelayTaskDispatcher(ctx context.Context) error {
	if s == nil {
		return errStorageNotConfigured
	}
	if s.delayDispatcher == nil {
		return errDispatcherMissing
	}
	return s.delayDispatcher.Drain(ctx)
}

func (s *storageRuntime) startDelayScheduler(ctx workerContext) error {
	if s == nil {
		return errStorageNotConfigured
	}
	if ctx == nil {
		return errWorkerContextMissing
	}
	if s.delayWorker == nil {
		return errWorkerNotConfigured
	}
	return s.delayWorker.Start(ctx)
}
