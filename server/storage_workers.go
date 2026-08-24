package server

import (
	"context"
	"errors"
)

var (
	errStorageNotConfigured = errors.New("server: storage is not configured")
	errWorkerNotConfigured  = errors.New("server: delayed task worker is not configured")
	errHandoffWorkerMissing = errors.New("server: fusion handoff recovery worker is not configured")
	errWorkerContextMissing = errors.New("server: worker lifecycle context is not configured")
)

type workerContext interface {
	context.Context
	Go(func(context.Context)) error
}

func (s *storageRuntime) startHandoffWorker(ctx workerContext) error {
	if s == nil {
		return errStorageNotConfigured
	}
	if ctx == nil {
		return errWorkerContextMissing
	}
	if s.handoffWorker == nil {
		return errHandoffWorkerMissing
	}
	return s.handoffWorker.Start(ctx)
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
