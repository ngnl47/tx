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

// startBackgroundWorkers 启动存储后台任务。启动配置缺失必须返回错误，服务入口
// 据此停止启动，避免延迟回写没有运行却继续接收业务请求。
func (s *storageRuntime) startBackgroundWorkers(ctx workerContext) error {
	if s == nil {
		return errStorageNotConfigured
	}
	if ctx == nil {
		return errWorkerContextMissing
	}
	if s.delayWorker == nil {
		return errWorkerNotConfigured
	}
	if s.handoffWorker == nil {
		return errHandoffWorkerMissing
	}
	s.workerOnce.Do(func() {
		if err := s.handoffWorker.Start(ctx); err != nil {
			s.workerStartErr = err
			return
		}
		s.workerStartErr = s.delayWorker.Start(ctx)
	})
	return s.workerStartErr
}
