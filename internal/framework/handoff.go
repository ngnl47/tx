package framework

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/control"
)

// PrepareHandoff 等待已接受工作、业务任务及最终写回完成，保留通信和租约供逐槽交接。
// 调用方必须携带源启动身份；失败或响应未知不得晋升槽位，可对同一源显式重试。
// 与 Stop 串行共享收尾进度；它不执行 CAS、不重新开放业务，也不保证异常接管。
func (server *Server) PrepareHandoff(ctx context.Context, expectedInstanceID string) error {
	if server == nil {
		return control.ErrHandoffUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	server.mu.Lock()
	if !server.started || server.stopping || server.fatalErr != nil {
		server.mu.Unlock()
		return control.ErrHandoffUnavailable
	}
	if expectedInstanceID == "" || expectedInstanceID != server.runtime.instanceSnapshot().ID {
		server.mu.Unlock()
		return control.ErrHandoffInstanceMismatch
	}
	if server.prepared {
		server.mu.Unlock()
		return nil
	}
	if current := server.prepareAttempt; current != nil {
		server.mu.Unlock()
		select {
		case <-current.done:
			return current.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	attempt := &drainAttempt{done: make(chan struct{})}
	server.prepareAttempt = attempt
	server.mu.Unlock()

	err := server.finishWork(ctx, true)
	server.mu.Lock()
	// 持久化成功后调用方可能已经取消，或身份在准备期间丢失；都不能返回交接许可。
	err = errors.Join(err, ctx.Err(), server.fatalErr)
	server.prepared = err == nil
	attempt.err = err
	close(attempt.done)
	server.prepareAttempt = nil
	server.mu.Unlock()
	return err
}
