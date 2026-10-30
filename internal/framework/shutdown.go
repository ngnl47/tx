package framework

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/component"
	"github.com/ngnl47/tx/modules/discovery"
)

var errStopDeferred = errors.New("framework: shutdown deferred; resources retained for retry")

// Drain 先关闭服务和 Actor 准入，再摘除普通路由并等待已接受的工作完成。
// 它不会取消后台任务、完成最终写回或关闭 RPC；槽位交接应先调用 PrepareHandoff。
func (server *Server) Drain(ctx context.Context) error {
	if server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	server.mu.Lock()
	ready := server.started && !server.finalizing
	runtime := server.runtime
	discoveryRuntime := server.discovery
	host := server.host
	server.mu.Unlock()
	if !ready {
		return nil
	}
	host.beginDrain()
	var managementErr error
	if runtime != nil {
		managementErr = runtime.stopEntityCacheManagement(ctx, false)
	}
	if runtime != nil && runtime.placement != nil {
		runtime.placement.ingress.BeginDrain()
	}
	if err := server.drainTaskConsumer(ctx); err != nil {
		return err
	}
	if runtime != nil && runtime.entities != nil {
		runtime.entities.BeginDrain()
	}
	var actorErr error
	var discoveryErr error
	if discoveryRuntime != nil {
		discoveryErr = discoveryRuntime.MarkDraining(ctx)
	}
	if runtime != nil {
		runtime.setInstanceStatus(discovery.StatusDraining)
	}
	var hostErr error
	if host != nil {
		hostErr = host.drain(ctx)
	}
	if runtime != nil && runtime.entities != nil {
		actorErr = runtime.entities.Drain(ctx)
	}
	return errors.Join(managementErr, discoveryErr, hostErr, actorErr)
}

// Stop 具备幂等性，并负责完整的逆序关闭流程。
func (server *Server) Stop(ctx context.Context) error {
	return server.stop(ctx, true)
}

func (server *Server) stop(ctx context.Context, drain bool) error {
	if server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		server.mu.Lock()
		if server.starting {
			done := server.startDone
			server.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if server.stopping {
			done := server.stopDone
			attempt := server.stopAttempt
			if attempt != nil {
				done = attempt.done
			}
			server.mu.Unlock()
			select {
			case <-done:
				if attempt != nil {
					return attempt.err
				}
				server.mu.Lock()
				err := server.stopErr
				server.mu.Unlock()
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if server.prepareAttempt != nil {
			done := server.prepareAttempt.done
			server.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		server.stopping = true
		server.stopAttempt = &drainAttempt{done: make(chan struct{})}
		server.mu.Unlock()
		break
	}

	err := server.shutdown(ctx, drain)
	server.mu.Lock()
	server.stopErr = err
	server.stopAttempt.err = err
	close(server.stopAttempt.done)
	if errors.Is(err, errStopDeferred) {
		server.stopping = false
	} else {
		server.clearRuntimeLocked()
		close(server.stopDone)
	}
	server.mu.Unlock()
	return err
}

func (server *Server) shutdown(ctx context.Context, drain bool) error {
	server.mu.Lock()
	finalizing := server.finalizing
	server.mu.Unlock()
	if !finalizing {
		err := server.finishWork(ctx, drain)
		if errors.Is(err, errStopDeferred) {
			return err
		}
		server.mu.Lock()
		server.finalizing, server.finalizeErr = true, err
		server.mu.Unlock()
	}
	server.mu.Lock()
	runtime := server.runtime
	host := server.host
	cmodHost := server.cmodHost
	discoveryRuntime := server.discovery
	controlServer := server.control
	err := server.finalizeErr
	server.mu.Unlock()

	if runtime != nil {
		if err := runtime.stopEntityCacheManagement(ctx, true); err != nil {
			return errors.Join(errStopDeferred, err)
		}
	}
	errs := []error{err}
	if !server.releaseDone {
		if controlServer != nil {
			controlServer.stop()
		}
		// 服务先关闭自身发送器；此时进程级传输入口仍需有效。
		if host != nil {
			stopErr := host.stop(ctx)
			if errors.Is(stopErr, errStopDeferred) {
				return errors.Join(err, stopErr)
			}
			errs = append(errs, stopErr)
		}
		server.mu.Lock()
		server.finalizeErr = errors.Join(errs...)
		server.mu.Unlock()
		// 根任务实际退出前保留 CMod 连接和存储；重试不再排水或 Flush。
		if runtime != nil && runtime.lifecycle != nil {
			waitCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
			waitErr := runtime.lifecycle.CancelAndWait(waitCtx)
			cancel()
			if waitErr != nil {
				return errors.Join(errStopDeferred, errors.Join(errs...), waitErr)
			}
		}
		if runtime != nil && runtime.actorRPC != nil {
			runtime.actorRPC.Close()
		}
		if runtime != nil && runtime.rpcSender != nil {
			errs = append(errs, runtime.rpcSender.Close())
		}
		for index := len(server.cleanups) - 1; index >= 0; index-- {
			server.cleanups[index]()
		}
		server.cleanups = nil
		if server.bindStorage != nil {
			server.bindStorage(nil)
		}
		if runtime != nil {
			runtime.setInstanceStatus(discovery.StatusStopping)
		}
		if discoveryRuntime != nil {
			statusCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
			errs = append(errs, discoveryRuntime.MarkStopping(statusCtx))
			cancel()
		}
		server.mu.Lock()
		server.releaseDone = true
		server.finalizeErr = errors.Join(errs...)
		server.mu.Unlock()
	}
	if cmodHost != nil {
		stopCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		stopErr := cmodHost.stop(stopCtx)
		cancel()
		if errors.Is(stopErr, component.ErrStopDeferred) {
			server.mu.Lock()
			server.finalizeErr = errors.Join(errs...)
			server.mu.Unlock()
			return errors.Join(errStopDeferred, errors.Join(errs...), stopErr)
		}
		errs = append(errs, stopErr)
	} else if discoveryRuntime != nil {
		closeCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		errs = append(errs, discoveryRuntime.Close(closeCtx))
		cancel()
	}
	server.mu.Lock()
	fatalErr := server.fatalErr
	server.mu.Unlock()
	return errors.Join(errors.Join(errs...), fatalErr)
}

// finishWork 由交接准备与最终关闭串行复用；成功的组件阶段由 component.Host 记住。
// 这里停止业务任务、冲刷状态并确认传输输出；租约、存储绑定及连接留给 shutdown 释放。
func (server *Server) finishWork(ctx context.Context, drain bool) error {
	server.mu.Lock()
	host, cmodHost, runtime := server.host, server.cmodHost, server.runtime
	prepared, fatal, ready := server.prepared, server.fatalErr != nil, server.started
	server.mu.Unlock()
	if prepared {
		return nil
	}
	var errs []error
	if drain {
		drainCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		err := server.Drain(drainCtx)
		cancel()
		if err != nil && !fatal {
			return errors.Join(errStopDeferred, err)
		}
		errs = append(errs, err)
	}
	if !ready {
		if host != nil {
			if err := host.retryInitializationRollback(ctx); err != nil {
				return errors.Join(errStopDeferred, err)
			}
			host.beginDrain()
		}
		if err := server.drainTaskConsumer(ctx); err != nil && !fatal {
			return errors.Join(errStopDeferred, err)
		}
	}
	if !ready && runtime != nil && runtime.entities != nil {
		// 启动失败也可能已有定向消息到达；卸载完成前不能释放业务依赖。
		drainCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		err := runtime.entities.Drain(drainCtx)
		cancel()
		if err != nil && !fatal {
			return errors.Join(errStopDeferred, err)
		}
		errs = append(errs, err)
		drain = true // 后续写回或传输失败同样保留启动资源，复用 Stop 重试。
	}
	if runtime != nil {
		if err := runtime.stopEntityCacheManagement(ctx, false); err != nil {
			return errors.Join(errStopDeferred, err)
		}
	}
	if host != nil {
		tasksCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		err := host.quiesceTasks(tasksCtx)
		cancel()
		if err != nil {
			return errors.Join(errStopDeferred, err)
		}
	}
	if runtime != nil && runtime.placement != nil {
		placementCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		err := runtime.placement.close(placementCtx)
		cancel()
		if err != nil {
			return errors.Join(errStopDeferred, err)
		}
	}

	if host != nil {
		if err := host.flush(ctx); err != nil {
			if !fatal {
				return errors.Join(errStopDeferred, err)
			}
			errs = append(errs, err)
		}
	}
	if cmodHost != nil {
		err := cmodHost.drain(ctx)
		if err != nil && !fatal {
			return errors.Join(errStopDeferred, err)
		}
		errs = append(errs, err)
	}
	// 已完成的写回阶段由 cmodHost 保留；传输确认失败只重试确认，不重复写回。
	if runtime != nil && runtime.actorRPC != nil {
		flushCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		err := runtime.actorRPC.Flush(flushCtx)
		cancel()
		if err != nil && drain && !fatal {
			return errors.Join(errStopDeferred, err)
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
