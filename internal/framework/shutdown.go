package framework

import (
	"context"
	"errors"

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
	ready := server.started
	runtime := server.runtime
	discoveryRuntime := server.discovery
	host := server.host
	server.mu.Unlock()
	if !ready {
		return nil
	}
	host.beginDrain()
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
	return errors.Join(discoveryErr, hostErr, actorErr)
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
	err := server.finishWork(ctx, drain)
	if errors.Is(err, errStopDeferred) {
		return err
	}
	server.mu.Lock()
	runtime := server.runtime
	host := server.host
	cmodHost := server.cmodHost
	discoveryRuntime := server.discovery
	controlServer := server.control
	server.mu.Unlock()

	errs := []error{err}
	if controlServer != nil {
		controlServer.stop()
	}
	if host != nil {
		errs = append(errs, host.stop(ctx))
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
	if server.bindStorage != nil {
		server.bindStorage(nil)
	}
	if runtime != nil {
		runtime.setInstanceStatus(discovery.StatusStopping)
	}
	if discoveryRuntime != nil {
		errs = append(errs, discoveryRuntime.MarkStopping(context.WithoutCancel(ctx)))
	}
	if runtime != nil && runtime.lifecycle != nil {
		runtime.lifecycle.Cancel()
		runtime.lifecycle.WaitDoneAndWaitGroupWait()
	}
	if cmodHost != nil {
		errs = append(errs, cmodHost.stop(context.WithoutCancel(ctx)))
	} else if discoveryRuntime != nil {
		errs = append(errs, discoveryRuntime.Close(context.WithoutCancel(ctx)))
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
	if host != nil {
		host.quiesceTasks()
	}
	if runtime != nil && runtime.placement != nil {
		placementCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
		err := runtime.placement.close(placementCtx)
		cancel()
		if err != nil {
			return errors.Join(errStopDeferred, err)
		}
	}

	if cmodHost != nil {
		err := cmodHost.drain(ctx)
		if err != nil && drain && !fatal {
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
