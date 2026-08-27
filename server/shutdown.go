package server

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/discovery"
)

// Drain 将实例移出普通路由，并等待服务入口已经接受的工作完成。
// 它不会取消后台任务或关闭 RPC。
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
	var discoveryErr error
	if discoveryRuntime != nil {
		discoveryErr = discoveryRuntime.markDraining(ctx)
	}
	if runtime != nil {
		runtime.setInstanceStatus(discovery.StatusDraining)
	}
	var hostErr error
	if host != nil {
		hostErr = host.drain(ctx)
	}
	return errors.Join(discoveryErr, hostErr)
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
			server.mu.Unlock()
			select {
			case <-done:
				server.mu.Lock()
				err := server.stopErr
				server.mu.Unlock()
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		server.stopping = true
		server.mu.Unlock()
		break
	}

	err := server.shutdown(ctx, drain)
	server.mu.Lock()
	server.stopErr = err
	server.clearRuntimeLocked()
	close(server.stopDone)
	server.mu.Unlock()
	return err
}

func (server *Server) shutdown(ctx context.Context, drain bool) error {
	server.mu.Lock()
	runtime := server.runtime
	host := server.host
	cmodHost := server.cmodHost
	discoveryRuntime := server.discovery
	controlServer := server.control
	server.mu.Unlock()

	var errs []error
	if controlServer != nil {
		controlServer.stop()
	}
	if drain {
		errs = append(errs, server.Drain(ctx))
		if runtime != nil && runtime.actor != nil {
			actorDrainCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
			errs = append(errs, runtime.actor.Drain(actorDrainCtx))
			cancel()
		}
	}
	if host != nil {
		errs = append(errs, host.stop(ctx))
	}
	for index := len(server.cleanups) - 1; index >= 0; index-- {
		server.cleanups[index]()
	}
	if server.bindStorage != nil {
		server.bindStorage(nil)
	}
	if cmodHost != nil {
		errs = append(errs, cmodHost.drain(ctx))
	}
	if runtime != nil {
		runtime.setInstanceStatus(discovery.StatusStopping)
	}
	if discoveryRuntime != nil {
		errs = append(errs, discoveryRuntime.markStopping(context.WithoutCancel(ctx)))
	}
	if runtime != nil && runtime.lifecycle != nil {
		runtime.lifecycle.Cancel()
		runtime.lifecycle.WaitDoneAndWaitGroupWait()
	}
	if cmodHost != nil {
		errs = append(errs, cmodHost.stop(context.WithoutCancel(ctx)))
	} else if discoveryRuntime != nil {
		errs = append(errs, discoveryRuntime.close(context.WithoutCancel(ctx)))
	}
	return errors.Join(errs...)
}
