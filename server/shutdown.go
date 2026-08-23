package server

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/discovery"
)

// Drain removes the instance from ordinary routing and waits for already
// accepted service ingress. It does not cancel background work or close RPC.
func (server *Server) Drain(ctx context.Context) error {
	if server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	server.mu.Lock()
	runtime := server.runtime
	discoveryRuntime := server.discovery
	host := server.host
	server.mu.Unlock()
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

// Stop is idempotent and owns the full reverse-order shutdown sequence.
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
	server.mu.Lock()
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
	runtime := server.runtime
	host := server.host
	discoveryRuntime := server.discovery
	controlServer := server.control
	server.mu.Unlock()

	var errs []error
	if controlServer != nil {
		controlServer.stop()
	}
	if drain {
		errs = append(errs, server.Drain(ctx))
	}
	if host != nil {
		errs = append(errs, host.stop(ctx))
	}
	if runtime != nil && runtime.rpc != nil {
		errs = append(errs, runtime.rpc.Stop(ctx))
	}
	if runtime != nil && runtime.lifecycle != nil {
		runtime.lifecycle.Cancel()
		runtime.lifecycle.WaitDoneAndWaitGroupWait()
	}
	if runtime != nil && runtime.transaction != nil {
		errs = append(errs, runtime.transaction.Shutdown(context.WithoutCancel(ctx)))
	}
	if runtime != nil && runtime.actor != nil {
		errs = append(errs, runtime.actor.Shutdown())
	}
	if runtime != nil {
		runtime.setInstanceStatus(discovery.StatusStopping)
	}
	if discoveryRuntime != nil {
		errs = append(errs, discoveryRuntime.markStopping(context.WithoutCancel(ctx)))
		errs = append(errs, discoveryRuntime.close(context.WithoutCancel(ctx)))
	}
	for index := len(server.cleanups) - 1; index >= 0; index-- {
		server.cleanups[index]()
	}
	server.bindStorage(nil)
	if runtime != nil && runtime.storage != nil {
		errs = append(errs, runtime.storage.close(context.WithoutCancel(ctx)))
	}

	server.mu.Lock()
	server.stopErr = errors.Join(errs...)
	close(server.stopDone)
	err := server.stopErr
	server.mu.Unlock()
	return err
}
