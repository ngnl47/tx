package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type cmodHost struct {
	context CModContext
	graph   cmodGraph
	mark    func(CModID, bool)

	mu      sync.Mutex
	started []CMod
	drained bool
	stopped bool
}

func newCModHost(context CModContext, graph cmodGraph, mark func(CModID, bool)) *cmodHost {
	return &cmodHost{context: context, graph: graph, mark: mark}
}

func (host *cmodHost) start() error {
	if host == nil {
		return errors.New("server: CMod host is nil")
	}
	for _, mod := range host.graph.ordered {
		if validator, ok := mod.(CModValidator); ok {
			if err := validator.Validate(host.context); err != nil {
				return fmt.Errorf("server: validate CMod %q: %w", mod.Spec().ID, err)
			}
		}
	}
	for _, mod := range host.graph.ordered {
		if err := mod.Start(host.context); err != nil {
			startErr := fmt.Errorf("server: start CMod %q: %w", mod.Spec().ID, err)
			if stopper, ok := mod.(CModStopper); ok {
				if stopErr := stopper.Stop(context.Background()); stopErr != nil {
					startErr = errors.Join(startErr, fmt.Errorf("server: rollback CMod %q: %w", mod.Spec().ID, stopErr))
				}
			}
			return host.rollback(startErr)
		}
		host.started = append(host.started, mod)
		if host.mark != nil {
			host.mark(mod.Spec().ID, true)
		}
	}
	return nil
}

func (host *cmodHost) rollback(startErr error) error {
	errs := []error{startErr}
	for index := len(host.started) - 1; index >= 0; index-- {
		mod := host.started[index]
		if stopper, ok := mod.(CModStopper); ok {
			if err := stopper.Stop(context.Background()); err != nil {
				errs = append(errs, fmt.Errorf("server: rollback CMod %q: %w", mod.Spec().ID, err))
			}
		}
		if host.mark != nil {
			host.mark(mod.Spec().ID, false)
		}
	}
	host.started = nil
	return errors.Join(errs...)
}

func (host *cmodHost) drain(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	host.mu.Lock()
	if host.drained {
		host.mu.Unlock()
		return nil
	}
	host.drained = true
	started := append([]CMod(nil), host.started...)
	host.mu.Unlock()

	var errs []error
	for index := len(started) - 1; index >= 0; index-- {
		mod := started[index]
		if drainer, ok := mod.(CModDrainer); ok {
			if err := drainer.Drain(ctx); err != nil {
				errs = append(errs, fmt.Errorf("server: drain CMod %q: %w", mod.Spec().ID, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (host *cmodHost) stop(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	host.mu.Lock()
	if host.stopped {
		host.mu.Unlock()
		return nil
	}
	host.stopped = true
	started := append([]CMod(nil), host.started...)
	host.started = nil
	host.mu.Unlock()

	var errs []error
	for index := len(started) - 1; index >= 0; index-- {
		mod := started[index]
		if stopper, ok := mod.(CModStopper); ok {
			if err := stopper.Stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("server: stop CMod %q: %w", mod.Spec().ID, err))
			}
		}
		if host.mark != nil {
			host.mark(mod.Spec().ID, false)
		}
	}
	return errors.Join(errs...)
}
