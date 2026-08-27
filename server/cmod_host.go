package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
)

type startedCMod struct {
	mod   CMod
	tasks *lifecycle.ChildWaitGroupContext
}

type cmodHost struct {
	context CModContext
	graph   cmodGraph

	mu      sync.Mutex
	started []startedCMod
	drained bool
	stopped bool
}

func newCModHost(context CModContext, graph cmodGraph) *cmodHost {
	return &cmodHost{context: context, graph: graph}
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
		started := startedCMod{mod: mod}
		startContext := host.context
		if context, ok := host.context.(*cmodContext); ok && context.runtime != nil && context.runtime.lifecycle != nil {
			started.tasks = lifecycle.NewChildWaitGroupContext(context.runtime.lifecycle)
			startContext = newCModContextWithTasks(context.server, taskAccess{
				Context: started.tasks,
				root:    started.tasks.WaitGroupContext,
			})
		}
		if err := mod.Start(startContext); err != nil {
			startErr := fmt.Errorf("server: start CMod %q: %w", mod.Spec().ID, err)
			stopCModTasks(started)
			if stopper, ok := mod.(CModStopper); ok {
				if stopErr := stopper.Stop(context.Background()); stopErr != nil {
					startErr = errors.Join(startErr, fmt.Errorf("server: rollback CMod %q: %w", mod.Spec().ID, stopErr))
				}
			}
			return host.rollback(startErr)
		}
		host.started = append(host.started, started)
	}
	return nil
}

func (host *cmodHost) rollback(startErr error) error {
	errs := []error{startErr}
	for index := len(host.started) - 1; index >= 0; index-- {
		started := host.started[index]
		mod := started.mod
		stopCModTasks(started)
		if stopper, ok := mod.(CModStopper); ok {
			if err := stopper.Stop(context.Background()); err != nil {
				errs = append(errs, fmt.Errorf("server: rollback CMod %q: %w", mod.Spec().ID, err))
			}
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
	started := append([]startedCMod(nil), host.started...)
	host.mu.Unlock()

	var errs []error
	for index := len(started) - 1; index >= 0; index-- {
		mod := started[index].mod
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
	started := append([]startedCMod(nil), host.started...)
	host.started = nil
	host.mu.Unlock()

	var errs []error
	for index := len(started) - 1; index >= 0; index-- {
		current := started[index]
		mod := current.mod
		stopCModTasks(current)
		if stopper, ok := mod.(CModStopper); ok {
			if err := stopper.Stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("server: stop CMod %q: %w", mod.Spec().ID, err))
			}
		}
	}
	return errors.Join(errs...)
}

func stopCModTasks(started startedCMod) {
	if started.tasks == nil {
		return
	}
	started.tasks.Cancel()
	started.tasks.WaitDoneAndWaitGroupWait()
}
