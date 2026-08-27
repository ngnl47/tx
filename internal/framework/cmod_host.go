package framework

import (
	"context"
	"errors"
	"fmt"

	"github.com/ngnl47/tx/component"
	"github.com/ngnl47/tx/lifecycle"
)

type cmodHost struct {
	host       *component.Host
	resolveErr error
}

func newCModHost(runtime CModContext, graph cmodGraph) *cmodHost {
	components := make([]component.Component, 0, len(graph.ordered))
	roots := make([]component.Root, 0, len(graph.ordered))
	for _, mod := range graph.ordered {
		adapter := &cmodLifecycleComponent{mod: mod, runtime: runtime}
		components = append(components, adapter)
		roots = append(roots, component.Root{Source: "server", ID: component.ID(mod.Spec().ID)})
	}
	resolved, err := component.Resolve(components, roots)
	if err != nil {
		return &cmodHost{resolveErr: fmt.Errorf("framework: prepare CMod host: %w", err)}
	}
	return &cmodHost{host: component.NewHost(cmodRootLifecycle(runtime), resolved)}
}

func (host *cmodHost) start() error {
	if host == nil {
		return errors.New("framework: CMod host is nil")
	}
	if host.resolveErr != nil {
		return host.resolveErr
	}
	return host.host.Start()
}

func (host *cmodHost) drain(ctx context.Context) error {
	if host == nil || host.host == nil {
		return nil
	}
	return host.host.Drain(ctx)
}

func (host *cmodHost) stop(ctx context.Context) error {
	if host == nil || host.host == nil {
		return nil
	}
	return host.host.Stop(ctx)
}

type cmodLifecycleComponent struct {
	mod     CMod
	runtime CModContext
}

func (current *cmodLifecycleComponent) Spec() component.Spec {
	spec := current.mod.Spec()
	return component.Spec{
		ID:       component.ID(spec.ID),
		Requires: componentIDs(spec.Requires),
		After:    componentIDs(spec.After),
	}
}

func (current *cmodLifecycleComponent) Validate(runtime component.Context) error {
	validator, ok := current.mod.(CModValidator)
	if !ok {
		return nil
	}
	return validator.Validate(current.context(runtime.Tasks()))
}

func (current *cmodLifecycleComponent) Start(runtime component.Context) error {
	return current.mod.Start(current.context(runtime.Tasks()))
}

func (current *cmodLifecycleComponent) Drain(ctx context.Context) error {
	drainer, ok := current.mod.(CModDrainer)
	if !ok {
		return nil
	}
	return drainer.Drain(ctx)
}

func (current *cmodLifecycleComponent) Stop(ctx context.Context) error {
	stopper, ok := current.mod.(CModStopper)
	if !ok {
		return nil
	}
	return stopper.Stop(ctx)
}

func (current *cmodLifecycleComponent) context(tasks lifecycle.Tasks) CModContext {
	if runtime, ok := current.runtime.(*cmodContext); ok && runtime != nil {
		return newCModContextWithTasks(runtime.server, tasks)
	}
	return current.runtime
}

func cmodRootLifecycle(runtime CModContext) *lifecycle.WaitGroupContext {
	current, ok := runtime.(*cmodContext)
	if !ok || current == nil || current.runtime == nil {
		return nil
	}
	return current.runtime.lifecycle
}
