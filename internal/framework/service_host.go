package framework

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
)

type serviceHost struct {
	registry *registry
	runtime  *runtime

	mu        sync.Mutex
	started   []startedService
	drainDone chan struct{}
	drainErr  error
}

type startedService struct {
	service Service
	ingress *lifecycle.Ingress
	tasks   *lifecycle.ChildWaitGroupContext
}

func newServiceHost(registry *registry, runtime *runtime) *serviceHost {
	return &serviceHost{registry: registry, runtime: runtime}
}

func (host *serviceHost) start(expression string) error {
	services, err := host.registry.selectServices(expression)
	if err != nil {
		return err
	}
	return host.startServices(services)
}

func (host *serviceHost) startServices(services []Service) error {
	if host == nil || host.runtime == nil || host.runtime.lifecycle == nil {
		return errors.New("framework: runtime lifecycle is not configured")
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.started) > 0 {
		return errors.New("framework: services are already started")
	}
	for _, service := range services {
		spec := service.Spec()
		for _, required := range spec.Requires {
			if !host.runtime.hasCMod(required) {
				return host.rollbackStart(fmt.Errorf("framework: service %q requires unavailable CMod %q", spec.Name, required))
			}
		}
		ingress := lifecycle.NewIngress(host.runtime.lifecycle)
		tasks := lifecycle.NewChildWaitGroupContext(host.runtime.lifecycle)
		current := startedService{service: service, ingress: ingress, tasks: tasks}
		serviceTasks := taskAccess{Context: tasks, root: tasks.WaitGroupContext}
		if err := service.Start(newServiceContextWithTasks(host.runtime, ingress, serviceTasks)); err != nil {
			startErr := fmt.Errorf("framework: start service %q: %w", spec.Name, err)
			if stopErr := rollbackService(context.Background(), current); stopErr != nil {
				startErr = errors.Join(startErr, stopErr)
			}
			return host.rollbackStart(startErr)
		}
		host.started = append(host.started, current)
	}
	return nil
}

func (host *serviceHost) rollbackStart(startErr error) error {
	var errs []error
	for index := len(host.started) - 1; index >= 0; index-- {
		if err := rollbackService(context.Background(), host.started[index]); err != nil {
			errs = append(errs, err)
		}
	}
	host.started = nil
	return errors.Join(append([]error{startErr}, errs...)...)
}

func (host *serviceHost) drain(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	host.mu.Lock()
	services := append([]startedService(nil), host.started...)
	if host.drainDone != nil {
		done := host.drainDone
		host.mu.Unlock()
		select {
		case <-done:
			host.mu.Lock()
			err := host.drainErr
			host.mu.Unlock()
			return errors.Join(err, waitServiceIngress(ctx, services))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	host.drainDone = make(chan struct{})
	done := host.drainDone
	host.mu.Unlock()

	var errs []error
	for index := len(services) - 1; index >= 0; index-- {
		services[index].ingress.BeginDrain()
	}
	for index := len(services) - 1; index >= 0; index-- {
		if drainer, ok := services[index].service.(Drainer); ok {
			if err := drainer.Drain(ctx); err != nil {
				errs = append(errs, fmt.Errorf("framework: drain service %q: %w", services[index].service.Spec().Name, err))
			}
		}
	}
	host.mu.Lock()
	host.drainErr = errors.Join(errs...)
	close(done)
	err := host.drainErr
	host.mu.Unlock()
	return errors.Join(err, waitServiceIngress(ctx, services))
}

func waitServiceIngress(ctx context.Context, services []startedService) error {
	var errs []error
	for index := len(services) - 1; index >= 0; index-- {
		if err := services[index].ingress.WaitContext(ctx); err != nil {
			errs = append(errs, fmt.Errorf(
				"framework: drain service %q with pending %d: %w",
				services[index].service.Spec().Name,
				services[index].ingress.Pending(),
				err,
			))
		}
	}
	return errors.Join(errs...)
}

func (host *serviceHost) stop(ctx context.Context) error {
	if host == nil {
		return nil
	}
	drainErr := host.drain(ctx)
	host.mu.Lock()
	services := append([]startedService(nil), host.started...)
	host.started = nil
	host.mu.Unlock()
	var errs []error
	for index := len(services) - 1; index >= 0; index-- {
		if err := stopService(ctx, services[index], "stop"); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(append([]error{drainErr}, errs...)...)
}

func stopService(ctx context.Context, current startedService, operation string) error {
	if current.tasks != nil {
		current.tasks.Cancel()
		current.tasks.WaitDoneAndWaitGroupWait()
	}
	return stopServiceResources(ctx, current, operation)
}

func rollbackService(ctx context.Context, current startedService) error {
	if current.tasks != nil {
		current.tasks.Cancel()
	}
	err := stopServiceResources(ctx, current, "rollback")
	if current.tasks != nil {
		current.tasks.WaitDoneAndWaitGroupWait()
	}
	return err
}

func stopServiceResources(ctx context.Context, current startedService, operation string) error {
	stopper, ok := current.service.(Stopper)
	if !ok {
		return nil
	}
	if err := stopper.Stop(ctx); err != nil {
		return fmt.Errorf("framework: %s service %q: %w", operation, current.service.Spec().Name, err)
	}
	return nil
}
