package server

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
		return errors.New("server: runtime lifecycle is not configured")
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.started) > 0 {
		return errors.New("server: services are already started")
	}
	for _, service := range services {
		spec := service.Spec()
		if missing := spec.Requires &^ host.runtime.capabilities; missing != 0 {
			return host.rollbackStart(fmt.Errorf("server: service %q requires unavailable capabilities %s", spec.Name, missing))
		}
		ingress := lifecycle.NewIngress(host.runtime.lifecycle)
		if err := service.Start(newServiceContext(host.runtime, ingress)); err != nil {
			return host.rollbackStart(fmt.Errorf("server: start service %q: %w", spec.Name, err))
		}
		host.started = append(host.started, startedService{service: service, ingress: ingress})
	}
	return nil
}

func (host *serviceHost) rollbackStart(startErr error) error {
	var errs []error
	for index := len(host.started) - 1; index >= 0; index-- {
		if stopper, ok := host.started[index].service.(Stopper); ok {
			if err := stopper.Stop(context.Background()); err != nil {
				errs = append(errs, fmt.Errorf("server: rollback service %q: %w", host.started[index].service.Spec().Name, err))
			}
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
	if host.drainDone != nil {
		done := host.drainDone
		host.mu.Unlock()
		select {
		case <-done:
			host.mu.Lock()
			err := host.drainErr
			host.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	services := append([]startedService(nil), host.started...)
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
				errs = append(errs, fmt.Errorf("server: drain service %q: %w", services[index].service.Spec().Name, err))
			}
		}
	}
	for index := len(services) - 1; index >= 0; index-- {
		services[index].ingress.Wait()
	}
	host.mu.Lock()
	host.drainErr = errors.Join(errs...)
	close(done)
	err := host.drainErr
	host.mu.Unlock()
	return err
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
		if stopper, ok := services[index].service.(Stopper); ok {
			if err := stopper.Stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("server: stop service %q: %w", services[index].service.Spec().Name, err))
			}
		}
	}
	return errors.Join(append([]error{drainErr}, errs...)...)
}
