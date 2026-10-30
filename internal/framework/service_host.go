package framework

import (
	"context"
	"errors"
	"fmt"
	txruntime "github.com/ngnl47/tx/runtime"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
)

type serviceHost struct {
	registry *registry
	runtime  *runtime

	mu              sync.Mutex
	started         []startedService
	drainAttempt    *drainAttempt
	rollbackPending bool
	tasksStopOnce   sync.Once
	tasksStopped    chan struct{}
}

type startedService struct {
	flushed         bool
	rollbackStopped bool
	service         Service
	access          *runtimeAccess
	ingress         *lifecycle.Ingress
	tasks           *lifecycle.ChildWaitGroupContext
	phase           *servicePhase
}

func newServiceHost(registry *registry, runtime *runtime) *serviceHost {
	return &serviceHost{registry: registry, runtime: runtime}
}

func (host *serviceHost) start(names []string) error {
	services, err := host.registry.selectServices(names)
	if err != nil {
		return err
	}
	if err := host.initServices(context.Background(), services); err != nil {
		return err
	}
	if err := host.startServices(context.Background()); err != nil {
		return err
	}
	host.openServices()
	return nil
}

func (host *serviceHost) initServices(ctx context.Context, services []Service) error {
	if host == nil || host.runtime == nil || host.runtime.lifecycle == nil {
		return errors.New("framework: runtime lifecycle is not configured")
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.started) > 0 {
		return errors.New("framework: services are already started")
	}
	for _, service := range services {
		if err := context.Cause(ctx); err != nil {
			return host.rollbackStart(err)
		}
		spec := service.Spec()
		for _, required := range spec.Requires {
			if !host.runtime.hasCMod(required) {
				return host.rollbackStart(fmt.Errorf("framework: service %q requires unavailable CMod %q", spec.Name, required))
			}
		}
		phase := host.runtime.servicePhases[spec.Name]
		if phase == nil {
			phase = &servicePhase{}
		}
		phase.set(lifecycle.ServiceInitializing, nil)
		ingress := lifecycle.NewIngress(host.runtime.lifecycle)
		tasks := lifecycle.NewChildWaitGroupContext(host.runtime.lifecycle)
		current := startedService{phase: phase, service: service, ingress: ingress, tasks: tasks, access: host.runtime.serviceAccess[spec.Name]}
		serviceTasks := taskAccess{Context: txruntime.Bind(tasks, host.runtime.serviceAccess[spec.Name]), root: tasks.WaitGroupContext}
		admission := lifecycle.AdmissionFunc(func() (func(), error) {
			if phase.snapshot().State != lifecycle.ServiceRunning {
				return nil, lifecycle.ErrNotReady
			}
			return ingress.Enter()
		})
		if err := service.Init(newServiceContextWithTasks(host.runtime, admission, serviceTasks)); err != nil {
			phase.set(lifecycle.ServiceInitializing, err)
			startErr := fmt.Errorf("framework: initialize service %q: %w", spec.Name, err)
			host.started = append(host.started, current)
			return host.rollbackStart(startErr)
		}
		phase.set(lifecycle.ServiceInitialized, nil)
		host.started = append(host.started, current)
	}
	return nil
}

func (host *serviceHost) startServices(ctx context.Context) error {
	host.mu.Lock()
	services := append([]startedService(nil), host.started...)
	host.mu.Unlock()
	for _, current := range services {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		current.phase.set(lifecycle.ServiceStarting, nil)
		var err error
		if starter, ok := current.service.(Starter); ok {
			err = starter.Start(serviceCallbackContext(ctx, current))
		}
		if err != nil {
			current.phase.set(lifecycle.ServiceStarting, err)
			return fmt.Errorf("framework: start service %q: %w", current.service.Spec().Name, err)
		}
	}
	return nil
}

// openServices 在全部启动钩子成功、会话分配就绪后统一开放业务。
func (host *serviceHost) openServices() {
	host.mu.Lock()
	defer host.mu.Unlock()
	for _, current := range host.started {
		current.phase.set(lifecycle.ServiceRunning, nil)
	}
}

func (host *serviceHost) rollbackStart(startErr error) error {
	host.rollbackPending = true
	return errors.Join(startErr, host.rollbackInitialization(context.Background()))
}

// retryInitializationRollback 保留初始化失败的资源关闭顺序，重试尚未完成的 Procedure 排水。
func (host *serviceHost) retryInitializationRollback(ctx context.Context) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	if !host.rollbackPending {
		return nil
	}
	return host.rollbackInitialization(ctx)
}

// 调用方持有 host.mu；已接纳的独立 Procedure 退出前不能关闭服务资源。
func (host *serviceHost) rollbackInitialization(ctx context.Context) error {
	drainCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(host.runtime.config.Runtime.Lifecycle.DrainTimeout))
	defer cancel()
	err := host.runtime.transaction.Drain(drainCtx)
	if err != nil {
		return errors.Join(errStopDeferred, err)
	}
	for len(host.started) > 0 {
		index := len(host.started) - 1
		if err := rollbackService(drainCtx, &host.started[index]); err != nil {
			return errors.Join(errStopDeferred, err)
		}
		host.started = host.started[:index]
	}
	host.rollbackPending = false
	return nil
}

func (host *serviceHost) beginDrain() {
	if host == nil {
		return
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	for _, service := range host.started {
		service.phase.set(lifecycle.ServiceDraining, nil)
		service.ingress.BeginDrain()
	}
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
	if host.drainAttempt != nil {
		attempt := host.drainAttempt
		host.mu.Unlock()
		select {
		case <-attempt.done:
			return errors.Join(attempt.err, waitServiceIngress(ctx, services))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	attempt := &drainAttempt{done: make(chan struct{})}
	host.drainAttempt = attempt
	host.mu.Unlock()

	var errs []error
	for index := len(services) - 1; index >= 0; index-- {
		services[index].phase.set(lifecycle.ServiceDraining, nil)
		services[index].ingress.BeginDrain()
	}
	for index := len(services) - 1; index >= 0; index-- {
		if drainer, ok := services[index].service.(Drainer); ok {
			if err := drainer.Drain(serviceCallbackContext(ctx, services[index])); err != nil {
				services[index].phase.set(lifecycle.ServiceDraining, err)
				errs = append(errs, fmt.Errorf("framework: drain service %q: %w", services[index].service.Spec().Name, err))
			}
		}
	}
	host.mu.Lock()
	attempt.err = errors.Join(errs...)
	if attempt.err != nil {
		host.drainAttempt = nil
	}
	close(attempt.done)
	err := attempt.err
	host.mu.Unlock()
	return errors.Join(err, waitServiceIngress(ctx, services))
}

func waitServiceIngress(ctx context.Context, services []startedService) error {
	var errs []error
	for index := len(services) - 1; index >= 0; index-- {
		if err := services[index].ingress.WaitContext(ctx); err != nil {
			services[index].phase.set(lifecycle.ServiceDraining, err)
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

// quiesceTasks 停止业务后台任务，但保留服务资源供最终持久化及失败重试使用。
func (host *serviceHost) quiesceTasks(ctx context.Context) error {
	host.tasksStopOnce.Do(func() {
		host.mu.Lock()
		services := append([]startedService(nil), host.started...)
		host.mu.Unlock()
		host.tasksStopped = make(chan struct{})
		// 先通知全部服务；一个服务收尾不应阻止其他服务收到取消。
		for _, current := range services {
			if current.tasks != nil {
				current.tasks.Cancel()
			}
		}
		// 所有重试复用同一个完成信号，不为每次超时累积等待协程。
		go func() {
			for _, current := range services {
				if current.tasks != nil {
					current.tasks.Wait()
				}
			}
			close(host.tasksStopped)
		}()
	})
	select {
	case <-host.tasksStopped:
		return nil
	default:
	}
	select {
	case <-host.tasksStopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("framework: wait for service background tasks: %w", ctx.Err())
	}
}

func (host *serviceHost) stop(ctx context.Context) error {
	if host == nil {
		return nil
	}
	drainErr := host.drain(ctx)
	if err := host.quiesceTasks(ctx); err != nil {
		return errors.Join(errStopDeferred, drainErr, err)
	}
	host.mu.Lock()
	services := append([]startedService(nil), host.started...)
	host.started = nil
	host.mu.Unlock()
	var errs []error
	for index := len(services) - 1; index >= 0; index-- {
		if err := stopServiceResources(ctx, services[index], "stop"); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(append([]error{drainErr}, errs...)...)
}

func rollbackService(ctx context.Context, current *startedService) error {
	if current.tasks != nil {
		current.tasks.Cancel()
	}
	if !current.rollbackStopped {
		if err := stopServiceResources(ctx, *current, "rollback"); err != nil {
			return err
		}
		current.rollbackStopped = true
	}
	if err := current.tasks.CancelAndWait(ctx); err != nil {
		current.phase.set(lifecycle.ServiceDraining, err)
		return fmt.Errorf("framework: wait for service %q rollback tasks: %w", current.service.Spec().Name, err)
	}
	current.phase.set(lifecycle.ServiceStopped, nil)
	return nil
}

func stopServiceResources(ctx context.Context, current startedService, operation string) (err error) {
	current.phase.set(lifecycle.ServiceDraining, nil)
	defer func() {
		if err == nil {
			current.phase.set(lifecycle.ServiceStopped, nil)
		} else {
			current.phase.set(lifecycle.ServiceDraining, err)
		}
	}()
	stopper, ok := current.service.(Stopper)
	if !ok {
		return nil
	}
	if err := stopper.Stop(serviceCallbackContext(ctx, current)); err != nil {
		return fmt.Errorf("framework: %s service %q: %w", operation, current.service.Spec().Name, err)
	}
	return nil
}

// 关闭等待使用调用方时限，能力仍来自被关闭服务，避免串用调用方的作用域。
func serviceCallbackContext(ctx context.Context, current startedService) context.Context {
	if current.access == nil {
		return ctx
	}
	return txruntime.Bind(ctx, current.access)
}

// flush 由串行收尾流程调用；成功服务不因其他服务或传输失败再次刷盘。
func (host *serviceHost) flush(ctx context.Context) error {
	host.mu.Lock()
	services := append([]startedService(nil), host.started...)
	host.mu.Unlock()
	for i := len(services) - 1; i >= 0; i-- {
		if services[i].flushed {
			continue
		}
		if flusher, ok := services[i].service.(Flusher); ok {
			if err := flusher.Flush(serviceCallbackContext(ctx, services[i])); err != nil {
				return fmt.Errorf("framework: flush service %q: %w", services[i].service.Spec().Name, err)
			}
		}
		host.mu.Lock()
		host.started[i].flushed = true
		host.mu.Unlock()
	}
	return nil
}
