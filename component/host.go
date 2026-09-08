package component

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
)

// ErrHostStopped 表示组件宿主已经停止，不能再次启动。
var ErrHostStopped = errors.New("component: host is stopped")

// Host 按已经解析的 Graph 管理组件生命周期。
type Host struct {
	root    *lifecycle.WaitGroupContext
	ownRoot bool
	graph   Graph

	transition sync.Mutex
	started    []startedComponent
	state      hostState

	startOnce sync.Once
	startErr  error
	drained   int
	stopOnce  sync.Once
	stopErr   error
}

type hostState uint8

const (
	hostStateNew hostState = iota
	hostStateStarted
	hostStateFailed
	hostStateStopped
)

type startedComponent struct {
	entry graphEntry
	tasks *lifecycle.ChildWaitGroupContext
}

// NewHost 创建组件生命周期宿主。root 为 nil 时，Host 创建并管理独立根生命周期。
func NewHost(root *lifecycle.WaitGroupContext, graph Graph) *Host {
	ownRoot := root == nil
	if root == nil {
		root = lifecycle.NewWaitGroupContext(context.Background())
	}
	return &Host{root: root, ownRoot: ownRoot, graph: graph}
}

// Start 先校验全部组件，再按拓扑顺序启动。启动失败会逆序清理已接触的组件。
func (host *Host) Start() error {
	if host == nil {
		return errors.New("component: host is nil")
	}
	host.startOnce.Do(func() {
		host.transition.Lock()
		defer host.transition.Unlock()
		if host.state == hostStateStopped {
			host.startErr = ErrHostStopped
			return
		}
		host.startErr = host.start()
		if host.startErr != nil {
			host.state = hostStateFailed
			return
		}
		host.state = hostStateStarted
	})
	return host.startErr
}

func (host *Host) start() error {
	validateContext := runtimeContext{root: host.root, tasks: host.root}
	for _, entry := range host.graph.ordered {
		if validator, ok := entry.component.(Validator); ok {
			if err := validator.Validate(validateContext); err != nil {
				return fmt.Errorf("component: validate %q: %w", entry.spec.ID, err)
			}
		}
	}

	for _, entry := range host.graph.ordered {
		current := startedComponent{
			entry: entry,
			tasks: lifecycle.NewChildWaitGroupContext(host.root),
		}
		startContext := runtimeContext{root: host.root, tasks: current.tasks}
		if err := entry.component.Start(startContext); err != nil {
			startErr := fmt.Errorf("component: start %q: %w", entry.spec.ID, err)
			startErr = errors.Join(startErr, stopComponent(context.Background(), current, "rollback"))
			return host.rollback(startErr)
		}
		host.started = append(host.started, current)
	}
	return nil
}

func (host *Host) rollback(startErr error) error {
	errs := []error{startErr}
	for index := len(host.started) - 1; index >= 0; index-- {
		if err := stopComponent(context.Background(), host.started[index], "rollback"); err != nil {
			errs = append(errs, err)
		}
	}
	host.started = nil
	if host.ownRoot {
		host.root.Cancel()
		host.root.WaitDoneAndWaitGroupWait()
	}
	return errors.Join(errs...)
}

// Drain 逆序冲刷组件，失败即停止；重试从失败组件继续，不重复已成功的组件。
func (host *Host) Drain(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	host.transition.Lock()
	defer host.transition.Unlock()
	for host.drained < len(host.started) {
		index := len(host.started) - 1 - host.drained
		current := host.started[index]
		if drainer, ok := current.entry.component.(Drainer); ok {
			if err := drainer.Drain(ctx); err != nil {
				return fmt.Errorf("component: drain %q: %w", current.entry.spec.ID, err)
			}
		}
		host.drained++
	}
	return nil
}

// Stop 逆序取消组件任务并释放资源。并发调用共享第一次结果。
func (host *Host) Stop(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	host.stopOnce.Do(func() {
		host.transition.Lock()
		defer host.transition.Unlock()
		var errs []error
		for index := len(host.started) - 1; index >= 0; index-- {
			if err := stopComponent(ctx, host.started[index], "stop"); err != nil {
				errs = append(errs, err)
			}
		}
		host.started = nil
		host.state = hostStateStopped
		if host.ownRoot {
			host.root.Cancel()
			host.root.WaitDoneAndWaitGroupWait()
		}
		host.stopErr = errors.Join(errs...)
	})
	return host.stopErr
}

func stopComponent(ctx context.Context, current startedComponent, operation string) error {
	if current.tasks != nil {
		current.tasks.Cancel()
		current.tasks.WaitDoneAndWaitGroupWait()
	}
	stopper, ok := current.entry.component.(Stopper)
	if !ok {
		return nil
	}
	if err := stopper.Stop(ctx); err != nil {
		return fmt.Errorf("component: %s %q: %w", operation, current.entry.spec.ID, err)
	}
	return nil
}
