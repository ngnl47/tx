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

// ErrStopDeferred 表示工作或资源关闭尚未完成，依赖必须保留并允许重试。
var ErrStopDeferred = errors.New("component: stop deferred")

// Host 按已经解析的 Graph 管理组件生命周期。
type Host struct {
	root    *lifecycle.WaitGroupContext
	ownRoot bool
	graph   Graph

	transition chan struct{}
	started    []startedComponent
	state      hostState

	startOnce sync.Once
	startErr  error
	drained   int
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
	return &Host{root: root, ownRoot: ownRoot, graph: graph, transition: make(chan struct{}, 1)}
}

// Start 先校验全部组件，再按拓扑顺序启动。ctx 限定失败回滚的等待，后台任务使用独立生命周期。
func (host *Host) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if host == nil {
		return errors.New("component: host is nil")
	}
	host.startOnce.Do(func() {
		if err := host.enter(ctx); err != nil {
			host.startErr = err
			return
		}
		defer host.leave()
		if host.state == hostStateStopped {
			host.startErr = ErrHostStopped
			return
		}
		host.startErr = host.start(ctx)
		if host.startErr != nil {
			host.state = hostStateFailed
			return
		}
		host.state = hostStateStarted
	})
	return host.startErr
}

func (host *Host) start(ctx context.Context) error {
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
		host.started = append(host.started, current)
		if err := entry.component.Start(startContext); err != nil {
			startErr := fmt.Errorf("component: start %q: %w", entry.spec.ID, err)
			return errors.Join(startErr, host.stop(ctx, "rollback"))
		}
	}
	return nil
}

// Drain 逆序冲刷组件，失败即停止；重试从失败组件继续，不重复已成功的组件。
func (host *Host) Drain(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := host.enter(ctx); err != nil {
		return err
	}
	defer host.leave()
	// 启动失败保留的是待清理的部分资源，不能执行只适用于完整启动的 Drain。
	if host.state == hostStateFailed || host.state == hostStateStopped {
		return nil
	}
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

// Stop 逆序取消组件任务并释放资源。未退出时保留失败组件及其依赖，后续调用可重试。
func (host *Host) Stop(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := host.enter(ctx); err != nil {
		return errors.Join(ErrStopDeferred, err)
	}
	defer host.leave()
	if host.state == hostStateStopped {
		return host.stopErr
	}
	return host.stop(ctx, "stop")
}

func (host *Host) stop(ctx context.Context, operation string) error {
	for len(host.started) > 0 {
		index := len(host.started) - 1
		err := stopComponent(ctx, host.started[index], operation)
		if errors.Is(err, ErrStopDeferred) {
			return errors.Join(host.stopErr, err)
		}
		host.stopErr = errors.Join(host.stopErr, err)
		host.started = host.started[:index]
	}
	if host.ownRoot {
		if err := host.root.CancelAndWait(ctx); err != nil {
			return errors.Join(ErrStopDeferred, host.stopErr, err)
		}
	}
	host.state = hostStateStopped
	return host.stopErr
}

func (host *Host) enter(ctx context.Context) error {
	select {
	case host.transition <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (host *Host) leave() { <-host.transition }

func stopComponent(ctx context.Context, current startedComponent, operation string) error {
	if err := current.tasks.CancelAndWait(ctx); err != nil {
		return errors.Join(ErrStopDeferred, fmt.Errorf("component: wait for %q tasks: %w", current.entry.spec.ID, err))
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
