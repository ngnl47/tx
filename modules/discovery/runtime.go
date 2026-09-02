package discovery

import (
	"context"
	"errors"
	"sync"
	"time"

	txlog "github.com/ngnl47/tx/log"
)

const runtimeWatchRetryDelay = time.Second

// Runtime 管理当前进程的服务实例租约、服务目录和稳定槽位目录。
type Runtime struct {
	registry        runtimeRegistry
	registration    runtimeRegistration
	persistentSlots runtimeSlotStore
	directory       *ServiceDirectory
	slotDirectory   *SlotDirectory
	slots           *CachedSlotStore
	identity        RuntimeIdentity
	instance        Instance
	done            chan struct{}
	doneOnce        sync.Once
	errMu           sync.Mutex
	err             error
	closeOnce       sync.Once
	closeErr        error
}

type runtimeRegistry interface {
	Watch(context.Context, *ServiceDirectory) error
	Close() error
}

type runtimeRegistration interface {
	SetStatus(context.Context, Status) error
	Close(context.Context) error
	Done() <-chan struct{}
	Err() error
	Identity() RuntimeIdentity
	Instance() Instance
}

type runtimeSlotStore interface {
	SlotStore
	Watch(context.Context, *SlotDirectory) error
}

// NewRuntime 连接 etcd、注册当前实例并创建只读缓存目录。
func NewRuntime(ctx context.Context, config EtcdConfig, instance Instance) (*Runtime, error) {
	registry, err := NewEtcdRegistry(ctx, config)
	if err != nil {
		return nil, err
	}
	registration, err := registry.Register(ctx, instance)
	if err != nil {
		return nil, errors.Join(err, registry.Close())
	}
	runtime, err := newRuntime(registry, registration, registry.Slots())
	if err != nil {
		_ = registration.Close(context.WithoutCancel(ctx))
		return nil, errors.Join(err, registry.Close())
	}
	return runtime, nil
}

// NewRuntimeWithIdentity 申请运行身份、注册最终实例并创建只读缓存目录。
func NewRuntimeWithIdentity(ctx context.Context, config EtcdConfig, appID string, descriptor Instance) (*Runtime, error) {
	registry, err := NewEtcdRegistry(ctx, config)
	if err != nil {
		return nil, err
	}
	registration, err := registry.RegisterRuntime(ctx, appID, descriptor)
	if err != nil {
		return nil, errors.Join(err, registry.Close())
	}
	runtime, err := newRuntime(registry, registration, registry.Slots())
	if err != nil {
		_ = registration.Close(context.WithoutCancel(ctx))
		return nil, errors.Join(err, registry.Close())
	}
	return runtime, nil
}

func newRuntime(registry runtimeRegistry, registration runtimeRegistration, slots runtimeSlotStore) (*Runtime, error) {
	directory := NewServiceDirectory()
	slotDirectory := NewSlotDirectory()
	cachedSlots, err := NewCachedSlotStore(slots, slotDirectory)
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{
		registry:        registry,
		registration:    registration,
		persistentSlots: slots,
		directory:       directory,
		slotDirectory:   slotDirectory,
		slots:           cachedSlots,
		identity:        registration.Identity(),
		instance:        registration.Instance(),
		done:            make(chan struct{}),
	}
	go runtime.waitRegistration()
	return runtime, nil
}

// Identity 返回当前进程不可变的运行身份快照。
func (runtime *Runtime) Identity() RuntimeIdentity {
	if runtime == nil {
		return RuntimeIdentity{}
	}
	return runtime.identity
}

// Instance 返回使用最终 InstanceID 冻结的实例描述副本。
func (runtime *Runtime) Instance() Instance {
	if runtime == nil {
		return Instance{}
	}
	return cloneInstance(runtime.instance)
}

// Done 在运行身份正常关闭或意外丢失时关闭。
func (runtime *Runtime) Done() <-chan struct{} {
	if runtime == nil {
		return nil
	}
	return runtime.done
}

// Err 返回运行身份的最终结束原因；运行期间返回 nil。
func (runtime *Runtime) Err() error {
	if runtime == nil {
		return nil
	}
	runtime.errMu.Lock()
	defer runtime.errMu.Unlock()
	return runtime.err
}

// Directory 返回由 Watch 维护的共享服务目录。
func (runtime *Runtime) Directory() *ServiceDirectory {
	if runtime == nil {
		return nil
	}
	return runtime.directory
}

// Slots 返回稳定逻辑槽位的缓存门面。读取只访问 Watch 目录，写入委托持久 SlotStore。
func (runtime *Runtime) Slots() SlotStore {
	if runtime == nil {
		return nil
	}
	return runtime.slots
}

// Watch 持续维护实例与槽位目录。任一 Watch 中断都会同时废弃两份快照并重建。
func (runtime *Runtime) Watch(ctx context.Context) error {
	if runtime == nil || runtime.registry == nil || runtime.directory == nil || runtime.persistentSlots == nil || runtime.slotDirectory == nil {
		return errors.New("discovery runtime: dependencies are not configured")
	}
	for {
		err := runtime.watchOnce(ctx)
		runtime.directory.Invalidate()
		runtime.slotDirectory.Invalidate()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		txlog.ErrorContext(ctx, "service discovery watch interrupted; rebuilding snapshot", "err", err)
		timer := time.NewTimer(runtimeWatchRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (runtime *Runtime) watchOnce(ctx context.Context) error {
	watchCtx, cancel := context.WithCancel(ctx)
	results := make(chan error, 2)
	go func() { results <- runtime.registry.Watch(watchCtx, runtime.directory) }()
	go func() { results <- runtime.persistentSlots.Watch(watchCtx, runtime.slotDirectory) }()
	first := <-results
	cancel()
	second := <-results
	return errors.Join(first, second)
}

// WaitReady 等待服务实例和逻辑槽位两份初始快照就绪。
func (runtime *Runtime) WaitReady(ctx context.Context) error {
	if runtime == nil || runtime.directory == nil || runtime.slotDirectory == nil {
		return errors.New("discovery runtime: dependencies are not configured")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-runtime.directory.Ready():
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-runtime.slotDirectory.Ready():
		return nil
	}
}

// MarkRunning 表示进程已经完成能力启动，可以接受普通服务路由。
func (runtime *Runtime) MarkRunning(ctx context.Context) error {
	return runtime.setStatus(ctx, StatusRunning)
}

// MarkDraining 从普通服务路由中摘除实例，精确路由仍可在排水窗口内找到它。
func (runtime *Runtime) MarkDraining(ctx context.Context) error {
	return runtime.setStatus(ctx, StatusDraining)
}

// MarkStopping 表示实例已经结束排水并进入最终关闭阶段。
func (runtime *Runtime) MarkStopping(ctx context.Context) error {
	return runtime.setStatus(ctx, StatusStopping)
}

func (runtime *Runtime) setStatus(ctx context.Context, status Status) error {
	if runtime == nil || runtime.registration == nil {
		return nil
	}
	return runtime.registration.SetStatus(ctx, status)
}

// Close 撤销实例租约并关闭 etcd 客户端。并发调用共享第一次结果。
func (runtime *Runtime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	runtime.closeOnce.Do(func() {
		var registrationErr error
		if runtime.registration != nil {
			registrationErr = runtime.registration.Close(ctx)
			runtime.finishRegistration()
		} else {
			runtime.finish(ErrRuntimeClosed)
		}
		if runtime.registry == nil {
			runtime.closeErr = registrationErr
			return
		}
		runtime.closeErr = errors.Join(registrationErr, runtime.registry.Close())
	})
	return runtime.closeErr
}

func (runtime *Runtime) waitRegistration() {
	<-runtime.registration.Done()
	runtime.finishRegistration()
}

func (runtime *Runtime) finishRegistration() {
	err := runtime.registration.Err()
	if errors.Is(err, ErrRegistrationDone) {
		runtime.finish(ErrRuntimeClosed)
		return
	}
	runtime.finish(errors.Join(ErrRuntimeIdentityLost, err))
}

func (runtime *Runtime) finish(err error) {
	runtime.doneOnce.Do(func() {
		runtime.errMu.Lock()
		runtime.err = err
		runtime.errMu.Unlock()
		close(runtime.done)
	})
}
