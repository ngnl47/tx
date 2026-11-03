package discovery

import (
	"context"
	"errors"
	"sync"
	"time"

	txetcd "github.com/ngnl47/tx/etcd"
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
	ownership       *SlotOwnership
	identity        RuntimeIdentity
	instance        Instance

	// Watch 就绪通知与最终错误分别同步。
	watchMu      sync.Mutex
	watchReady   bool
	watchChanged chan struct{}

	done     chan struct{}
	doneOnce sync.Once

	errMu sync.Mutex
	err   error

	closeOnce sync.Once
	closeErr  error
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
	VersionedSlotReader
	Watch(context.Context, *SlotDirectory) error
}

// NewRuntime 创建并持有 etcd 客户端，注册运行身份并创建只读缓存目录。
func NewRuntime(ctx context.Context, config EtcdConfig, appID string, descriptor Instance) (*Runtime, error) {
	registry, err := NewEtcdRegistry(ctx, config)
	if err != nil {
		return nil, err
	}
	return newRuntimeWithRegistry(ctx, registry, appID, descriptor)
}

// NewRuntimeWithClient 借用已有客户端；关闭运行时只撤销本实例租约，不关闭客户端。
func NewRuntimeWithClient(ctx context.Context, client *txetcd.Client, config EtcdConfig, appID string, descriptor Instance) (*Runtime, error) {
	registry, err := NewEtcdRegistryWithClient(client, config)
	if err != nil {
		return nil, err
	}
	return newRuntimeWithRegistry(ctx, registry, appID, descriptor)
}

func newRuntimeWithRegistry(ctx context.Context, registry *EtcdRegistry, appID string, descriptor Instance) (*Runtime, error) {
	registration, err := registry.Register(ctx, appID, descriptor)
	if err != nil {
		return nil, errors.Join(err, registry.Close())
	}
	runtime, err := newRuntime(registry, registration, registry.Slots())
	if err != nil {
		return nil, errors.Join(err, registration.Close(context.WithoutCancel(ctx)), registry.Close())
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
	ownership, err := NewSlotOwnership(slots, directory)
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
		ownership:       ownership,
		identity:        registration.Identity(),
		instance:        registration.Instance(),
		watchChanged:    make(chan struct{}),
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

// Slots 返回稳定逻辑槽位的只读缓存。写入必须使用权威存储或本实例认领能力。
func (runtime *Runtime) Slots() SlotReader {
	if runtime == nil {
		return nil
	}
	return runtime.slots
}

// AuthoritativeSlots 提供首次分配、加载校验和交接复核所需的权威存储。
// 复用当前运行时的连接；普通消息路由继续使用 Slots 的 Watch 缓存。
func (runtime *Runtime) AuthoritativeSlots() SlotStore {
	if runtime == nil {
		return nil
	}
	return runtime.persistentSlots
}

// Ownership 返回本运行时装配的归属能力，内部使用权威存储和同一成员目录。
// 调用方无需选择缓存/持久层，也不取得连接生命周期所有权。
func (runtime *Runtime) Ownership() *SlotOwnership {
	if runtime == nil {
		return nil
	}
	return runtime.ownership
}

// Watch 持续维护实例与槽位目录。任一 Watch 中断都会同时废弃两份快照并重建。
func (runtime *Runtime) Watch(ctx context.Context) error {
	if runtime == nil || runtime.registry == nil || runtime.directory == nil || runtime.persistentSlots == nil || runtime.slotDirectory == nil {
		return errors.New("discovery runtime: dependencies are not configured")
	}
	for {
		err := runtime.watchOnce(ctx)
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
	defer cancel()
	// 每轮使用独立写入入口；失效与快照发布共享同一把锁，阻止迟到写入。
	active, servicesReady, slotsReady := true, false, false
	services, slots := NewServiceDirectory(), NewSlotDirectory()
	publish := func(apply func() error, ready *bool) error {
		runtime.watchMu.Lock()
		defer runtime.watchMu.Unlock()
		if !active || watchCtx.Err() != nil {
			return context.Canceled
		}
		if err := apply(); err != nil {
			return err
		}
		*ready = true
		if servicesReady && slotsReady && !runtime.watchReady {
			runtime.watchReady = true
			close(runtime.watchChanged)
			runtime.watchChanged = make(chan struct{})
		}
		return nil
	}
	services.publish = func(values []Instance) error {
		return publish(func() error { return runtime.directory.Replace(values) }, &servicesReady)
	}
	slots.publish = func(values []SlotAssignment, revision int64) error {
		return publish(func() error { return runtime.slotDirectory.ReplaceSnapshot(values, revision) }, &slotsReady)
	}
	slots.publishChanges = func(changes []slotChange, revision int64) error {
		return publish(func() error { return runtime.slotDirectory.applyChanges(changes, revision) }, &slotsReady)
	}
	results := make(chan error, 2)
	go func() { results <- runtime.registry.Watch(watchCtx, services) }()
	go func() { results <- runtime.persistentSlots.Watch(watchCtx, slots) }()
	first := <-results
	runtime.watchMu.Lock()
	active = false
	runtime.watchReady = false
	runtime.directory.Invalidate()
	runtime.slotDirectory.Invalidate()
	close(runtime.watchChanged)
	runtime.watchChanged = make(chan struct{})
	runtime.watchMu.Unlock()
	cancel()
	second := <-results
	return errors.Join(first, second)
}

// WaitReady 等待当前有效 Watch 轮次的服务实例和逻辑槽位快照同时就绪。
func (runtime *Runtime) WaitReady(ctx context.Context) error {
	if runtime == nil || runtime.directory == nil || runtime.slotDirectory == nil {
		return errors.New("discovery runtime: dependencies are not configured")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		runtime.watchMu.Lock()
		ready, changed := runtime.watchReady, runtime.watchChanged
		runtime.watchMu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
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

// Close 撤销实例租约并关闭自建的 etcd 客户端；借用客户端由调用方关闭。并发调用共享第一次结果。
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
