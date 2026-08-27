package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/modules/discovery"
)

const discoveryWatchRetryDelay = time.Second

// discoveryRuntime 管理当前进程在 etcd 中的服务实例租约和共享路由目录。
type discoveryRuntime struct {
	registry        *discovery.EtcdRegistry
	registration    *discovery.EtcdRegistration
	directory       *discovery.ServiceDirectory
	persistentSlots *discovery.EtcdSlotStore
	slotDirectory   *discovery.SlotDirectory
	slots           *discovery.CachedSlotStore
	closeOnce       sync.Once
	closeErr        error
}

// LogicalSlot 是部署为有状态服务显式声明的稳定业务槽位。它与每次启动都会变化的
// RuntimeInstanceID 分离且不会参与实例 ID 拼接；业务使用它决定 FixedSlot 路由，
// Runtime.Slots 保存实际 generation 归属。
type LogicalSlot struct {
	// ID 是部署控制面分配的稳定逻辑槽位，不随进程重启变化。
	ID string
	// RuntimeInstanceID 是本次进程实例身份，用于校验槽位当前归属；它不包含逻辑槽位 ID。
	RuntimeInstanceID string
}

// newLogicalSlot 校验当前运行实例对稳定逻辑槽位的部署声明。它不创建 SlotStore
// assignment，也不接管 generation；这些动作必须由发布控制面在优雅交接后执行。
func newLogicalSlot(id, runtimeInstanceID string) (*LogicalSlot, error) {
	assignment := discovery.SlotAssignment{
		SlotID:            strings.TrimSpace(id),
		RuntimeInstanceID: strings.TrimSpace(runtimeInstanceID),
		Generation:        1,
	}
	if err := assignment.Validate(); err != nil {
		return nil, err
	}
	return &LogicalSlot{ID: assignment.SlotID, RuntimeInstanceID: assignment.RuntimeInstanceID}, nil
}

func newDiscovery(ctx context.Context, etcdConfig discovery.EtcdConfig, instance discovery.Instance) (*discoveryRuntime, error) {
	registry, err := discovery.NewEtcdRegistry(ctx, etcdConfig)
	if err != nil {
		return nil, err
	}
	registration, err := registry.Register(ctx, instance)
	if err != nil {
		return nil, errors.Join(err, registry.Close())
	}
	slotStore := registry.Slots()
	slotDirectory := discovery.NewSlotDirectory()
	slots, err := discovery.NewCachedSlotStore(slotStore, slotDirectory)
	if err != nil {
		_ = registration.Close(context.WithoutCancel(ctx))
		return nil, errors.Join(err, registry.Close())
	}
	return &discoveryRuntime{
		registry:        registry,
		registration:    registration,
		directory:       discovery.NewServiceDirectory(),
		persistentSlots: slotStore,
		slotDirectory:   slotDirectory,
		slots:           slots,
	}, nil
}

// Directory 返回由 Watch 维护的共享服务目录。服务发现未启用时返回 nil。
func (d *discoveryRuntime) directorySnapshot() *discovery.ServiceDirectory {
	if d == nil {
		return nil
	}
	return d.directory
}

// Slots 返回稳定逻辑槽位的缓存门面。读取只访问 Watch 目录，持久创建和迁移委托
// etcd Store；业务服务负责在 Promote 前确认目标实例 ready，并决定槽位规则。
func (d *discoveryRuntime) slotStore() discovery.SlotStore {
	if d == nil {
		return nil
	}
	return d.slots
}

// Watch 将实例和槽位视为同一个路由一致性域。任一 Watch 终止都会让两份目录
// 同时失败关闭，随后重新载入完整快照，避免其中一边继续使用已经过期的视图。
func (d *discoveryRuntime) watch(ctx context.Context) error {
	if d == nil || d.registry == nil || d.directory == nil || d.persistentSlots == nil || d.slotDirectory == nil {
		return errors.New("server: discovery is not configured")
	}
	for {
		err := d.watchOnce(ctx)
		d.directory.Invalidate()
		d.slotDirectory.Invalidate()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		txlog.ErrorContext(ctx, "service discovery watch interrupted; rebuilding snapshot", "err", err)
		timer := time.NewTimer(discoveryWatchRetryDelay)
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

func (d *discoveryRuntime) watchOnce(ctx context.Context) error {
	watchCtx, cancel := context.WithCancel(ctx)
	results := make(chan error, 2)
	go func() { results <- d.registry.Watch(watchCtx, d.directory) }()
	go func() { results <- d.persistentSlots.Watch(watchCtx, d.slotDirectory) }()
	first := <-results
	cancel()
	second := <-results
	return errors.Join(first, second)
}

func (d *discoveryRuntime) waitReady(ctx context.Context) error {
	if d == nil || d.directory == nil || d.slotDirectory == nil {
		return errors.New("server: discovery is not configured")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.directory.Ready():
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.slotDirectory.Ready():
		return nil
	}
}

// MarkRunning 表示进程已经完成选中模块的启动，可以接受普通服务路由。
func (d *discoveryRuntime) markRunning(ctx context.Context) error {
	if d == nil || d.registration == nil {
		return nil
	}
	return d.registration.SetStatus(ctx, discovery.StatusRunning)
}

// MarkDraining 会先从普通服务路由中摘除本实例；精确路由仍可在排水窗口内找到
// draining 实例，供既有会话或固定归属 Actor 完成业务。
func (d *discoveryRuntime) markDraining(ctx context.Context) error {
	if d == nil || d.registration == nil {
		return nil
	}
	return d.registration.SetStatus(ctx, discovery.StatusDraining)
}

// MarkStopping 表示实例已经结束排水并开始最终关闭。精确路由在进程退出前仍可
// 找到 stopping 实例，避免目录状态与最终连接关闭之间出现错误的静默改派。
func (d *discoveryRuntime) markStopping(ctx context.Context) error {
	if d == nil || d.registration == nil {
		return nil
	}
	return d.registration.SetStatus(ctx, discovery.StatusStopping)
}

func (d *discoveryRuntime) close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	d.closeOnce.Do(func() {
		var registrationErr error
		if d.registration != nil {
			registrationErr = d.registration.Close(ctx)
		}
		if d.registry == nil {
			d.closeErr = registrationErr
			return
		}
		d.closeErr = errors.Join(registrationErr, d.registry.Close())
	})
	return d.closeErr
}
