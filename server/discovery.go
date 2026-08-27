package server

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/ngnl47/tx/modules/discovery"
)

// discoveryRuntime 管理当前进程在 etcd 中的服务实例租约和共享路由目录。
type discoveryRuntime struct {
	registry     *discovery.EtcdRegistry
	registration *discovery.EtcdRegistration
	directory    *discovery.ServiceDirectory
	closeOnce    sync.Once
	closeErr     error
}

// LogicalSlot 是部署为有状态服务显式声明的稳定业务槽位。它与每次启动都会变化的
// RuntimeInstanceID 分离；业务使用它决定 FixedSlot 路由，Runtime.Slots 保存实际
// generation 归属。
type LogicalSlot struct {
	// ID 是部署控制面分配的稳定逻辑槽位，不随进程重启变化。
	ID string
	// RuntimeInstanceID 是本次进程实例身份，用于校验槽位当前归属。
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
	return &discoveryRuntime{
		registry:     registry,
		registration: registration,
		directory:    discovery.NewServiceDirectory(),
	}, nil
}

// Directory 返回由 Watch 维护的共享服务目录。服务发现未启用时返回 nil。
func (d *discoveryRuntime) directorySnapshot() *discovery.ServiceDirectory {
	if d == nil {
		return nil
	}
	return d.directory
}

// Slots 返回稳定逻辑槽位的持久归属存储。它只保存 SlotID 到运行实例 generation
// 的映射；业务服务负责在 Promote 前确认目标实例已经 ready，并决定哪些实体使用
// 哪个槽位。
func (d *discoveryRuntime) slotStore() discovery.SlotStore {
	if d == nil || d.registry == nil {
		return nil
	}
	return d.registry.Slots()
}

// Watch 持续将 etcd 实例快照写入共享目录。非上下文取消的异常会清空目录，
// 防止调用方继续向已过期 endpoint 路由。
func (d *discoveryRuntime) watch(ctx context.Context) error {
	if d == nil || d.registry == nil || d.directory == nil {
		return errors.New("server: discovery is not configured")
	}
	err := d.registry.Watch(ctx, d.directory)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		_ = d.directory.Replace(nil)
	}
	return err
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
