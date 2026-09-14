package discovery

import (
	"context"
	"errors"
	"slices"

	"github.com/cespare/xxhash/v2"
)

var (
	ErrSlotNotOwner            = errors.New("discovery: not current slot owner")
	ErrSlotCapacityUnavailable = errors.New("discovery: no slot capacity available")
)

// CheckSlotOwner 在加载前读取权威归属。reader 不得注入 Watch 缓存；已激活实体依靠排水交接。
func CheckSlotOwner(ctx context.Context, reader SlotReader, slotID, instance string) error {
	current, err := reader.ResolveSlot(ctx, slotID)
	if err != nil {
		return err
	}
	if current.RuntimeInstanceID != instance {
		return ErrSlotNotOwner
	}
	return nil
}

// SlotOwnership 集中维护首次分配和正常交接，不拥有 Actor、传输连接或持久化任务。
// 普通消息继续使用同包 SlotResolver 的 Watch 缓存，低层 SlotRouter/CAS SPI 保留。
type SlotOwnership struct {
	store     SlotStore
	directory Directory
	router    *SlotRouter
}

// NewSlotOwnership 使用权威存储；容量预检和实例收尾由每次操作绑定的执行方完成。
func NewSlotOwnership(store SlotStore, directory Directory) (*SlotOwnership, error) {
	router, err := NewSlotRouter(store, directory)
	if err != nil {
		return nil, err
	}
	return &SlotOwnership{store: store, directory: directory, router: router}, nil
}

// Ensure 保留已有归属，仅未分配时预检候选并通过 CAS 创建。created 表示本次成功创建。
// check 必须无加载副作用：false 表示容量不足或已关闸，可继续预检其他候选；错误表示结果未知，立即停止。
func (a *SlotOwnership) Ensure(ctx context.Context, service, slotID string, check func(context.Context, string) (bool, error)) (current SlotAssignment, created bool, err error) {
	current, err = a.store.ResolveSlot(ctx, slotID)
	if errors.Is(err, ErrSlotNotFound) {
		candidates := a.directory.RunningInstances(service)
		if len(candidates) == 0 {
			return current, false, ErrSlotRuntimeUnavailable
		}
		// 哈希只决定首次选址起点；成员增减不重算已存在的归属。
		start := int(xxhash.Sum64String(slotID) % uint64(len(candidates)))
		err = ErrSlotCapacityUnavailable
		for i := range candidates {
			candidate := candidates[(start+i)%len(candidates)].ID
			available, checkErr := check(ctx, candidate)
			if checkErr != nil {
				return current, false, checkErr
			}
			if !available {
				continue
			}
			current = SlotAssignment{SlotID: slotID, RuntimeInstanceID: candidate, Generation: 1}
			err = a.router.CreateSlot(ctx, current)
			if errors.Is(err, ErrSlotCapacityUnavailable) {
				continue
			}
			if errors.Is(err, ErrSlotConflict) {
				current, err = a.store.ResolveSlot(ctx, slotID)
			} else if err == nil {
				created = true
			}
			break // 写入结果未知时不改派；明确冲突才权威复核已有归属。
		}
	}
	if err != nil {
		return current, false, err
	}
	if !a.available(service, current.RuntimeInstanceID) {
		return current, created, ErrSlotRuntimeUnavailable
	}
	return current, created, nil
}

// Handoff 先权威复核，再检查目标容量、等待源实例全量排水与持久化，最后逐槽 CAS。
// prepare 必须确认对应启动身份完成收尾；多槽交接可复用执行方的成功收尾进度。
// 结果未知只返回错误，不自动接管、回滚已晋升槽位或重投业务。
func (a *SlotOwnership) Handoff(ctx context.Context, service string, current SlotAssignment, nextInstance string, check func(context.Context, string) (bool, error), prepare func(context.Context, string) error) (SlotAssignment, error) {
	next := SlotAssignment{SlotID: current.SlotID, RuntimeInstanceID: nextInstance, Generation: current.Generation + 1}
	if err := ValidateSlotPromotion(current, next); err != nil {
		return current, err
	}
	stored, err := a.store.ResolveSlot(ctx, current.SlotID)
	if err != nil {
		return current, err
	}
	if stored != current {
		return stored, ErrSlotConflict
	}
	if !a.available(service, nextInstance) {
		return current, ErrSlotRuntimeUnavailable
	}
	available, err := check(ctx, nextInstance)
	if err != nil {
		return current, err
	}
	if !available {
		return current, ErrSlotCapacityUnavailable
	}
	// 默认持久存储在排水前先检查稳定占位，晋升写入时仍做原子复核。
	if capacity, ok := a.store.(interface {
		CheckSlotCapacity(context.Context, SlotAssignment) error
	}); ok {
		if err := capacity.CheckSlotCapacity(ctx, next); err != nil {
			return current, err
		}
	}
	if err = prepare(ctx, current.RuntimeInstanceID); err != nil {
		return current, err
	}
	if err = a.router.PromoteSlot(ctx, current, next); err != nil {
		return SlotAssignment{}, err
	}
	return next, nil
}

func (a *SlotOwnership) available(service, instanceID string) bool {
	instance, ok := a.directory.LookupInstance(instanceID)
	return ok && instance.Status == StatusRunning && slices.Contains(instance.Services, service)
}

// Resolve 读取权威归属并校验服务成员状态，供低频控制查询使用；不创建或改派槽位。
func (a *SlotOwnership) Resolve(ctx context.Context, service, slotID string) (SlotTarget, error) {
	return a.router.resolver.ResolveServiceSlot(ctx, service, slotID)
}
