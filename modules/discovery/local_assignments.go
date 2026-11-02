package discovery

import (
	"context"
	"errors"
)

// LocalAssignments 只允许为本次运行实例认领槽位，不开放任意目标和 generation 写入。
// previousOwner 仅用于运维已确认旧实例停止写入后的显式接管；正常释放的槽位无需提供。
// 成功返回前会按权威版本更新本地缓存；不能以此证明其他进程已经同步观察到新归属。
type LocalAssignments interface {
	Claim(ctx context.Context, slotID, previousOwner string) (SlotAssignment, error)
}

type localAssignments struct{ runtime *Runtime }

// LocalAssignments 返回绑定本次运行身份的认领能力，不转移存储连接和生命周期所有权。
func (runtime *Runtime) LocalAssignments() LocalAssignments {
	if runtime == nil {
		return nil
	}
	return localAssignments{runtime: runtime}
}

func (access localAssignments) Claim(ctx context.Context, slotID, previousOwner string) (SlotAssignment, error) {
	r := access.runtime
	if err := ctx.Err(); err != nil {
		return SlotAssignment{}, err
	}
	// Watch 中断时不使用单键读取恢复整个目录，也不继续启动业务对象。
	if _, err := r.slotDirectory.resolve(slotID); errors.Is(err, ErrSlotDirectoryUnavailable) || errors.Is(err, ErrInvalidSlotAssignment) {
		return SlotAssignment{}, err
	}
	current, err := r.persistentSlots.ResolveSlot(ctx, slotID)
	next := SlotAssignment{SlotID: slotID, RuntimeInstanceID: r.instance.ID, Generation: 1}
	switch {
	case errors.Is(err, ErrSlotNotFound):
		if previousOwner != "" {
			return SlotAssignment{}, ErrSlotNotFound
		}
		err = r.persistentSlots.CreateSlot(ctx, next)
	case err != nil:
		return SlotAssignment{}, err
	case current.RuntimeInstanceID == r.instance.ID:
		next = current
	case !current.Released() && (previousOwner == "" || previousOwner != current.RuntimeInstanceID):
		return SlotAssignment{}, ErrSlotConflict
	default:
		next.Generation = current.Generation + 1
		err = r.persistentSlots.PromoteSlot(ctx, current, next)
	}
	// 未知写入结果不自动重试；调用方决定是否结束启动并人工核对。
	if err != nil {
		return SlotAssignment{}, err
	}
	actual, err := r.slots.refreshSlot(ctx, slotID)
	if err != nil {
		return SlotAssignment{}, err
	}
	if actual != next {
		return SlotAssignment{}, ErrSlotConflict
	}
	return actual, nil
}
