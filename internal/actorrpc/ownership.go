package actorrpc

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/modules/discovery"
)

// EntityRouter 统一绑定缓存寻址、首次分配及激活；归属决策仍由 L2 维护。
// 装配时须统一绑定 Service 与 SlotID，不由业务调用逐次选择。
type EntityRouter struct {
	Resolver  *discovery.SlotResolver
	Ownership *discovery.SlotOwnership
	Transport *NATSTransport
	Service   string
	SlotID    actorhost.SlotMapping
}

// Ensure 先无副作用预检、CAS 确立归属，再发起激活；失败不会重投业务或改派。
// 状态可能仍在初始化中，不把控制请求成功当作业务对象已经加载完成。
func (a *EntityRouter) Ensure(ctx context.Context, id actorhost.Identity) (discovery.SlotAssignment, actorhost.ActivationStatus, error) {
	current, created, err := a.Ownership.Ensure(ctx, a.Service, a.SlotID.Resolve(id), func(ctx context.Context, instance string) (bool, error) {
		err := a.Transport.CheckActivationCapacity(ctx, actorhost.Target{Instance: instance, Identity: id})
		if errors.Is(err, actorhost.ErrCapacity) || errors.Is(err, actorhost.ErrDraining) {
			return false, nil
		}
		return err == nil, err
	})
	if errors.Is(err, discovery.ErrSlotCapacityUnavailable) {
		err = actorhost.ErrCapacity
	}
	if err != nil {
		return current, actorhost.ActivationStatus{}, err
	}
	target := actorhost.Target{Instance: current.RuntimeInstanceID, Identity: id}
	var state actorhost.ActivationStatus
	if created {
		state, err = a.Transport.RetryActivation(ctx, target)
	} else {
		state, err = a.Transport.EnsureActivation(ctx, target)
	}
	return current, state, err
}

// Resolve 优先读取 Watch 缓存，仅明确缺少归属时回源并分配。
// 快照不可用、旧 owner 下线或调用失败均不触发自动改派或业务重投。
func (a *EntityRouter) Resolve(ctx context.Context, id actorhost.Identity) (actorhost.Target, error) {
	current, err := a.Resolver.ResolveSlot(ctx, a.SlotID.Resolve(id))
	if err == nil {
		return actorhost.Target{Instance: current.Assignment.RuntimeInstanceID, Identity: id}, nil
	}
	if !errors.Is(err, discovery.ErrSlotNotFound) {
		return actorhost.Target{}, err
	}
	assignment, _, err := a.Ensure(ctx, id)
	if err != nil {
		return actorhost.Target{}, err
	}
	// 不等待 Watch 回显，也不向只读缓存手写归属；本次复用权威结果。
	return actorhost.Target{Instance: assignment.RuntimeInstanceID, Identity: id}, nil
}
