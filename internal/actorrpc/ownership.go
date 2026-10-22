package actorrpc

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/modules/discovery"
)

// EntityRouter 统一绑定缓存寻址、首次分配及激活；归属决策仍由 L2 维护。
// Service 从应用目录解析 Type 的服务池；装配时与 SlotID 统一绑定，不由业务逐次选择。
type EntityRouter struct {
	Resolver  *discovery.SlotResolver
	Ownership *discovery.SlotOwnership
	Transport *NATSTransport
	Service   func(actorType string) (string, error)
	SlotID    actorhost.SlotMapping
}

// Ensure 先无副作用预检、CAS 确立归属，再发起激活；失败不会重投业务或改派。
// 状态可能仍在初始化中，不把控制请求成功当作业务对象已经加载完成。
func (a *EntityRouter) Ensure(ctx context.Context, id actorhost.Identity) (discovery.SlotAssignment, actorhost.ActivationStatus, error) {
	return a.ensure(ctx, id, false)
}

// Retry 重试业务明确要求恢复的失败加载，归属仍由同一控制路径校验。
func (a *EntityRouter) Retry(ctx context.Context, id actorhost.Identity) (discovery.SlotAssignment, actorhost.ActivationStatus, error) {
	return a.ensure(ctx, id, true)
}

func (a *EntityRouter) ensure(ctx context.Context, id actorhost.Identity, retry bool) (discovery.SlotAssignment, actorhost.ActivationStatus, error) {
	if id.ID == "" {
		return discovery.SlotAssignment{}, actorhost.ActivationStatus{}, errors.New("actor: empty actor ID")
	}
	service, err := a.Service(id.Type)
	if err != nil {
		return discovery.SlotAssignment{}, actorhost.ActivationStatus{}, err
	}
	current, created, err := a.Ownership.Ensure(ctx, service, a.SlotID.Resolve(id), func(ctx context.Context, instance string) (bool, error) {
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
	if created || retry {
		state, err = a.Transport.RetryActivation(ctx, target)
	} else {
		state, err = a.Transport.EnsureActivation(ctx, target)
	}
	return current, state, err
}

// Resolve 优先读取 Watch 缓存，仅明确缺少归属时回源并分配。
// 快照不可用、旧 owner 下线或调用失败均不触发自动改派或业务重投。
func (a *EntityRouter) Resolve(ctx context.Context, id actorhost.Identity) (actorhost.Target, error) {
	service, err := a.Service(id.Type)
	if err != nil {
		return actorhost.Target{}, err
	}
	current, err := a.Resolver.ResolveServiceSlot(ctx, service, a.SlotID.Resolve(id))
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

// Probe 走只读权威解析，避免 Watch 尚未回显时把刚创建的归属误报为未加载。
func (a *EntityRouter) Probe(ctx context.Context, id actorhost.Identity) (actorhost.ActivationStatus, error) {
	if id.ID == "" {
		return actorhost.ActivationStatus{}, errors.New("actor: empty actor ID")
	}
	service, err := a.Service(id.Type)
	if err != nil {
		return actorhost.ActivationStatus{}, err
	}
	target, err := a.Ownership.Resolve(ctx, service, a.SlotID.Resolve(id))
	if errors.Is(err, discovery.ErrSlotNotFound) {
		return actorhost.ActivationStatus{State: actorhost.Unloaded}, nil
	}
	if err != nil {
		return actorhost.ActivationStatus{}, err
	}
	return a.Transport.Probe(ctx, actorhost.Target{Instance: target.Assignment.RuntimeInstanceID, Identity: id})
}
