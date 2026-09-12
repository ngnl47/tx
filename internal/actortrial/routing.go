package actortrial

import (
	"context"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

// Caller 是两种传输共有的完成结果契约；TellAccepted 的 NATS 确认含义不推广到直连。
type Caller interface {
	Call(context.Context, Target, *actorpb.Delivery) (*actorpb.Result, error)
}

// RoutedCaller 在组合根绑定路由和传输，业务只给出实体身份。
// Resolver 应使用 Watch 缓存；首次分配和滚动交接仍由 SlotAllocator 单独处理。
type RoutedCaller struct {
	Resolver  *discovery.SlotResolver
	SlotID    func(Identity) string
	Transport Caller
}

func (c *RoutedCaller) Call(ctx context.Context, id Identity, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	target, err := c.Resolver.ResolveSlot(ctx, c.SlotID(id))
	if err != nil {
		return nil, err
	}
	return c.Transport.Call(ctx, Target{target.Assignment.RuntimeInstanceID, id}, delivery)
}
