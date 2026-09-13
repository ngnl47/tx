package actorrpc

import (
	"context"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

// Caller 是两种传输共有的完成结果契约；TellAccepted 的 NATS 确认含义不推广到直连。
type Caller interface {
	Call(context.Context, actorhost.Target, *actorpb.Delivery) (*actorpb.Result, error)
}

// RoutedCaller 为同一实体路由绑定指定传输；上行装配直连，实体间 RPC 装配 NATS。
// 不因业务调用失败再次寻址或重投。
type RoutedCaller struct {
	Router    *EntityRouter
	Transport Caller
}

func (c *RoutedCaller) Call(ctx context.Context, id actorhost.Identity, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	target, err := c.Router.Resolve(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.Transport.Call(ctx, target, delivery)
}
