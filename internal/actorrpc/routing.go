package actorrpc

import (
	"context"
	"errors"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

// Caller 是两种传输共有的完成结果契约；TellAccepted 的 NATS 确认含义不推广到直连。
type Caller interface {
	Call(context.Context, actorhost.Target, *actorpb.Delivery) (*actorpb.Result, error)
}

type Enqueuer interface {
	Enqueue(context.Context, actorhost.Target, *actorpb.Delivery) error
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

func (c *RoutedCaller) Enqueue(ctx context.Context, id actorhost.Identity, delivery *actorpb.Delivery) error {
	target, err := c.Router.Resolve(ctx, id)
	// 此时尚未调用传输：正常交接可能已摘除旧实例，但新归属还未进入 Watch 缓存。
	// 只在调用方给出的期限内等待路由；不改变归属，也不重试任何发送结果。
	if _, bounded := ctx.Deadline(); bounded && errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				target, err = c.Router.Resolve(ctx, id)
			}
		}
	}
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	enqueuer, ok := c.Transport.(Enqueuer)
	if !ok {
		return errors.New("actor: direct transport does not support mailbox enqueue")
	}
	return enqueuer.Enqueue(ctx, target, delivery)
}
