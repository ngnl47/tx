// Package goakteval 是 feature 分支上的最小 Role Grain 评估，不属于 tx 公共 API。
package goakteval

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Handler 在每次激活时由项目加载边界创建；运行时不认识 GD 或存储。
type Handler func(context.Context, *actorpb.Delivery) (*actorpb.Result, error)
type Factory func(context.Context, int64) (Handler, error)

type bindings struct {
	factory Factory
	context context.Context
}

func (*bindings) ID() string { return "tx-role-bindings" }

type Runtime struct {
	system   actor.ActorSystem
	capacity int64
	idle     time.Duration
	// 统计四种入口的提交/等待阶段，不能代替邮箱完成确认。
	ingress  *lifecycle.Ingress
	shutdown shutdownState
	slots    chan struct{}
}

func Start(ctx context.Context, name string, capacity int64, maxPending int, idle time.Duration, factory Factory, options ...actor.Option) (*Runtime, error) {
	if maxPending <= 0 {
		return nil, errors.New("positive pending capacity required")
	}
	options = append(options, actor.WithLogger(nil), actor.WithExtensions(&bindings{factory: factory, context: ctx}))
	system, err := actor.NewActorSystem(name, options...)
	if err != nil {
		return nil, err
	}
	if err = system.Start(ctx); err != nil {
		return nil, err
	}
	return &Runtime{system: system, capacity: capacity, idle: idle, ingress: lifecycle.NewIngress(nil), slots: make(chan struct{}, maxPending)}, nil
}

// Call 使用现有 protobuf 数据信封；请求关联交给 GoAkt，不维护第二份等待表。
// 四种入口目前仅完成本地映射，跨节点投递及排水仍需独立验收。
func (r *Runtime) Call(ctx context.Context, message *actorpb.Delivery) (*actorpb.Result, error) {
	f, err := r.CallFuture(ctx, message)
	if err != nil {
		return nil, err
	}
	return f.Await(ctx)
}

// Stop 仅供评估资源清理，不是持久化排水入口，不得接入 Bee/Ant 关闭流程。
func (r *Runtime) Stop(ctx context.Context) error { return r.system.Stop(ctx) }

type roleGrain struct {
	handle  Handler
	context context.Context
}

func (g *roleGrain) OnActivate(ctx context.Context, props *actor.GrainProps) error {
	id, err := strconv.ParseInt(props.Identity().Name(), 10, 64)
	if err != nil {
		return err
	}
	bound := props.ActorSystem().Extension("tx-role-bindings").(*bindings)
	g.context = bound.context
	g.handle, err = bound.factory(ctx, id)
	return err
}
func (g *roleGrain) OnReceive(ctx *actor.GrainContext) {
	// 排水确认与业务使用同一 FIFO 邮箱；不得放到优先系统消息通道。
	if _, ok := ctx.Message().(*emptypb.Empty); ok {
		ctx.Response(&emptypb.Empty{})
		return
	}
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		ctx.Err(errors.New("invalid role delivery"))
		return
	}
	if message.Mode != actorpb.DeliveryMode_DELIVERY_MODE_CALL {
		ctx.Response(&actorpb.Acceptance{CommandId: message.CommandId})
		_, err := g.handle(g.context, message)
		if err != nil {
			reportDeliveryError(message, err)
		}
		return
	}
	result, err := g.handle(g.context, message)
	if err != nil {
		ctx.Response(errorResult(err))
		return
	}
	if result == nil {
		ctx.Err(ErrInvalidResponse)
		return
	}
	ctx.Response(proto.Clone(result))
}
func (*roleGrain) OnDeactivate(context.Context, *actor.GrainProps) error { return nil }
