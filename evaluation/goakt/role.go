// Package goakteval 是 feature 分支上的最小 Role Grain 评估，不属于 tx 公共 API。
package goakteval

import (
	"context"
	"errors"
	"strconv"
	"time"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"
)

// Handler 在每次激活时由项目加载边界创建；运行时不认识 GD 或存储。
type Handler func(context.Context, *actorpb.Delivery) (*actorpb.Result, error)
type Factory func(context.Context, int64) (Handler, error)

type bindings struct{ factory Factory }

func (*bindings) ID() string { return "tx-role-bindings" }

type Runtime struct {
	system   actor.ActorSystem
	capacity int64
	idle     time.Duration
}

func Start(ctx context.Context, name string, capacity int64, idle time.Duration, factory Factory) (*Runtime, error) {
	system, err := actor.NewActorSystem(name, actor.WithLogger(nil), actor.WithExtensions(&bindings{factory: factory}))
	if err != nil {
		return nil, err
	}
	if err = system.Start(ctx); err != nil {
		return nil, err
	}
	return &Runtime{system: system, capacity: capacity, idle: idle}, nil
}

// Call 使用现有 protobuf 数据信封；请求关联交给 GoAkt，不维护第二份等待表。
// 这不是现有 Messenger 的完整替代：四种投递模式及排水仍需独立验收。
func (r *Runtime) Call(ctx context.Context, message *actorpb.Delivery) (*actorpb.Result, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("request deadline required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if message == nil || message.RouteKeyKind != actorpb.RouteKeyKind_ROUTE_KEY_INT64 || int64(message.RouteKeyNumeric) <= 0 {
		return nil, errors.New("positive role route required")
	}
	id, err := actor.GrainOf[*roleGrain](ctx, r.system, strconv.FormatInt(int64(message.RouteKeyNumeric), 10),
		actor.WithGrainMailboxCapacity(r.capacity), actor.WithGrainDeactivateAfter(r.idle))
	if err != nil {
		return nil, err
	}
	response, err := r.system.AskGrain(ctx, id, proto.Clone(message), time.Until(deadline))
	if err != nil {
		return nil, err
	}
	result, ok := response.(*actorpb.Result)
	if !ok {
		return nil, errors.New("invalid role response")
	}
	return proto.Clone(result).(*actorpb.Result), nil
}

// Stop 仅供评估资源清理，不是持久化排水入口，不得接入 Bee/Ant 关闭流程。
func (r *Runtime) Stop(ctx context.Context) error { return r.system.Stop(ctx) }

type roleGrain struct{ handle Handler }

func (g *roleGrain) OnActivate(ctx context.Context, props *actor.GrainProps) error {
	id, err := strconv.ParseInt(props.Identity().Name(), 10, 64)
	if err != nil {
		return err
	}
	g.handle, err = props.ActorSystem().Extension("tx-role-bindings").(*bindings).factory(ctx, id)
	return err
}
func (g *roleGrain) OnReceive(ctx *actor.GrainContext) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		ctx.Err(errors.New("invalid role delivery"))
		return
	}
	result, err := g.handle(ctx.Context(), message)
	if err != nil {
		ctx.Err(err)
		return
	}
	ctx.Response(result)
}
func (*roleGrain) OnDeactivate(context.Context, *actor.GrainProps) error { return nil }
