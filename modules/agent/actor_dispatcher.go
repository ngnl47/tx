package agent

import (
	"context"
	"errors"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	txactor "github.com/ngnl47/tx/modules/actor"
)

const defaultActorCallTimeout = 5 * time.Second

var (
	ErrActorMessengerNil        = errors.New("agent: actor messenger is nil")
	ErrActorDispatchResolverNil = errors.New("agent: actor dispatch resolver is nil")
	ErrActorTargetNil           = errors.New("agent: actor target is nil")
	ErrInvalidActorCallTimeout  = errors.New("agent: actor call timeout is invalid")
)

// ActorDispatch 是项目路由策略解析出的单次 Actor 投递参数。
// WantsResult 为 false 时使用 Tell；为 true 时使用 Call，并把业务结果作为当前会话响应返回。
type ActorDispatch struct {
	Target      *protoactor.PID
	Delivery    txactor.Delivery
	WantsResult bool
}

// ActorDispatchResolver 将已经通过会话校验的业务帧转换为目标 Actor 投递参数。
// 命令号、目标服务、Actor 类型和路由键的业务含义仍由项目生成代码与路由策略决定。
type ActorDispatchResolver interface {
	ResolveActorDispatch(context.Context, DispatchRequest) (ActorDispatch, error)
}

// ActorDispatchResolverFunc 将函数适配为 ActorDispatchResolver。
type ActorDispatchResolverFunc func(context.Context, DispatchRequest) (ActorDispatch, error)

func (resolver ActorDispatchResolverFunc) ResolveActorDispatch(ctx context.Context, request DispatchRequest) (ActorDispatch, error) {
	return resolver(ctx, request)
}

// ActorDispatcherOptions 配置通用 Agent 到 Actor 的投递适配器。
type ActorDispatcherOptions struct {
	// Messenger 是当前进程共享的 Actor 消息入口。
	Messenger txactor.Messenger
	// Resolver 由项目根据生成命令元数据选择目标 PID 和投递模式。
	Resolver ActorDispatchResolver
	// CallTimeout 是等待业务结果的最长时间；零值使用 5 秒默认值。
	CallTimeout time.Duration
}

// NewActorDispatcher 创建通用的 Agent 到 Actor 分发器。
// 单向命令保持 Tell 路径；有直接响应的命令使用带 deadline 的 Call。
func NewActorDispatcher(options ActorDispatcherOptions) (Dispatcher, error) {
	if options.Messenger == nil {
		return nil, ErrActorMessengerNil
	}
	if options.Resolver == nil {
		return nil, ErrActorDispatchResolverNil
	}
	if options.CallTimeout < 0 {
		return nil, ErrInvalidActorCallTimeout
	}
	if options.CallTimeout == 0 {
		options.CallTimeout = defaultActorCallTimeout
	}
	return DispatchFunc(func(ctx context.Context, request DispatchRequest) (DispatchResult, error) {
		dispatch, err := options.Resolver.ResolveActorDispatch(ctx, request)
		if err != nil {
			return DispatchResult{}, err
		}
		if dispatch.Target == nil {
			return DispatchResult{}, ErrActorTargetNil
		}
		dispatch.Delivery.Identity = &request.Identity
		dispatch.Delivery.UpSeq = request.Seq
		if !dispatch.WantsResult {
			return DispatchResult{}, options.Messenger.Tell(dispatch.Target, dispatch.Delivery)
		}
		callCtx, cancel := context.WithTimeout(ctx, options.CallTimeout)
		defer cancel()
		result, err := options.Messenger.Call(callCtx, dispatch.Target, dispatch.Delivery)
		if err != nil {
			return DispatchResult{}, err
		}
		return DispatchResult{Payload: result.Payload}, nil
	}), nil
}
