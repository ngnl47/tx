package agent

import (
	"context"
	"errors"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/maypok86/otter/v2"
	txactor "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	"github.com/ngnl47/tx/modules/discovery"
)

const defaultActorCallTimeout = 5 * time.Second

var (
	ErrActorMessengerNil        = errors.New("agent: actor messenger is nil")
	ErrActorDispatchResolverNil = errors.New("agent: actor dispatch resolver is nil")
	ErrActorTargetNil           = errors.New("agent: actor target is nil")
	ErrInvalidActorCallTimeout  = errors.New("agent: actor call timeout is invalid")
	ErrInvalidDispatchResult    = errors.New("agent: invalid dispatch result")
)

// ActorDispatch 是项目路由策略解析出的单次 Actor 投递参数。
type ActorDispatch struct {
	Target   *protoactor.PID
	Delivery txactor.Delivery
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
	// Directory 提供本地地址身份查询；不支持该能力时跳过排水缓存。
	Directory discovery.Directory
	// Messenger 是当前进程共享的 Actor 消息入口。
	Messenger txactor.Messenger
	// Resolver 由项目根据生成命令元数据选择目标 PID 和投递内容。
	Resolver ActorDispatchResolver
	// CallTimeout 是等待 Actor 处理完成确认的最长时间；零值使用 5 秒默认值。
	CallTimeout time.Duration
}

// NewActorDispatcher 创建通用的 Agent 到 Actor 分发器。所有 Agent 上行均使用
// 带 deadline 的 Call 等待处理完成确认，业务响应不经过 Agent 返回。
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
	lookup, _ := options.Directory.(discovery.ActorEndpointDirectory)
	// 仅保存短期拒绝时间；容量淘汰只减少优化效果，不改变准入语义。
	var draining *otter.Cache[string, time.Time]
	if lookup != nil {
		draining = otter.Must(&otter.Options[string, time.Time]{MaximumSize: 4096, InitialCapacity: 16})
	}
	return DispatchFunc(func(ctx context.Context, request DispatchRequest) (DispatchResult, error) {
		dispatch, err := options.Resolver.ResolveActorDispatch(ctx, request)
		if err != nil {
			return DispatchResult{}, err
		}
		if dispatch.Target == nil {
			return DispatchResult{}, ErrActorTargetNil
		}
		// 固定本次调用的实例身份，迟到的旧响应不得污染同地址的新实例。
		var instanceID string
		if lookup != nil {
			if id, found := lookup.LookupActorInstance(dispatch.Target.Address); found {
				instanceID = id
			}
			if instanceID != "" {
				if until, ok := draining.GetIfPresent(instanceID); ok && time.Now().Before(until) {
					return DispatchResult{}, &txactor.CallError{Code: txactor.CallErrorCodeRejected, Message: txactor.ErrRuntimeDraining.Error()}
				}
			}
		}
		dispatch.Delivery.Identity = &request.Identity
		dispatch.Delivery.UpSeq = request.Seq
		callCtx, cancel := context.WithTimeout(ctx, options.CallTimeout)
		defer cancel()
		result, err := options.Messenger.Call(callCtx, dispatch.Target, dispatch.Delivery)
		if err != nil {
			var failure *txactor.CallError
			if instanceID != "" && errors.As(err, &failure) && failure.Code == txactor.CallErrorCodeRejected {
				draining.Set(instanceID, time.Now().Add(time.Second))
			}
			return DispatchResult{}, err
		}
		if result.ProcessedUpSeq != request.Seq || result.MessageType != "" || result.CommandID != 0 || len(result.Payload) != 0 {
			return DispatchResult{}, ErrInvalidDispatchResult
		}
		return DispatchResult{ProcessedUpSeq: result.ProcessedUpSeq}, nil
	}), nil
}
