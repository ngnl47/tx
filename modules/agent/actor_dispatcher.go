package agent

import (
	"context"
	"errors"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
)

const defaultActorCallTimeout = 5 * time.Second

var (
	ErrActorCallerNil           = errors.New("agent: actor caller is nil")
	ErrActorDispatchResolverNil = errors.New("agent: actor dispatch resolver is nil")
	ErrActorTargetNil           = errors.New("agent: actor identity is empty")
	ErrInvalidActorCallTimeout  = errors.New("agent: actor call timeout is invalid")
	ErrInvalidDispatchResult    = errors.New("agent: invalid dispatch result")
)

// ActorDispatch 只包含业务身份与信封，物理归属由框架解析。
type ActorDispatch struct {
	Target   txactor.Identity
	Delivery txactor.Delivery
}

// ActorDispatchResolver 按生成命令元数据和认证身份选择实体。
type ActorDispatchResolver interface {
	ResolveActorDispatch(context.Context, DispatchRequest) (ActorDispatch, error)
}
type ActorDispatchResolverFunc func(context.Context, DispatchRequest) (ActorDispatch, error)

func (f ActorDispatchResolverFunc) ResolveActorDispatch(ctx context.Context, r DispatchRequest) (ActorDispatch, error) {
	return f(ctx, r)
}

// ActorDispatcherOptions 的 Caller 应使用 CModActorDirect 提供的 gRPC 视图。
type ActorDispatcherOptions struct {
	Caller      txactor.Caller
	Resolver    ActorDispatchResolver
	CallTimeout time.Duration
}

// NewActorDispatcher 等待上行完成确认；业务下行由目标服务直接发布到 MsgQ。
func NewActorDispatcher(o ActorDispatcherOptions) (Dispatcher, error) {
	if o.Caller == nil {
		return nil, ErrActorCallerNil
	}
	if o.Resolver == nil {
		return nil, ErrActorDispatchResolverNil
	}
	if o.CallTimeout < 0 {
		return nil, ErrInvalidActorCallTimeout
	}
	if o.CallTimeout == 0 {
		o.CallTimeout = defaultActorCallTimeout
	}
	return DispatchFunc(func(ctx context.Context, r DispatchRequest) (DispatchResult, error) {
		d, err := o.Resolver.ResolveActorDispatch(ctx, r)
		if err != nil {
			return DispatchResult{}, err
		}
		if d.Target.Kind == "" || d.Target.Key == "" {
			return DispatchResult{}, ErrActorTargetNil
		}
		d.Delivery.Identity = &r.Identity
		d.Delivery.UpSeq = r.Seq
		call, cancel := context.WithTimeout(ctx, o.CallTimeout)
		defer cancel()
		result, err := o.Caller.Call(call, d.Target, d.Delivery)
		if err != nil {
			return DispatchResult{}, err
		}
		if result.ProcessedUpSeq != r.Seq || result.MessageType != "" || result.CommandID != 0 || len(result.Payload) != 0 {
			return DispatchResult{}, ErrInvalidDispatchResult
		}
		return DispatchResult{ProcessedUpSeq: result.ProcessedUpSeq}, nil
	}), nil
}
