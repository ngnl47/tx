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
	ErrActorEnqueuerNil         = errors.New("agent: actor enqueuer is unavailable")
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

// ActorDispatcherOptions 的 Caller 应使用 CModActorDirect 提供的 gRPC 入队视图。
type ActorDispatcherOptions struct {
	Caller      txactor.Caller
	Resolver    ActorDispatchResolver
	CallTimeout time.Duration
}

// NewActorDispatcher 将上行写入目标实例的流，不等待远端邮箱确认；业务下行由目标服务发布到 MsgQ。
func NewActorDispatcher(o ActorDispatcherOptions) (Dispatcher, error) {
	if o.Caller == nil {
		return nil, ErrActorCallerNil
	}
	if o.Resolver == nil {
		return nil, ErrActorDispatchResolverNil
	}
	enqueuer, ok := o.Caller.(txactor.Enqueuer)
	if !ok {
		return nil, ErrActorEnqueuerNil
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
		err = enqueuer.Enqueue(call, d.Target, d.Delivery)
		if err != nil {
			return DispatchResult{}, err
		}
		return DispatchResult{AcceptedUpSeq: r.Seq}, nil
	}), nil
}
