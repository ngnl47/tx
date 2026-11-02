package agent

import (
	"context"
	"errors"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
)

const defaultActorDispatchTimeout = 5 * time.Second

var (
	ErrActorTellerNil              = errors.New("agent: actor teller is nil")
	ErrActorDispatchResolverNil    = errors.New("agent: actor dispatch resolver is nil")
	ErrActorTargetNil              = errors.New("agent: actor identity is empty")
	ErrInvalidActorDispatchTimeout = errors.New("agent: actor dispatch timeout is invalid")
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

// ActorDispatcherOptions 的 Teller 应使用 CModActorDirect 提供的 gRPC 投递视图。
type ActorDispatcherOptions struct {
	// Teller 必须提供；投递成功只表示本地入队或远端写流，不表示远端邮箱接纳或业务完成。
	Teller txactor.Teller
	// Resolver 必须提供，用于解析业务身份和投递信封。
	Resolver ActorDispatchResolver
	// DispatchTimeout 仅限制身份解析后的投递；负数非法，零值使用默认 5 秒。
	DispatchTimeout time.Duration
}

// NewActorDispatcher 将上行写入目标实例的流，不等待远端邮箱确认；业务下行由目标服务发布到 MsgQ。
func NewActorDispatcher(o ActorDispatcherOptions) (Dispatcher, error) {
	if o.Teller == nil {
		return nil, ErrActorTellerNil
	}
	if o.Resolver == nil {
		return nil, ErrActorDispatchResolverNil
	}
	if o.DispatchTimeout < 0 {
		return nil, ErrInvalidActorDispatchTimeout
	}
	if o.DispatchTimeout == 0 {
		o.DispatchTimeout = defaultActorDispatchTimeout
	}
	return DispatchFunc(func(ctx context.Context, r DispatchRequest) error {
		d, err := o.Resolver.ResolveActorDispatch(ctx, r)
		if err != nil {
			return err
		}
		if d.Target.Type == "" || d.Target.ID == "" {
			return ErrActorTargetNil
		}
		d.Delivery.Identity = &r.Identity
		d.Delivery.Origin = txactor.OriginClient
		dispatch, cancel := context.WithTimeout(ctx, o.DispatchTimeout)
		defer cancel()
		return o.Teller.Tell(dispatch, d.Target, d.Delivery)
	}), nil
}
