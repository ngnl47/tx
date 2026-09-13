package actor

import "context"

// Caller 是已绑定传输的实体调用入口；Agent 使用框架提供的 gRPC 直连视图。
type Caller interface {
	Call(context.Context, Identity, Delivery) (Result, error)
}

// Messenger 是实体间 Core NATS 调用契约，不开放连接和进程关闭权限。
type Messenger interface {
	Caller
	Tell(context.Context, Identity, Delivery) error
	TellAccepted(context.Context, Identity, Delivery) error
	CallFuture(context.Context, Identity, Delivery) (*Future, error)
}
