package actor

import "context"

// Caller 是已绑定传输的实体调用入口；Agent 使用框架提供的 gRPC 直连视图。
type Caller interface {
	Call(context.Context, Identity, Delivery) (Result, error)
}

// Enqueuer 将消息交给本地运行时或写入远端流；远端路径不等待邮箱接收或业务执行。
type Enqueuer interface {
	Enqueue(context.Context, Identity, Delivery) error
}

// Messenger 同实例使用本地邮箱，跨实例使用 Core NATS；不开放连接和进程关闭权限。
type Messenger interface {
	Caller
	// Tell 提交本地邮箱或远端传输，不等待业务执行；已知准入错误立即返回。
	Tell(context.Context, Identity, Delivery) error
	// TellAccepted 同实例确认邮箱接纳，跨实例确认 NATS 到达，不确认业务执行或持久化。
	TellAccepted(context.Context, Identity, Delivery) error
	CallFuture(context.Context, Identity, Delivery) (*Future, error)
}

// Runtime 组合业务消息与低频激活控制，不开放宿主关闭或连接权限。
// 只发送消息时依赖 Messenger / Caller，只管理激活时依赖 Controller。
type Runtime interface {
	Messenger
	Controller
}
