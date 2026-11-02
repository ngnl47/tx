package actor

import "context"

// Caller 是已绑定传输的实体同步调用入口。
type Caller interface {
	Call(context.Context, Identity, Delivery) (Result, error)
}

// Teller 提交本地邮箱或远端传输，不等待业务执行；已知准入错误立即返回。
// Agent 直连使用 gRPC 长流，写流成功不表示远端邮箱接纳。
type Teller interface {
	Tell(context.Context, Identity, Delivery) error
}

// Messenger 同实例使用本地邮箱，跨实例使用 Core NATS；不开放连接和进程关闭权限。
type Messenger interface {
	Caller
	Teller
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
