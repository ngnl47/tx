package server

import (
	"context"

	txactor "github.com/ngnl47/tx/modules/actor"
)

// Spec 是可选服务的稳定身份和 runtime 契约。
type Spec struct {
	// Name 是命令行 --services、服务发现和日志共同使用的稳定服务名。
	Name string
	// Requires 声明该 L4 服务需要启用的 L3 能力模组。
	Requires []CModID
	// ActorKinds 声明该服务可以承载的 Actor 类型；空切片表示不注册 Actor 能力。
	ActorKinds []txactor.Kind
}

// Service 表示一个可独立选择的进程服务。
type Service interface {
	// Spec 返回服务的稳定身份和能力依赖，不得创建资源或启动 goroutine。
	Spec() Spec
	// Start 在依赖能力就绪后启动服务。
	Start(Context) error
}

// Drainer 让服务停止接受新的外部工作。
type Drainer interface {
	// Drain 停止接收新的外部工作，并在 ctx 截止前完成已接纳工作。
	Drain(context.Context) error
}

// Stopper 释放服务持有的资源。
// Stop 必须幂等，并能清理由失败的 Start 留下的部分资源。
type Stopper interface {
	// Stop 释放服务持有的资源。
	Stop(context.Context) error
}
