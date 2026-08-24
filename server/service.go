package server

import (
	"context"
)

// Spec 是可选服务的稳定身份和 runtime 契约。
type Spec struct {
	Name     string
	Requires []CModID
}

// Service 表示一个可独立选择的进程服务。
type Service interface {
	Spec() Spec
	Start(Context) error
}

// Drainer 让服务停止接受新的外部工作。
type Drainer interface {
	Drain(context.Context) error
}

// Stopper 释放服务持有的资源。
// Stop 必须幂等，并能清理由失败的 Start 留下的部分资源。
type Stopper interface {
	Stop(context.Context) error
}
