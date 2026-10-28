package lifecycle

import "errors"

// ServiceState 描述一个子服务的阶段，独立于进程发现状态和 Actor 状态。
type ServiceState string

const (
	ServiceCreated      ServiceState = "created"
	ServiceInitializing ServiceState = "initializing"
	ServiceInitialized  ServiceState = "initialized"
	ServiceStarting     ServiceState = "starting"
	ServiceRunning      ServiceState = "running"
	ServiceDraining     ServiceState = "draining"
	ServiceStopped      ServiceState = "stopped"
)

// ErrNotReady 表示子服务尚未开放业务入口；控制面加载不等同于业务准入。
var ErrNotReady = errors.New("service: not ready")

// ServiceStatus 保留当前阶段及最近一次生命周期失败，关闭成功不会抹除失败原因。
type ServiceStatus struct {
	State   ServiceState
	Failure error
}
