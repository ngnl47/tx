package component

import "context"

// ID 是组件在一个生命周期宿主内的稳定身份。
type ID string

// Spec 声明组件的必选依赖和可选启动顺序。
type Spec struct {
	// ID 用于依赖解析、去重和诊断。
	ID ID
	// Requires 会把依赖加入本次选择闭包，并要求依赖先启动。
	Requires []ID
	// After 只在目标已经被选择时调整启动顺序，不会隐式选择目标。
	After []ID
}

// Root 描述一项由上层来源请求的根组件。
type Root struct {
	// Source 是请求组件的上层来源名称，用于输出完整诊断路径。
	Source string
	// ID 是来源直接请求的组件。
	ID ID
}

// Component 是由 Host 管理启动顺序和生命周期的中立组件。
type Component interface {
	Spec() Spec
	Start(Context) error
}

// Validator 在任何组件启动前校验配置。
type Validator interface {
	Validate(Context) error
}

// Drainer 在根生命周期仍有效时停止入口或冲刷状态。
type Drainer interface {
	Drain(context.Context) error
}

// Stopper 释放组件持有的物理资源。Stop 必须幂等。
type Stopper interface {
	Stop(context.Context) error
}
