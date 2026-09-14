package actor

import "github.com/ngnl47/tx/internal/actorhost"

// CapacityScope 决定名额何时释放；默认随激活清理，稳定世界随归属交接。
type CapacityScope string

const (
	CapacityActivations CapacityScope = "activation"
	CapacityAssignments CapacityScope = "assignment"
)

// Capacity 配置本实例全部本地 Kind 共用的数量上限，零值不限制。
// assignment 模式要求独立实体槽位；即便 Actor 卸载，归属仍占名额。
// Initializations 单独限制同时初始化数量，不限制初始化时间。
type Capacity struct {
	Maximum         int
	Scope           CapacityScope
	Initializations int
}

// ActivationState 表示运行时生命周期阶段，不表示业务实体的持久存在性。
type ActivationState = actorhost.ActivationState

const (
	Unloaded     = actorhost.Unloaded
	Initializing = actorhost.Initializing
	Ready        = actorhost.Ready
	Closing      = actorhost.Closing
	Failed       = actorhost.Failed
)

// ActivationStatus 是探测瞬间的元数据快照，不读取业务对象或未就绪 GD。
// Failure 保留最近失败，可用 errors.Is 判定 ErrActivationDenied；失败记录有界且非永久登记。
// 控制调用返回非 nil error 时不得依赖状态，网络故障或容量不足均不等同于不存在。
type ActivationStatus = actorhost.ActivationStatus

// ActivationFailure 保留远端错误码，并支持框架错误的 errors.Is 分类。
type ActivationFailure = actorhost.RemoteError

// ErrCapacity 表示当前不能预留激活资源，不代表实体不存在或允许自动改派。
var ErrCapacity = actorhost.ErrCapacity
