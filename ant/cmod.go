package ant

import (
	"context"

	framework "github.com/ngnl47/tx/internal/framework"
)

// CModID 是 Ant 能力模组的稳定身份。
type CModID string

const (
	// CModTaskQueue 提供持久任务投递与可选消费。
	CModTaskQueue CModID = "task-queue"
	// CModRedisStorage 提供 Redis 文档读写与具名命令接口。
	CModRedisStorage CModID = "redis-storage"
	// CModMongoStorage 提供 MongoDB 文档读写能力。
	CModMongoStorage CModID = "mongo-storage"
	// CModFusionStorage 组合 Redis 热数据、MongoDB 冷数据和延迟回写能力。
	CModFusionStorage CModID = "fusion-storage"
	// CModRPC 提供进程共享的 gRPC Host。
	CModRPC CModID = "rpc"
	// CModDiscovery 提供 etcd 注册、发现和实例目录。
	CModDiscovery CModID = "discovery"
	// CModSessionID 提供绑定 Discovery 运行身份的 SessionID 分配能力。
	CModSessionID CModID = "session-id"
	// CModActor 提供 Actor 消息投递能力。
	CModActor CModID = "actor"
	// CModProcedure 组合单机事务、缓存提交参与者与直接写回。
	CModProcedure CModID = "procedure"
)

// SessionIDAllocator 只分配当前集群内有效生命周期的 SessionID。
type SessionIDAllocator interface {
	AllocateSessionID(context.Context) (int64, error)
}

// CModSpec 声明能力模组的身份和依赖顺序。
type CModSpec struct {
	// ID 是参与依赖解析、去重和启动日志的稳定能力标识。
	ID CModID
	// Requires 声明本 CMod 的直接前置能力，递归加入启动闭包并要求它们先启动。
	// 本 CMod 从 Context 取得的能力须在此显式声明，不因间接依赖已启动而开放访问。
	Requires []CModID
	// After 只对已被选中的 CMod 增加启动顺序，不会隐式启用它们。
	After []CModID
	// Components 描述该 L3 能力门面内部的强类型组件拓扑。
	Components []ComponentSpec
}

// ComponentSpec 描述 CMod 内部组件拓扑，只用于校验、日志和审计。
type ComponentSpec struct {
	// Role 是组件在父 Builder 中的稳定职责名称，同级不可重复。
	Role string
	// Name 是当前职责选用的具体实现名称。
	Name string
	// Optional 表示组件只在 Services 命中本轮 L4 服务时生效。
	Optional bool
	// Services 列出启用可选组件的 L4 服务名。
	Services []string
	// Components 描述当前组件继续组合的子组件。
	Components []ComponentSpec
}

// CMod 是由 Ant 管理生命周期的 L3 能力门面。
type CMod interface {
	// Spec 返回静态身份、依赖和组件拓扑。
	Spec() CModSpec
	// Start 在所有前置 CMod 就绪后启动该能力。
	Start(CModContext) error
}

// CModValidator 在任何外部资源创建前校验配置。
type CModValidator interface {
	// Validate 在任何 CMod 创建外部资源前校验配置与依赖。
	Validate(CModContext) error
}

// CModDrainer 在根生命周期有效时停止入口或冲刷状态。
type CModDrainer interface {
	// Drain 在依赖仍可用时封闭入口或冲刷已接纳状态。
	Drain(context.Context) error
}

// CModStopper 释放 CMod 持有的物理资源，Stop 必须幂等。
// 实际未完成时返回包含 component.ErrStopDeferred 的错误以保留依赖重试；其他错误作为最终关闭结果。
type CModStopper interface {
	// Stop 幂等释放 CMod 自身持有的物理资源。
	Stop(context.Context) error
}

// CModContext 是自定义 CMod 在构造边界读取已声明前置能力的视图。
type CModContext interface {
	Context
}

func toFrameworkCModID(id CModID) framework.CModID { return framework.CModID(id) }

func toFrameworkCModIDs(ids []CModID) []framework.CModID {
	result := make([]framework.CModID, len(ids))
	for i, id := range ids {
		result[i] = toFrameworkCModID(id)
	}
	return result
}

func toFrameworkComponentSpecs(values []ComponentSpec) []framework.ComponentSpec {
	result := make([]framework.ComponentSpec, len(values))
	for i, value := range values {
		result[i] = framework.ComponentSpec{
			Role:       value.Role,
			Name:       value.Name,
			Optional:   value.Optional,
			Services:   append([]string(nil), value.Services...),
			Components: toFrameworkComponentSpecs(value.Components),
		}
	}
	return result
}
