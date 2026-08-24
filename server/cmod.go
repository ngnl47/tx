package server

import (
	"context"
	"slices"

	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/wal"
)

// CModID 是能力模组的稳定身份，用于依赖解析和启动日志。
type CModID string

// CModSelection 是已经解析完成的能力模组依赖闭包。
// 它用于启动前的惰性配置转换，不允许调用方修改内部顺序。
type CModSelection struct {
	ids     map[CModID]struct{}
	ordered []CModID
}

// NewCModSelection 创建一份独立的能力模组选择，主要供配置提供者和测试使用。
func NewCModSelection(ids ...CModID) CModSelection {
	selection := CModSelection{
		ids:     make(map[CModID]struct{}, len(ids)),
		ordered: make([]CModID, 0, len(ids)),
	}
	for _, id := range ids {
		if _, exists := selection.ids[id]; exists {
			continue
		}
		selection.ids[id] = struct{}{}
		selection.ordered = append(selection.ordered, id)
	}
	return selection
}

// Has 判断能力模组是否属于本次启动的完整依赖闭包。
func (selection CModSelection) Has(id CModID) bool {
	_, ok := selection.ids[id]
	return ok
}

// IDs 按稳定拓扑顺序返回能力模组 ID 的副本。
func (selection CModSelection) IDs() []CModID {
	return append([]CModID(nil), selection.ordered...)
}

// CModConfigProvider 在依赖图确定后，将项目配置惰性转换为框架强类型配置。
// 提供者只能读取配置，不得创建连接、监听端口或启动 goroutine。
type CModConfigProvider func(CModSelection) (Config, error)

// CModSpec 声明能力模组的身份和直接依赖。
type CModSpec struct {
	ID       CModID
	Requires []CModID
}

// CMod 是组合 L0-L2 能力并由 Server 管理生命周期的 L3 单元。
type CMod interface {
	Spec() CModSpec
	Start(CModContext) error
}

// CModValidator 在任何能力模组创建外部资源前校验自身配置。
type CModValidator interface {
	Validate(CModContext) error
}

// CModDrainer 在根生命周期仍有效时停止入口或冲刷状态。
type CModDrainer interface {
	Drain(context.Context) error
}

// CModStopper 释放能力模组持有的物理资源。
// Stop 必须幂等，并能清理由失败的 Start 留下的部分资源。
type CModStopper interface {
	Stop(context.Context) error
}

// CModContext 向能力模组提供只读运行时能力，不暴露 Server 所有权。
// Validate 阶段只能读取身份和配置；Start 阶段可以使用依赖图中已经启动的前置能力。
type CModContext interface {
	AppID() string
	Instance() discovery.Instance
	Tasks() lifecycle.Tasks
	Storage() Storage
	WAL() wal.Store
	RPC() txrpc.Registrar
	Directory() discovery.Directory
	Actor() txactor.Messenger
	Transaction() txn.Executor
	HasCMod(CModID) bool
	Advanced() Advanced
}

// ServiceSelection 是当前进程已经选中的 L4 服务只读集合。
type ServiceSelection struct {
	names map[string]struct{}
}

func newServiceSelection(services []Service) ServiceSelection {
	names := make(map[string]struct{}, len(services))
	for _, service := range services {
		if service != nil {
			names[service.Spec().Name] = struct{}{}
		}
	}
	return ServiceSelection{names: names}
}

// Has 判断指定服务是否在本轮启动选择中。
func (selection ServiceSelection) Has(name string) bool {
	_, ok := selection.names[name]
	return ok
}

// Names 按稳定字典序返回本轮选中的服务名。
func (selection ServiceSelection) Names() []string {
	names := make([]string, 0, len(selection.names))
	for name := range selection.names {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// CModRequirementsProvider 为合法部署形态补充条件依赖。
// 实现只能读取配置和服务选择，不得创建资源或启动 goroutine。
type CModRequirementsProvider interface {
	RequiredCMods(ServiceSelection) ([]CModID, error)
}
