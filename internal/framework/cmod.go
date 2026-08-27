package framework

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/wal"
)

// CModID 是能力模组的稳定身份，用于依赖解析和启动日志。
type CModID string

type cmodSelection struct {
	ids     map[CModID]struct{}
	ordered []CModID
}

func newCModSelection(ids ...CModID) cmodSelection {
	selection := cmodSelection{
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
func (selection cmodSelection) Has(id CModID) bool {
	_, ok := selection.ids[id]
	return ok
}

// IDs 按稳定拓扑顺序返回能力模组 ID 的副本。
func (selection cmodSelection) IDs() []CModID {
	return append([]CModID(nil), selection.ordered...)
}

// AssemblySelection 是启动前已经解析完成的 L4 服务与 L3 CMod 选择。
// 配置提供者通过它惰性转换本轮实际需要的组件参数。
type AssemblySelection struct {
	cmods    cmodSelection
	services ServiceSelection
}

func newAssemblySelection(cmods cmodSelection, services ServiceSelection) AssemblySelection {
	return AssemblySelection{cmods: cmods, services: services}
}

// NewAssemblySelection 创建一份独立的启动选择，主要供项目配置转换器和测试使用。
func NewAssemblySelection(cmodIDs []CModID, serviceNames []string) AssemblySelection {
	services := ServiceSelection{names: make(map[string]struct{}, len(serviceNames))}
	for _, name := range serviceNames {
		name = strings.TrimSpace(name)
		if name != "" {
			services.names[name] = struct{}{}
		}
	}
	return newAssemblySelection(newCModSelection(cmodIDs...), services)
}

// HasCMod 判断指定 L3 能力门面是否进入本轮启动闭包。
func (selection AssemblySelection) HasCMod(id CModID) bool {
	return selection.cmods.Has(id)
}

// HasService 判断指定 L4 服务是否进入本轮启动集合。
func (selection AssemblySelection) HasService(name string) bool {
	return selection.services.Has(name)
}

// CModIDs 返回稳定拓扑顺序的 CMod ID 副本。
func (selection AssemblySelection) CModIDs() []CModID {
	if selection.cmods.ids == nil {
		return nil
	}
	return selection.cmods.IDs()
}

// ServiceNames 返回稳定字典序的服务名。
func (selection AssemblySelection) ServiceNames() []string {
	if selection.services.names == nil {
		return nil
	}
	return selection.services.Names()
}

// CModConfigProvider 在依赖图确定后，将项目配置惰性转换为框架强类型配置。
// 提供者只能读取配置，不得创建连接、监听端口或启动 goroutine。
type CModConfigProvider func(AssemblySelection) (Config, error)

// CModSpec 声明能力模组的身份、必选依赖和可选前置顺序。
// Requires 会把依赖加入选择闭包；After 只在目标已经进入本次选择闭包时建立启动顺序。
type CModSpec struct {
	// ID 是参与依赖解析、去重和启动日志的稳定能力标识。
	ID CModID
	// Requires 是必须进入本轮启动闭包并先于当前 CMod 启动的依赖。
	Requires []CModID
	// After 只在目标已被其他服务选中时增加启动顺序，不会隐式启用目标 CMod。
	After []CModID
	// Components 描述该 L3 能力门面内部的强类型组件拓扑。
	// 它只用于校验、日志和审计，不参与全局 CMod 依赖解析。
	Components []ComponentSpec
}

// ComponentSpec 描述 CMod Builder 选择的一个内部组件。
// 组件没有全局身份，也不单独参与 Server 生命周期。
type ComponentSpec struct {
	// Role 是组件在父级 Builder 中的稳定职责名称，同级组件不可重复。
	Role string
	// Name 是当前职责采用的具体实现名称，用于启动日志和架构审计。
	Name string
	// Optional 表示组件只在 Services 命中的 L4 服务被选择时生效。
	Optional bool
	// Services 列出可选组件的启用服务；非可选组件必须保持为空。
	Services []string
	// Components 描述当前组件内部继续组合的子组件拓扑。
	Components []ComponentSpec
}

func validateComponentSpecs(parent string, components []ComponentSpec) error {
	roles := make(map[string]struct{}, len(components))
	for _, component := range components {
		role := strings.TrimSpace(component.Role)
		name := strings.TrimSpace(component.Name)
		if role == "" || name == "" {
			return fmt.Errorf("framework: %s component role and name are required", parent)
		}
		if role != component.Role || name != component.Name {
			return fmt.Errorf("framework: %s component %q role and name must not contain surrounding whitespace", parent, role)
		}
		if _, exists := roles[role]; exists {
			return fmt.Errorf("framework: %s contains duplicate component role %q", parent, role)
		}
		roles[role] = struct{}{}
		if component.Optional && len(component.Services) == 0 {
			return fmt.Errorf("framework: %s.%s optional component services are required", parent, role)
		}
		if !component.Optional && len(component.Services) > 0 {
			return fmt.Errorf("framework: %s.%s non-optional component must not declare services", parent, role)
		}
		services := make(map[string]struct{}, len(component.Services))
		for _, service := range component.Services {
			trimmed := strings.TrimSpace(service)
			if trimmed == "" || trimmed != service {
				return fmt.Errorf("framework: %s.%s component service %q is invalid", parent, role, service)
			}
			if _, exists := services[service]; exists {
				return fmt.Errorf("framework: %s.%s component contains duplicate service %q", parent, role, service)
			}
			services[service] = struct{}{}
		}
		if err := validateComponentSpecs(parent+"."+role, component.Components); err != nil {
			return err
		}
	}
	return nil
}

// CMod 是由 Server 管理生命周期和依赖顺序的能力模组。
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
	StorageFor(meta.StorageMode) ModeStorage
	WAL() wal.Store
	RPC() txrpc.Registrar
	Directory() discovery.Directory
	Actor() txactor.Messenger
	Transaction() txn.Executor
	BindTransactionCommitParticipant(txn.CommitParticipant) error
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
