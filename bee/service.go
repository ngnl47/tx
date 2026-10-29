package bee

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	framework "github.com/ngnl47/tx/internal/framework"
	txactor "github.com/ngnl47/tx/modules/actor"
)

// ServiceSpec 声明可选服务的稳定身份和能力依赖。
type ServiceSpec struct {
	// Name 是启动参数和服务发现中使用的稳定 L4 服务名。
	Name string
	// Requires 声明服务直接使用的 CMod，构造前由框架递归启用依赖并按拓扑启动。
	// Context 只允许取得显式声明的能力；间接依赖已启动不代表服务可以直接访问。
	Requires []CModID
	// ActorTypes 列出由本服务承载的多个 Actor 种类；Type 名称在应用全部注册中唯一。
	// 未选中服务只提供规格，不启动其工厂。
	ActorTypes []txactor.Type
	// Placements 声明本服务提供的命名分组策略，远端只读取声明。
	Placements []Placement
}

// ServiceFactory 在依赖 CMod 全部就绪后构造服务实例。
type ServiceFactory func(Context) (Service, error)

// ServiceRegistration 组合服务的静态规格和延迟构造工厂。
type ServiceRegistration struct {
	// Spec 描述服务身份、直接能力依赖和 Actor 种类。
	Spec ServiceSpec
	// Factory 在所有依赖 CMod 就绪后构造服务实例。
	Factory ServiceFactory
}

// Service 是由 Bee 托管的 L4 运行单元。
type Service interface {
	// Init 在 CMod 就绪后装配资源与 Handler，业务入口尚未开放。
	Init(context.Context) error
}

// ServiceStarter 执行通信就绪后的必备启动工作，返回成功后服务进入 Running。
type ServiceStarter interface{ Start(context.Context) error }

// ServiceDrainer 让服务停止接收新的外部工作。
type ServiceDrainer interface {
	// Drain 封闭服务外部入口并等待已接纳工作完成。
	Drain(context.Context) error
}

// ServiceStopper 释放服务持有的资源。
type ServiceStopper interface {
	// Stop 幂等释放服务自身持有的资源。
	Stop(context.Context) error
}

type serviceAdapter struct {
	registration ServiceRegistration
	instance     Service
}

func (adapter *serviceAdapter) Spec() framework.Spec {
	placements := make([]framework.Placement, 0, len(adapter.registration.Spec.Placements))
	for _, declaration := range adapter.registration.Spec.Placements {
		var factory func(framework.Context) (txactor.GroupResolver, error)
		if declaration.Factory != nil {
			factory = func(runtime framework.Context) (resolve txactor.GroupResolver, err error) {
				defer framework.RecoverCapability(&err)
				view := newContextView(runtime, "placement "+declaration.Name, adapter.registration.Spec.Requires)
				return declaration.Factory(view)
			}
		}
		placements = append(placements, framework.Placement{Name: declaration.Name, Factory: factory})
	}
	return framework.Spec{
		Name:       adapter.registration.Spec.Name,
		Placements: placements,
		Requires:   toFrameworkCModIDs(adapter.registration.Spec.Requires),
		ActorTypes: append([]txactor.Type(nil), adapter.registration.Spec.ActorTypes...),
	}
}

func (adapter *serviceAdapter) Init(runtime framework.Context) (err error) {
	defer framework.RecoverCapability(&err)
	view := newContextView(runtime, "service "+fmt.Sprintf("%q", adapter.registration.Spec.Name), adapter.registration.Spec.Requires)
	instance, err := adapter.registration.Factory(view)
	if err != nil {
		return err
	}
	if isNil(instance) {
		return fmt.Errorf("bee: service %q factory returned nil", adapter.registration.Spec.Name)
	}
	adapter.instance = instance
	ctx := context.Background()
	if tasks := runtime.Tasks(); tasks != nil {
		ctx = tasks
	}
	return instance.Init(ctx)
}

func (adapter *serviceAdapter) Start(ctx context.Context) (err error) {
	defer framework.RecoverCapability(&err)
	if starter, ok := adapter.instance.(ServiceStarter); ok {
		return starter.Start(ctx)
	}
	return nil
}

func (adapter *serviceAdapter) Drain(ctx context.Context) error {
	if drainer, ok := adapter.instance.(ServiceDrainer); ok && !isNil(drainer) {
		return drainer.Drain(ctx)
	}
	return nil
}

func (adapter *serviceAdapter) Stop(ctx context.Context) error {
	if stopper, ok := adapter.instance.(ServiceStopper); ok && !isNil(stopper) {
		return stopper.Stop(ctx)
	}
	return nil
}

func validateRegistration(registration ServiceRegistration) error {
	if strings.TrimSpace(registration.Spec.Name) == "" {
		return errors.New("bee: service name is required")
	}
	if registration.Factory == nil {
		return fmt.Errorf("bee: service %q factory is nil", registration.Spec.Name)
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	kind := reflect.ValueOf(value).Kind()
	return (kind == reflect.Chan || kind == reflect.Func || kind == reflect.Interface || kind == reflect.Map || kind == reflect.Pointer || kind == reflect.Slice) && reflect.ValueOf(value).IsNil()
}

// Placement 在依赖就绪后构造只读分组解析器；不承担 Actor 初始化或独立资源管理。
type Placement struct {
	Name    string
	Factory func(Context) (txactor.GroupResolver, error)
}

// ServiceFlusher 在全部 Actor 与后台任务排空后保存服务拥有的数据。
// 失败保留资源并沿用 Stop/交接重试；成功后才进入资源释放。
type ServiceFlusher interface{ Flush(context.Context) error }

func (adapter *serviceAdapter) Flush(ctx context.Context) error {
	if flusher, ok := adapter.instance.(ServiceFlusher); ok && !isNil(flusher) {
		return flusher.Flush(ctx)
	}
	return nil
}
