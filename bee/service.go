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
	// Requires 是服务构造前必须就绪的直接 CMod 依赖。
	Requires []CModID
	// ActorKinds 列出由本服务承载的 Actor 种类。
	ActorKinds []txactor.Kind
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
	// Start 在所有声明的 CMod 就绪后启动服务。
	Start(context.Context) error
}

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
	return framework.Spec{
		Name:       adapter.registration.Spec.Name,
		Requires:   toFrameworkCModIDs(adapter.registration.Spec.Requires),
		ActorKinds: append([]txactor.Kind(nil), adapter.registration.Spec.ActorKinds...),
	}
}

func (adapter *serviceAdapter) Start(runtime framework.Context) (err error) {
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
	return instance.Start(ctx)
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
