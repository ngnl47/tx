package framework

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/txn"
	writebackmodule "github.com/ngnl47/tx/modules/writeback"
	storageruntime "github.com/ngnl47/tx/storage/runtime"
)

// CModProcedure 是有状态服务的 L3 运行时门面。
const CModProcedure CModID = "procedure"

// ProcedureContext 是写回组件在 Procedure 内可见的窄上下文。
// 组件不能取得 CMod 身份、依赖图或 Server 生命周期所有权。
type ProcedureContext interface {
	// Tasks 返回 Procedure 子生命周期的后台任务入口。
	Tasks() lifecycle.Tasks
	// Storage 返回框架管理的文档读写能力。
	Storage() Storage
	// WriteAdmission 返回按本次事务目标存储模式检查健康状态的写入闸门。
	WriteAdmission() writebackmodule.WriteAdmission
	// Transaction 返回 Procedure 独占装配的单机事务执行器。
	Transaction() txn.Executor
	// BindTransactionCommitParticipant 绑定唯一的事务提交参与者。
	BindTransactionCommitParticipant(txn.CommitParticipant) error
}

// ProcedureWriteBack 是 Procedure 使用的写回运行时组件。
// 它由 Procedure 统一管理校验、启动、排水和停止顺序。
type ProcedureWriteBack interface {
	// ComponentSpec 返回写回组件的职责、实现和可选子组件拓扑。
	ComponentSpec() ComponentSpec
	// Validate 在创建资源前校验组件配置和依赖。
	Validate(ProcedureContext) error
	// Start 绑定事务提交边界并启动写回运行时。
	Start(ProcedureContext) error
	// Drain 在依赖仍可用时刷空已接纳的待持久化对象。
	Drain(context.Context) error
	// Stop 释放组件自身资源；实现必须幂等。
	Stop(context.Context) error
}

// ProcedureBuilder 以强类型组件组装有状态运行时 CMod。
type ProcedureBuilder struct {
	writeBack ProcedureWriteBack
}

// NewProcedureBuilder 创建有状态运行时 Builder。
func NewProcedureBuilder() *ProcedureBuilder {
	return &ProcedureBuilder{}
}

// WriteBack 设置事务提交后负责登记和刷盘根对象的写回组件。
func (builder *ProcedureBuilder) WriteBack(component ProcedureWriteBack) *ProcedureBuilder {
	if builder != nil {
		builder.writeBack = component
	}
	return builder
}

// Build 创建由 Server 管理生命周期的 Procedure CMod 实例。
func (builder *ProcedureBuilder) Build() (CMod, error) {
	if builder == nil {
		return nil, errors.New("framework: procedure builder is nil")
	}
	if isNilInterface(builder.writeBack) {
		return nil, errors.New("framework: procedure writeback component is required")
	}
	writeBackSpec := cloneComponentSpec(builder.writeBack.ComponentSpec())
	components := []ComponentSpec{
		{Role: "transaction", Name: "default"},
		writeBackSpec,
	}
	if err := validateComponentSpecs("procedure", components); err != nil {
		return nil, err
	}
	return &procedureCMod{
		writeBack:  builder.writeBack,
		components: components,
	}, nil
}

type procedureCMod struct {
	writeBack   ProcedureWriteBack
	components  []ComponentSpec
	transaction *txn.Runtime
}

var (
	_ CMod          = (*procedureCMod)(nil)
	_ CModValidator = (*procedureCMod)(nil)
	_ CModDrainer   = (*procedureCMod)(nil)
	_ CModStopper   = (*procedureCMod)(nil)
)

func (mod *procedureCMod) Spec() CModSpec {
	return CModSpec{
		ID:         CModProcedure,
		After:      []CModID{CModMongoStorage, CModFusionStorage},
		Components: cloneComponentSpecs(mod.components),
	}
}

func (mod *procedureCMod) Validate(runtime CModContext) error {
	_, ok := runtime.(procedureHost)
	if !ok {
		return errors.New("framework: procedure host is not available")
	}
	if err := mod.writeBack.Validate(procedureContext{CModContext: runtime}); err != nil {
		return fmt.Errorf("framework: validate procedure writeback: %w", err)
	}
	return nil
}

func (mod *procedureCMod) Start(runtime CModContext) error {
	host, ok := runtime.(procedureHost)
	if !ok {
		return errors.New("framework: procedure host is not available")
	}
	transaction, err := host.startProcedureTransaction()
	if err != nil {
		return err
	}
	mod.transaction = transaction
	if err := mod.writeBack.Start(procedureContext{CModContext: runtime}); err != nil {
		return fmt.Errorf("framework: start procedure writeback: %w", err)
	}
	return nil
}

func (mod *procedureCMod) Drain(ctx context.Context) error {
	if mod == nil || mod.writeBack == nil {
		return nil
	}
	if err := mod.transaction.Drain(ctx); err != nil {
		return err
	}
	return mod.writeBack.Drain(ctx)
}

func (mod *procedureCMod) Stop(ctx context.Context) error {
	if mod == nil || mod.writeBack == nil {
		return nil
	}
	return mod.writeBack.Stop(ctx)
}

type procedureContext struct{ CModContext }

// WriteAdmission 返回按事务目标存储模式检查健康状态的 Procedure 专用闸门。
func (runtime procedureContext) WriteAdmission() writebackmodule.WriteAdmission {
	context, ok := runtime.CModContext.(*cmodContext)
	if !ok || context == nil || context.runtime == nil || context.runtime.storage == nil {
		return nil
	}
	return storageruntime.NewWriteAdmission(context.runtime.storage)
}

type procedureHost interface {
	CModContext
	startProcedureTransaction() (*txn.Runtime, error)
}

func (context *cmodContext) startProcedureTransaction() (*txn.Runtime, error) {
	if context == nil || context.server == nil {
		return nil, errors.New("framework: procedure server is not configured")
	}
	if err := context.server.startTransaction(); err != nil {
		return nil, err
	}
	return context.server.runtime.transaction, nil
}

func cloneComponentSpecs(components []ComponentSpec) []ComponentSpec {
	result := make([]ComponentSpec, len(components))
	for index, component := range components {
		result[index] = cloneComponentSpec(component)
	}
	return result
}

func cloneComponentSpec(component ComponentSpec) ComponentSpec {
	component.Services = append([]string(nil), component.Services...)
	component.Components = cloneComponentSpecs(component.Components)
	return component
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
