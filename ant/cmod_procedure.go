package ant

import (
	"context"
	"errors"
	"fmt"

	framework "github.com/ngnl47/tx/internal/framework"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/txn"
	writebackmodule "github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/wal"
)

// CModProcedureContext 是 WriteBack 和 WAL 组件可见的窄运行时边界。
// 组件不能取得 Ant Server、依赖图或其他 CMod 的生命周期所有权。
type CModProcedureContext interface {
	// Tasks 返回 CModProcedure 子生命周期的后台任务入口。
	Tasks() lifecycle.Tasks
	// Storage 返回框架管理的文档读写能力。
	Storage() Storage
	// WriteAdmission 返回按事务目标存储模式检查健康状态的写入闸门。
	WriteAdmission() writebackmodule.WriteAdmission
	// WAL 返回当前 L4 服务实际启用的预写日志；未启用时返回 nil。
	WAL() wal.Store
	// Executor 返回 CModProcedure 独占装配的单机事务执行器。
	Executor() txn.Executor
	// BindCommitParticipant 绑定唯一的事务提交参与者。
	BindCommitParticipant(txn.CommitParticipant) error
	// WALEnabled 判断当前 L4 服务是否启用了 Builder 中的可选 WAL 组件。
	WALEnabled() bool
}

// CModProcedureWriteBack 接收成功事务的脏根对象，并管理延迟写回生命周期。
type CModProcedureWriteBack interface {
	ComponentSpec() ComponentSpec
	Validate(CModProcedureContext) error
	Start(CModProcedureContext) error
	Drain(context.Context) error
	Stop(context.Context) error
}

// CModProcedureWAL 是写回管线可选使用的预写日志组件。
type CModProcedureWAL interface {
	ComponentSpec() ComponentSpec
	Validate(CModProcedureContext) error
	Start(CModProcedureContext) (wal.Store, error)
	Stop(context.Context) error
}

// CModProcedureBuilder 以强类型组件组装 Ant 单机事务能力。
type CModProcedureBuilder struct {
	writeBack CModProcedureWriteBack
	wal       CModProcedureWAL
	walSet    bool
}

// NewCModProcedureBuilder 创建 Procedure CMod Builder。
func NewCModProcedureBuilder() *CModProcedureBuilder {
	return &CModProcedureBuilder{}
}

// WriteBack 设置事务提交后负责登记和刷盘根对象的写回组件。
func (builder *CModProcedureBuilder) WriteBack(component CModProcedureWriteBack) *CModProcedureBuilder {
	if builder != nil {
		builder.writeBack = component
	}
	return builder
}

// WAL 设置写回管线可选使用的预写日志组件。
func (builder *CModProcedureBuilder) WAL(component CModProcedureWAL) *CModProcedureBuilder {
	if builder != nil {
		builder.wal = component
		builder.walSet = true
	}
	return builder
}

// Build 创建由 Ant 管理生命周期的 CModProcedure。
func (builder *CModProcedureBuilder) Build() (CMod, error) {
	if builder == nil {
		return nil, errors.New("ant: CModProcedure builder is nil")
	}
	if isNil(builder.writeBack) {
		return nil, errors.New("ant: CModProcedure writeback component is required")
	}
	if builder.walSet && isNil(builder.wal) {
		return nil, errors.New("ant: CModProcedure WAL component is required")
	}

	frameworkBuilder := framework.NewProcedureBuilder().WriteBack(procedureWriteBackAdapter{component: builder.writeBack})
	if builder.walSet {
		frameworkBuilder.WAL(procedureWALAdapter{component: builder.wal})
	}
	kernel, err := frameworkBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("ant: build CModProcedure: %w", err)
	}
	return &procedureCMod{kernel: kernel}, nil
}

// NewCModProcedureMongoWAL 创建仅对指定 L4 服务启用的 MongoDB WAL 组件。
func NewCModProcedureMongoWAL(services ...string) CModProcedureWAL {
	return &mongoProcedureWAL{kernel: framework.NewProcedureMongoWAL(services...)}
}

type procedureCMod struct {
	kernel framework.CMod
}

var (
	_ CMod          = (*procedureCMod)(nil)
	_ CModValidator = (*procedureCMod)(nil)
	_ CModDrainer   = (*procedureCMod)(nil)
	_ CModStopper   = (*procedureCMod)(nil)
)

func (mod *procedureCMod) Spec() CModSpec {
	if mod == nil || mod.kernel == nil {
		return CModSpec{ID: CModProcedure}
	}
	kernel := mod.kernel.Spec()
	return CModSpec{
		ID:         CModProcedure,
		Requires:   fromFrameworkCModIDs(kernel.Requires),
		After:      fromFrameworkCModIDs(kernel.After),
		Components: fromFrameworkComponentSpecs(kernel.Components),
	}
}

func (mod *procedureCMod) Validate(runtime CModContext) error {
	validator, ok := mod.kernel.(framework.CModValidator)
	if !ok {
		return nil
	}
	kernel, err := unwrapFrameworkCModContext(runtime)
	if err != nil {
		return err
	}
	return validator.Validate(kernel)
}

func (mod *procedureCMod) Start(runtime CModContext) error {
	kernel, err := unwrapFrameworkCModContext(runtime)
	if err != nil {
		return err
	}
	return mod.kernel.Start(kernel)
}

func (mod *procedureCMod) Drain(ctx context.Context) error {
	if drainer, ok := mod.kernel.(framework.CModDrainer); ok {
		return drainer.Drain(ctx)
	}
	return nil
}

func (mod *procedureCMod) Stop(ctx context.Context) error {
	if stopper, ok := mod.kernel.(framework.CModStopper); ok {
		return stopper.Stop(ctx)
	}
	return nil
}

type procedureWriteBackAdapter struct {
	component CModProcedureWriteBack
}

func (adapter procedureWriteBackAdapter) ComponentSpec() framework.ComponentSpec {
	return toFrameworkComponentSpec(adapter.component.ComponentSpec())
}

func (adapter procedureWriteBackAdapter) Validate(runtime framework.ProcedureContext) error {
	return adapter.component.Validate(procedureContextView{kernel: runtime})
}

func (adapter procedureWriteBackAdapter) Start(runtime framework.ProcedureContext) error {
	return adapter.component.Start(procedureContextView{kernel: runtime})
}

func (adapter procedureWriteBackAdapter) Drain(ctx context.Context) error {
	return adapter.component.Drain(ctx)
}

func (adapter procedureWriteBackAdapter) Stop(ctx context.Context) error {
	return adapter.component.Stop(ctx)
}

type procedureWALAdapter struct {
	component CModProcedureWAL
}

func (adapter procedureWALAdapter) ComponentSpec() framework.ComponentSpec {
	return toFrameworkComponentSpec(adapter.component.ComponentSpec())
}

func (adapter procedureWALAdapter) Validate(runtime framework.ProcedureContext) error {
	return adapter.component.Validate(procedureContextView{kernel: runtime})
}

func (adapter procedureWALAdapter) Start(runtime framework.ProcedureContext) (wal.Store, error) {
	return adapter.component.Start(procedureContextView{kernel: runtime})
}

func (adapter procedureWALAdapter) Stop(ctx context.Context) error {
	return adapter.component.Stop(ctx)
}

type procedureContextView struct {
	kernel framework.ProcedureContext
}

func (view procedureContextView) Tasks() lifecycle.Tasks { return view.kernel.Tasks() }
func (view procedureContextView) Storage() Storage       { return view.kernel.Storage() }
func (view procedureContextView) WriteAdmission() writebackmodule.WriteAdmission {
	return view.kernel.WriteAdmission()
}
func (view procedureContextView) WAL() wal.Store         { return view.kernel.WAL() }
func (view procedureContextView) Executor() txn.Executor { return view.kernel.Transaction() }
func (view procedureContextView) WALEnabled() bool       { return view.kernel.WALEnabled() }
func (view procedureContextView) BindCommitParticipant(participant txn.CommitParticipant) error {
	return view.kernel.BindTransactionCommitParticipant(participant)
}

type mongoProcedureWAL struct {
	kernel framework.ProcedureWAL
}

func (component *mongoProcedureWAL) ComponentSpec() ComponentSpec {
	if component == nil || component.kernel == nil {
		return ComponentSpec{}
	}
	return fromFrameworkComponentSpec(component.kernel.ComponentSpec())
}

func (component *mongoProcedureWAL) Validate(runtime CModProcedureContext) error {
	kernel, err := unwrapFrameworkProcedureContext(runtime)
	if err != nil {
		return err
	}
	return component.kernel.Validate(kernel)
}

func (component *mongoProcedureWAL) Start(runtime CModProcedureContext) (wal.Store, error) {
	kernel, err := unwrapFrameworkProcedureContext(runtime)
	if err != nil {
		return nil, err
	}
	return component.kernel.Start(kernel)
}

func (component *mongoProcedureWAL) Stop(ctx context.Context) error {
	return component.kernel.Stop(ctx)
}

func unwrapFrameworkCModContext(runtime CModContext) (framework.CModContext, error) {
	view, ok := runtime.(*contextView)
	if !ok || view == nil {
		return nil, errors.New("ant: CModProcedure context is unavailable")
	}
	kernel, ok := view.kernel.(framework.CModContext)
	if !ok || kernel == nil {
		return nil, errors.New("ant: kernel CMod context is unavailable")
	}
	return kernel, nil
}

func unwrapFrameworkProcedureContext(runtime CModProcedureContext) (framework.ProcedureContext, error) {
	view, ok := runtime.(procedureContextView)
	if !ok || view.kernel == nil {
		return nil, errors.New("ant: CModProcedure component context is unavailable")
	}
	return view.kernel, nil
}

func fromFrameworkCModIDs(ids []framework.CModID) []CModID {
	result := make([]CModID, len(ids))
	for index, id := range ids {
		result[index] = CModID(id)
	}
	return result
}

func toFrameworkComponentSpec(value ComponentSpec) framework.ComponentSpec {
	return framework.ComponentSpec{
		Role:       value.Role,
		Name:       value.Name,
		Optional:   value.Optional,
		Services:   append([]string(nil), value.Services...),
		Components: toFrameworkComponentSpecs(value.Components),
	}
}

func fromFrameworkComponentSpecs(values []framework.ComponentSpec) []ComponentSpec {
	result := make([]ComponentSpec, len(values))
	for index, value := range values {
		result[index] = fromFrameworkComponentSpec(value)
	}
	return result
}

func fromFrameworkComponentSpec(value framework.ComponentSpec) ComponentSpec {
	return ComponentSpec{
		Role:       value.Role,
		Name:       value.Name,
		Optional:   value.Optional,
		Services:   append([]string(nil), value.Services...),
		Components: fromFrameworkComponentSpecs(value.Components),
	}
}
