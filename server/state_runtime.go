package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/ngnl47/tx/lifecycle"
	cachemodule "github.com/ngnl47/tx/modules/cache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/wal"
)

// CModStateRuntime 是有状态服务的 L3 运行时门面。
const CModStateRuntime CModID = "state-runtime"

// StateRuntimeContext 是缓存组件在 StateRuntime 内可见的窄上下文。
// 组件不能取得 CMod 身份、依赖图或 Server 生命周期所有权。
type StateRuntimeContext interface {
	// Tasks 返回 StateRuntime 子生命周期的后台任务入口。
	Tasks() lifecycle.Tasks
	// Storage 返回框架管理的文档读写能力。
	Storage() Storage
	// WriteAdmission 返回按本次事务目标存储模式检查健康状态的写入闸门。
	WriteAdmission() cachemodule.WriteAdmission
	// WAL 返回当前 L4 服务实际启用的预写日志；未启用时返回 nil。
	WAL() wal.Store
	// Transaction 返回 StateRuntime 独占装配的单机事务执行器。
	Transaction() txn.Executor
	// BindTransactionCommitParticipant 绑定唯一的事务提交参与者。
	BindTransactionCommitParticipant(txn.CommitParticipant) error
	// WALEnabled 判断当前 L4 服务是否启用了 Builder 中的可选 WAL 组件。
	WALEnabled() bool
}

// StateCache 是 StateRuntime 使用的缓存运行时组件。
// 它由 StateRuntime 统一管理校验、启动、排水和停止顺序。
type StateCache interface {
	// ComponentSpec 返回缓存组件的职责、实现和可选子组件拓扑。
	ComponentSpec() ComponentSpec
	// Validate 在创建资源前校验组件配置和依赖。
	Validate(StateRuntimeContext) error
	// Start 绑定事务提交边界并启动缓存运行时。
	Start(StateRuntimeContext) error
	// Drain 在依赖仍可用时刷空已接纳的待持久化对象。
	Drain(context.Context) error
	// Stop 释放组件自身资源；实现必须幂等。
	Stop(context.Context) error
}

// StateWAL 是 StateRuntime Cache 管线使用的可选预写日志组件。
// 组件创建安全 wal.Store，但不能安装全局能力或接管 Server 生命周期。
type StateWAL interface {
	// ComponentSpec 返回 WAL 组件的职责、实现和适用 L4 服务。
	ComponentSpec() ComponentSpec
	// Validate 在创建连接或集合前校验 WAL 配置和依赖。
	Validate(StateRuntimeContext) error
	// Start 创建由 StateRuntime 托管的 WAL Store。
	Start(StateRuntimeContext) (wal.Store, error)
	// Stop 释放 WAL 组件自身资源；实现必须幂等。
	Stop(context.Context) error
}

type mongoStateWAL struct {
	services []string
}

// MongoWALForServices 创建按 L4 服务选择启用的 Mongo WAL 组件。
func MongoWALForServices(names ...string) StateWAL {
	services := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		services = append(services, name)
	}
	slices.Sort(services)
	return &mongoStateWAL{services: services}
}

func (component *mongoStateWAL) ComponentSpec() ComponentSpec {
	if component == nil {
		return ComponentSpec{}
	}
	return ComponentSpec{
		Role:     "wal",
		Name:     "mongo",
		Optional: true,
		Services: append([]string(nil), component.services...),
	}
}

func (*mongoStateWAL) Validate(runtime StateRuntimeContext) error {
	host, err := stateRuntimeHostFrom(runtime)
	if err != nil {
		return err
	}
	if !host.HasCMod(CModMongoStorage) {
		return errors.New("server: Mongo WAL requires Mongo storage")
	}
	return host.validateMongoStateWAL()
}

func (*mongoStateWAL) Start(runtime StateRuntimeContext) (wal.Store, error) {
	host, err := stateRuntimeHostFrom(runtime)
	if err != nil {
		return nil, err
	}
	return host.buildMongoStateWAL()
}

func (*mongoStateWAL) Stop(context.Context) error { return nil }

// StateRuntimeBuilder 以强类型组件组装有状态运行时 CMod。
type StateRuntimeBuilder struct {
	cache  StateCache
	wal    StateWAL
	walSet bool
}

// NewStateRuntimeBuilder 创建有状态运行时 Builder。
func NewStateRuntimeBuilder() *StateRuntimeBuilder {
	return &StateRuntimeBuilder{}
}

// Cache 设置事务提交后负责登记和刷盘根对象的缓存组件。
func (builder *StateRuntimeBuilder) Cache(cache StateCache) *StateRuntimeBuilder {
	if builder != nil {
		builder.cache = cache
	}
	return builder
}

// WAL 设置可选的预写日志组件。
func (builder *StateRuntimeBuilder) WAL(component StateWAL) *StateRuntimeBuilder {
	if builder != nil {
		builder.wal = component
		builder.walSet = true
	}
	return builder
}

// Build 创建由 Server 管理生命周期的 StateRuntime CMod 实例。
func (builder *StateRuntimeBuilder) Build() (CMod, error) {
	if builder == nil {
		return nil, errors.New("server: state runtime builder is nil")
	}
	if isNilInterface(builder.cache) {
		return nil, errors.New("server: state runtime cache component is required")
	}
	if builder.walSet && isNilInterface(builder.wal) {
		return nil, errors.New("server: state runtime WAL component is required")
	}
	cacheSpec := cloneComponentSpec(builder.cache.ComponentSpec())
	if builder.wal != nil {
		walSpec := cloneComponentSpec(builder.wal.ComponentSpec())
		cacheSpec.Components = append(cacheSpec.Components, walSpec)
	}
	components := []ComponentSpec{
		{Role: "transaction", Name: "default"},
		cacheSpec,
	}
	if err := validateComponentSpecs("state-runtime", components); err != nil {
		return nil, err
	}
	return &stateRuntimeCMod{
		cache:      builder.cache,
		wal:        builder.wal,
		components: components,
	}, nil
}

type stateRuntimeCMod struct {
	cache        StateCache
	wal          StateWAL
	components   []ComponentSpec
	host         stateRuntimeHost
	walStarted   bool
	walInstalled bool
}

var (
	_ CMod          = (*stateRuntimeCMod)(nil)
	_ CModValidator = (*stateRuntimeCMod)(nil)
	_ CModDrainer   = (*stateRuntimeCMod)(nil)
	_ CModStopper   = (*stateRuntimeCMod)(nil)
)

func (mod *stateRuntimeCMod) Spec() CModSpec {
	return CModSpec{
		ID:         CModStateRuntime,
		After:      []CModID{CModMongoStorage, CModFusionStorage},
		Components: cloneComponentSpecs(mod.components),
	}
}

func (mod *stateRuntimeCMod) Validate(runtime CModContext) error {
	host, ok := runtime.(stateRuntimeHost)
	if !ok {
		return errors.New("server: state runtime host is not available")
	}
	walEnabled := mod.walEnabled(host.stateServices())
	if walEnabled {
		if err := mod.wal.Validate(stateRuntimeContext{CModContext: runtime, walEnabled: true}); err != nil {
			return err
		}
	}
	if err := mod.cache.Validate(stateRuntimeContext{CModContext: runtime, walEnabled: walEnabled}); err != nil {
		return fmt.Errorf("server: validate state cache: %w", err)
	}
	return nil
}

func (mod *stateRuntimeCMod) Start(runtime CModContext) error {
	host, ok := runtime.(stateRuntimeHost)
	if !ok {
		return errors.New("server: state runtime host is not available")
	}
	walEnabled := mod.walEnabled(host.stateServices())
	if walEnabled {
		// Start 返回错误时 CMod Host 仍会调用 Stop，因此先登记组件所有权。
		mod.host = host
		mod.walStarted = true
		store, err := mod.wal.Start(stateRuntimeContext{CModContext: runtime, walEnabled: true})
		if err != nil {
			return err
		}
		if isNilInterface(store) {
			return errors.New("server: state runtime WAL component returned nil Store")
		}
		if err := host.installStateWAL(store); err != nil {
			return err
		}
		mod.walInstalled = true
	}
	if err := host.startStateTransaction(); err != nil {
		return err
	}
	if err := mod.cache.Start(stateRuntimeContext{CModContext: runtime, walEnabled: walEnabled}); err != nil {
		return fmt.Errorf("server: start state cache: %w", err)
	}
	return nil
}

func (mod *stateRuntimeCMod) Drain(ctx context.Context) error {
	if mod == nil || mod.cache == nil {
		return nil
	}
	return mod.cache.Drain(ctx)
}

func (mod *stateRuntimeCMod) Stop(ctx context.Context) error {
	if mod == nil {
		return nil
	}
	var errs []error
	if mod.cache != nil {
		errs = append(errs, mod.cache.Stop(ctx))
	}
	if mod.walStarted {
		if mod.walInstalled && mod.host != nil {
			mod.host.clearStateWAL()
		}
		errs = append(errs, mod.wal.Stop(ctx))
		mod.walStarted = false
		mod.walInstalled = false
		mod.host = nil
	}
	return errors.Join(errs...)
}

func (mod *stateRuntimeCMod) walEnabled(selection ServiceSelection) bool {
	return mod != nil && mod.wal != nil && componentEnabled(mod.wal.ComponentSpec(), selection)
}

type stateRuntimeContext struct {
	CModContext
	walEnabled bool
}

// WriteAdmission 返回按事务目标存储模式检查健康状态的 StateRuntime 专用闸门。
func (runtime stateRuntimeContext) WriteAdmission() cachemodule.WriteAdmission {
	context, ok := runtime.CModContext.(*cmodContext)
	if !ok || context == nil || context.runtime == nil || context.runtime.storage == nil {
		return nil
	}
	return storageWriteAdmission{storage: context.runtime.storage}
}

func (runtime stateRuntimeContext) WALEnabled() bool { return runtime.walEnabled }

type stateRuntimeHost interface {
	CModContext
	stateServices() ServiceSelection
	validateMongoStateWAL() error
	buildMongoStateWAL() (wal.Store, error)
	installStateWAL(wal.Store) error
	clearStateWAL()
	startStateTransaction() error
}

func stateRuntimeHostFrom(runtime StateRuntimeContext) (stateRuntimeHost, error) {
	context, ok := runtime.(stateRuntimeContext)
	if !ok {
		return nil, errors.New("server: state runtime context is not available")
	}
	host, ok := context.CModContext.(stateRuntimeHost)
	if !ok {
		return nil, errors.New("server: state runtime host is not available")
	}
	return host, nil
}

func (context *cmodContext) stateServices() ServiceSelection {
	if context == nil || context.runtime == nil {
		return ServiceSelection{}
	}
	services := context.runtime.instanceSnapshot().Services
	selection := ServiceSelection{names: make(map[string]struct{}, len(services))}
	for _, service := range services {
		selection.names[service] = struct{}{}
	}
	return selection
}

func (context *cmodContext) validateMongoStateWAL() error {
	if context == nil || context.server == nil {
		return errors.New("server: state runtime server is not configured")
	}
	return context.server.config.Storage.WAL.validate(context.server.config.Storage.MongoDB)
}

func (context *cmodContext) buildMongoStateWAL() (wal.Store, error) {
	if context == nil || context.server == nil || context.server.runtime == nil || context.server.runtime.storage == nil {
		return nil, errors.New("server: storage runtime is not configured")
	}
	return context.server.runtime.storage.buildWAL(
		context.server.runtime.lifecycle,
		context.server.config.Storage.WAL,
		context.server.config.Storage.MongoDB,
	)
}

func (context *cmodContext) installStateWAL(store wal.Store) error {
	if context == nil || context.runtime == nil {
		return errors.New("server: state runtime is not configured")
	}
	if isNilInterface(store) {
		return errors.New("server: WAL Store is nil")
	}
	if context.runtime.wal != nil {
		return errors.New("server: WAL Store is already installed")
	}
	context.runtime.wal = store
	return nil
}

func (context *cmodContext) clearStateWAL() {
	if context != nil && context.runtime != nil {
		context.runtime.wal = nil
	}
}

func (context *cmodContext) startStateTransaction() error {
	if context == nil || context.server == nil {
		return errors.New("server: state runtime server is not configured")
	}
	return context.server.startTransaction()
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
