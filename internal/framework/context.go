package framework

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/sessionid"
	txstorage "github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txredis "github.com/ngnl47/tx/storage/redis"
)

// StorageReaders 是生成 data 包使用的读取契约。
type StorageReaders interface {
	// RedisReader 返回带健康闸门的 Redis 文档读取器；未装配时返回 nil。
	RedisReader() txstorage.DocumentLoader
	// MongoReader 返回带健康闸门的 Mongo 文档读取器；未装配时返回 nil。
	MongoReader() txstorage.DocumentLoader
	// FusionReader 返回同时依赖 Redis 与 Mongo 的 Fusion 读取器；未装配时返回 nil。
	FusionReader() txstorage.DocumentLoader
}

// Storage 是稳定的应用侧存储契约。连接池、健康状态、后台任务和关闭流程仍由 server 管理。
type Storage interface {
	StorageReaders
	// Context 返回由进程根生命周期派生的只读上下文。
	Context() context.Context
	// ChangeWriter 返回按 TypeSpec 存储模式分发的变更写入器。
	ChangeWriter() txstorage.ChangeApplier
}

// ModeStorage 是绑定到单一持久化模式的窄能力视图。
// Reader 和 ChangeWriter 都拒绝跨模式访问，避免 L4 Service 越过 Requires 声明使用其他存储。
type ModeStorage interface {
	txmongo.DocumentFinder
	// Context 返回由进程根生命周期派生的只读上下文。
	Context() context.Context
	// Reader 返回当前模式唯一的文档读取器。
	Reader() txstorage.DocumentLoader
	// ChangeWriter 返回只接受当前模式 TypeSpec 的变更写入器。
	ChangeWriter() txstorage.ChangeApplier
	// BackfillDocumentWithOptions 只允许 Mongo 模式执行仅缺失时写入。
	BackfillDocumentWithOptions(context.Context, meta.TypeSpec, txstorage.RecordKey, map[string]any, txstorage.ApplyOptions) (bool, error)
	// Reserve 只允许 Mongo 模式按路由分片预留调用方指定的计数区间。
	Reserve(context.Context, string, any, string, string, int64) (int64, error)
}

// Advanced 暴露第一版受支持的专家级扩展面。普通业务服务应直接使用 Context 的能力方法。
type Advanced interface {
	// Redis 返回 Redis 脚本路由能力；普通 GD 读写不应使用该入口。
	Redis() txredis.ScriptRoutes
	// Slots 返回有状态服务逻辑槽位门面。读取只访问 Watch 内存目录，创建和迁移
	// 仍通过 etcd CAS 持久化；业务消息热路径不会执行 etcd Get。
	Slots() discovery.SlotStore
}

// Context 是传递给选中服务的稳定能力视图，并隔离资源所有权。
// 它可以执行业务工作，但返回的模块契约均不包含进程启动或停止权限。
type Context interface {
	// AppID 返回当前应用的稳定集群标识。
	AppID() string
	// Instance 返回当前进程已经注册的实例快照。
	Instance() discovery.Instance
	// Tasks 返回当前 owner 独立的后台任务入口；框架会在释放 owner 前取消并等待这些任务。
	Tasks() lifecycle.Tasks
	// Admission 返回当前进程的外部工作准入状态。
	Admission() lifecycle.Admission
	// Storage 返回框架管理的存储能力；服务未声明存储依赖时可能为 nil。
	Storage() Storage
	// StorageFor 返回绑定到单一持久化模式的窄存储能力；未装配时返回 nil。
	StorageFor(meta.StorageMode) ModeStorage
	// RPC 返回共享 gRPC Host 的注册入口；未启用时返回 nil。
	RPC() txrpc.Registrar
	// Directory 返回服务发现维护的只读实例目录；未启用时返回 nil。
	Directory() discovery.Directory
	// Actor 返回 Actor 消息投递入口；未启用时返回 nil。
	Actor() txactor.Runtime
	// DirectActor 返回使用相同实体归属的 gRPC 直连调用入口。
	DirectActor() txactor.Caller
	// Transaction 返回单机 Procedure 执行器；无状态服务返回 nil。
	Transaction() txn.Executor
	// SessionIDs 返回绑定当前 Discovery 运行身份的 SessionID 分配器；未启用时返回 nil。
	SessionIDs() sessionid.Allocator
	// HasCMod 判断能力模组是否进入本次启动闭包。
	HasCMod(CModID) bool
	// Advanced 返回受支持的专家级扩展面。
	Advanced() Advanced
}

type serviceContext struct {
	runtime   *runtime
	admission lifecycle.Admission
	tasks     lifecycle.Tasks
}

type cmodContext struct {
	*serviceContext
	server *Server
	tasks  lifecycle.Tasks
}

func newCModContextWithTasks(server *Server, tasks lifecycle.Tasks) CModContext {
	context := newCModContext(server).(*cmodContext)
	context.tasks = tasks
	return context
}

func (context *cmodContext) Tasks() lifecycle.Tasks {
	if context != nil && context.tasks != nil {
		return context.tasks
	}
	if context == nil || context.serviceContext == nil {
		return nil
	}
	return context.serviceContext.Tasks()
}

func newServiceContext(runtime *runtime, admission lifecycle.Admission) Context {
	return &serviceContext{runtime: runtime, admission: admission}
}

func newServiceContextWithTasks(runtime *runtime, admission lifecycle.Admission, tasks lifecycle.Tasks) Context {
	return &serviceContext{runtime: runtime, admission: admission, tasks: tasks}
}

func newCModContext(server *Server) CModContext {
	if server == nil {
		return &cmodContext{}
	}
	return &cmodContext{
		serviceContext: &serviceContext{runtime: server.runtime},
		server:         server,
	}
}

func (context *cmodContext) BindTransactionCommitParticipant(participant txn.CommitParticipant) error {
	if context == nil || context.runtime == nil || context.runtime.transaction == nil {
		return errors.New("framework: transaction runtime is not configured")
	}
	return context.runtime.transaction.BindCommitParticipant(participant)
}

func (service *serviceContext) AppID() string {
	if service == nil || service.runtime == nil {
		return ""
	}
	return service.runtime.config.AppID
}

func (service *serviceContext) Instance() discovery.Instance {
	if service == nil || service.runtime == nil {
		return discovery.Instance{}
	}
	return service.runtime.instanceSnapshot()
}

func (service *serviceContext) Tasks() lifecycle.Tasks {
	if service != nil && service.tasks != nil {
		return service.tasks
	}
	if service == nil || service.runtime == nil || service.runtime.lifecycle == nil {
		return nil
	}
	return taskAccess{Context: service.runtime.lifecycle, root: service.runtime.lifecycle}
}

func (service *serviceContext) Admission() lifecycle.Admission {
	if service == nil {
		return nil
	}
	return service.admission
}

func (service *serviceContext) Storage() Storage {
	if service == nil || service.runtime == nil || service.runtime.storage == nil {
		return nil
	}
	return storageAccess{storage: service.runtime.storage}
}

func (service *serviceContext) StorageFor(mode meta.StorageMode) ModeStorage {
	if service == nil || service.runtime == nil || service.runtime.storage == nil {
		return nil
	}
	return modeStorageAccess{storage: service.runtime.storage, mode: mode}
}

func (service *serviceContext) RPC() txrpc.Registrar {
	if service == nil || service.runtime == nil || service.runtime.rpc == nil {
		return nil
	}
	return rpcAccess{host: service.runtime.rpc}
}

func (service *serviceContext) Directory() discovery.Directory {
	if service == nil || service.runtime == nil || service.runtime.directory == nil {
		return nil
	}
	return directoryAccess{directory: service.runtime.directory}
}

func (service *serviceContext) Actor() txactor.Runtime {
	if service == nil || service.runtime == nil || service.runtime.actor == nil {
		return nil
	}
	return service.runtime.actor
}

func (service *serviceContext) Transaction() txn.Executor {
	if service == nil || service.runtime == nil || service.runtime.transaction == nil {
		return nil
	}
	return transactionAccess{runtime: service.runtime.transaction}
}

func (service *serviceContext) SessionIDs() sessionid.Allocator {
	if service == nil || service.runtime == nil {
		return nil
	}
	return service.runtime.sessionIDsSnapshot()
}

func (service *serviceContext) HasCMod(id CModID) bool {
	return service != nil && service.runtime != nil && service.runtime.hasCMod(id)
}

func (service *serviceContext) Advanced() Advanced {
	if service == nil {
		return nil
	}
	return advancedAccess{runtime: service.runtime}
}

func (service *serviceContext) DirectActor() txactor.Caller {
	if service == nil || service.runtime == nil {
		return nil
	}
	return service.runtime.actorDirect
}
