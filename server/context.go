package server

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	txstorage "github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/wal"
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
	// Tasks 返回受进程根生命周期管理的后台任务入口。
	Tasks() lifecycle.Tasks
	// Admission 返回当前进程的外部工作准入状态。
	Admission() lifecycle.Admission
	// Storage 返回框架管理的存储能力；服务未声明存储依赖时可能为 nil。
	Storage() Storage
	// WAL 返回当前 L4 服务选中的预写日志；未启用时返回 nil。
	WAL() wal.Store
	// RPC 返回共享 gRPC Host 的注册入口；未启用时返回 nil。
	RPC() txrpc.Registrar
	// Directory 返回服务发现维护的只读实例目录；未启用时返回 nil。
	Directory() discovery.Directory
	// Actor 返回 Actor 消息投递入口；未启用时返回 nil。
	Actor() txactor.Messenger
	// Transaction 返回单机 Procedure 执行器；无状态服务返回 nil。
	Transaction() txn.Executor
	// LogicalSlot 返回当前有状态实例声明的稳定逻辑槽位。
	LogicalSlot() (LogicalSlot, bool)
	// HasCMod 判断能力模组是否进入本次启动闭包。
	HasCMod(CModID) bool
	// Advanced 返回受支持的专家级扩展面。
	Advanced() Advanced
}

type serviceContext struct {
	runtime   *runtime
	admission lifecycle.Admission
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
		return errors.New("server: transaction runtime is not configured")
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

func (service *serviceContext) WAL() wal.Store {
	if service == nil || service.runtime == nil {
		return nil
	}
	return service.runtime.wal
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

func (service *serviceContext) Actor() txactor.Messenger {
	if service == nil || service.runtime == nil || service.runtime.actor == nil {
		return nil
	}
	return actorAccess{runtime: service.runtime.actor}
}

func (service *serviceContext) Transaction() txn.Executor {
	if service == nil || service.runtime == nil || service.runtime.transaction == nil {
		return nil
	}
	return transactionAccess{runtime: service.runtime.transaction}
}

func (service *serviceContext) LogicalSlot() (LogicalSlot, bool) {
	if service == nil || service.runtime == nil || service.runtime.logicalSlot == nil {
		return LogicalSlot{}, false
	}
	return *service.runtime.logicalSlot, true
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
