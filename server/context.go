package server

import (
	"context"

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
	RedisReader() txstorage.DocumentLoader
	MongoReader() txstorage.DocumentLoader
	FusionReader() txstorage.DocumentLoader
}

// Storage 是稳定的应用侧存储契约。连接池、健康状态、后台任务和关闭流程仍由 server 管理。
type Storage interface {
	StorageReaders
	Context() context.Context
	ChangeWriter() txstorage.ChangeApplier
	RequireWritable() error
}

// Advanced 暴露第一版受支持的专家级扩展面。普通业务服务应直接使用 Context 的能力方法。
type Advanced interface {
	Redis() txredis.ScriptRoutes
	Slots() discovery.SlotStore
}

// Context 是传递给选中服务的稳定能力视图，并隔离资源所有权。
// 它可以执行业务工作，但返回的模块契约均不包含进程启动或停止权限。
type Context interface {
	AppID() string
	Instance() discovery.Instance
	Tasks() lifecycle.Tasks
	Admission() lifecycle.Admission
	Storage() Storage
	WAL() wal.Store
	RPC() txrpc.Registrar
	Directory() discovery.Directory
	Actor() txactor.Messenger
	Transaction() txn.Executor
	LogicalSlot() (LogicalSlot, bool)
	HasCMod(CModID) bool
	Advanced() Advanced
}

type serviceContext struct {
	runtime   *runtime
	admission lifecycle.Admission
}

func newServiceContext(runtime *runtime, admission lifecycle.Admission) Context {
	return &serviceContext{runtime: runtime, admission: admission}
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
	if service == nil || service.runtime == nil || service.runtime.storage == nil {
		return nil
	}
	return service.runtime.storage.WAL()
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
