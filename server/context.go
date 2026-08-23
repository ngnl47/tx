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
)

// StorageReaders is the read contract consumed by generated data packages.
type StorageReaders interface {
	RedisReader() txstorage.DocumentLoader
	MongoReader() txstorage.DocumentLoader
	FusionReader() txstorage.DocumentLoader
}

// Storage is the stable application-facing storage contract. Connection pools,
// health state, background workers, and shutdown remain server-owned.
type Storage interface {
	StorageReaders
	Context() context.Context
	ChangeWriter() txstorage.ChangeApplier
	RequireWritable() error
}

// Advanced exposes the first supported expert extension surface. Ordinary
// business services should use the direct Context capability methods.
type Advanced interface {
	Redis() txredis.ScriptRoutes
	Slots() discovery.SlotStore
}

// Context is the stable, ownership-safe capability view passed to a selected
// service. It can perform business work, but every returned module contract
// excludes process start/stop ownership.
type Context interface {
	AppID() string
	Instance() discovery.Instance
	Tasks() lifecycle.Tasks
	Admission() lifecycle.Admission
	Storage() Storage
	RPC() txrpc.Registrar
	Directory() discovery.Directory
	Actor() txactor.Messenger
	Transaction() txn.Executor
	LogicalSlot() (LogicalSlot, bool)
	Has(Capabilities) bool
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

func (service *serviceContext) Has(capabilities Capabilities) bool {
	return service != nil && service.runtime != nil && service.runtime.capabilities&capabilities == capabilities
}

func (service *serviceContext) Advanced() Advanced {
	if service == nil {
		return nil
	}
	return advancedAccess{runtime: service.runtime}
}
