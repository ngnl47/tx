package server

import (
	"sync"

	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
)

type runtime struct {
	config       Config
	lifecycle    *lifecycle.WaitGroupContext
	storage      *storageRuntime
	rpc          *txrpc.Host
	actor        *txactor.Runtime
	transaction  *txn.Runtime
	directory    *discovery.ServiceDirectory
	slots        discovery.SlotStore
	instanceMu   sync.RWMutex
	instance     discovery.Instance
	logicalSlot  *LogicalSlot
	capabilities Capabilities
}

func (runtime *runtime) instanceSnapshot() discovery.Instance {
	if runtime == nil {
		return discovery.Instance{}
	}
	runtime.instanceMu.RLock()
	defer runtime.instanceMu.RUnlock()
	instance := runtime.instance
	instance.Services = append([]string(nil), instance.Services...)
	return instance
}

func (runtime *runtime) setInstanceStatus(status discovery.Status) {
	if runtime == nil {
		return
	}
	runtime.instanceMu.Lock()
	runtime.instance.Status = status
	runtime.instanceMu.Unlock()
}
