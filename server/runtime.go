package server

import (
	"sync"

	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/wal"
)

type runtime struct {
	config        Config
	lifecycle     *lifecycle.WaitGroupContext
	storage       *storageRuntime
	rpc           *txrpc.Host
	actor         *txactor.Runtime
	actorKinds    []txactor.Kind
	transaction   *txn.Runtime
	wal           wal.Store
	directory     *discovery.ServiceDirectory
	slots         discovery.SlotStore
	instanceMu    sync.RWMutex
	instance      discovery.Instance
	logicalSlot   *LogicalSlot
	selectedCMods map[CModID]struct{}
}

func (runtime *runtime) hasCMod(id CModID) bool {
	if runtime == nil {
		return false
	}
	_, ok := runtime.selectedCMods[id]
	return ok
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

func (runtime *runtime) setInstanceEndpoint(endpoint string) {
	if runtime == nil {
		return
	}
	runtime.instanceMu.Lock()
	runtime.instance.Endpoint = endpoint
	runtime.instanceMu.Unlock()
}

func (runtime *runtime) ensureStorage() *storageRuntime {
	if runtime == nil {
		return nil
	}
	if runtime.storage == nil {
		runtime.storage = newStorageRuntime(runtime.lifecycle)
	}
	return runtime.storage
}
