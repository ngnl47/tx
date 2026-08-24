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
	config      Config
	lifecycle   *lifecycle.WaitGroupContext
	storage     *storageRuntime
	rpc         *txrpc.Host
	actor       *txactor.Runtime
	transaction *txn.Runtime
	directory   *discovery.ServiceDirectory
	slots       discovery.SlotStore
	instanceMu  sync.RWMutex
	instance    discovery.Instance
	logicalSlot *LogicalSlot
	cmodsMu     sync.RWMutex
	cmods       map[CModID]struct{}
}

func (runtime *runtime) markCMod(id CModID, started bool) {
	if runtime == nil {
		return
	}
	runtime.cmodsMu.Lock()
	defer runtime.cmodsMu.Unlock()
	if runtime.cmods == nil {
		runtime.cmods = make(map[CModID]struct{})
	}
	if started {
		runtime.cmods[id] = struct{}{}
	} else {
		delete(runtime.cmods, id)
	}
}

func (runtime *runtime) hasCMod(id CModID) bool {
	if runtime == nil {
		return false
	}
	runtime.cmodsMu.RLock()
	_, ok := runtime.cmods[id]
	runtime.cmodsMu.RUnlock()
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
