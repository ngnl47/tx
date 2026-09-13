package framework

import (
	"context"
	"maps"
	"sync"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/sessionid"
)

// actorTransport 是框架持有的实体传输收尾能力，不向业务开放连接所有权。
type actorTransport interface {
	Flush(context.Context) error
	Close()
}

type runtime struct {
	config        Config
	lifecycle     *lifecycle.WaitGroupContext
	storage       *storageRuntime
	rpc           *txrpc.Host
	actor         *txactor.Runtime
	entities      *actorhost.Runtime
	actorRPC      actorTransport
	actorKinds    []txactor.Kind
	actorCatalog  map[string]actorKindSpec
	transaction   *txn.Runtime
	directory     *discovery.ServiceDirectory
	slots         discovery.SlotStore
	sessionIDMu   sync.RWMutex
	sessionIDs    *sessionIDRuntime
	instanceMu    sync.RWMutex
	instance      discovery.Instance
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
	instance.Endpoints = maps.Clone(instance.Endpoints)
	return instance
}

func (runtime *runtime) setInstance(instance discovery.Instance) {
	if runtime == nil {
		return
	}
	runtime.instanceMu.Lock()
	runtime.instance = cloneRuntimeInstance(instance)
	runtime.instanceMu.Unlock()
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

func (runtime *runtime) setActorEndpoint(endpoint string) {
	if runtime == nil {
		return
	}
	runtime.instanceMu.Lock()
	runtime.instance.ActorEndpoint = endpoint
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

func (runtime *runtime) setSessionIDs(allocator *sessionIDRuntime) {
	if runtime == nil {
		return
	}
	runtime.sessionIDMu.Lock()
	runtime.sessionIDs = allocator
	runtime.sessionIDMu.Unlock()
}

func (runtime *runtime) sessionIDsSnapshot() sessionid.Allocator {
	if runtime == nil {
		return nil
	}
	runtime.sessionIDMu.RLock()
	defer runtime.sessionIDMu.RUnlock()
	if runtime.sessionIDs == nil {
		return nil
	}
	return runtime.sessionIDs
}

func (runtime *runtime) enableSessionIDs() {
	if runtime == nil {
		return
	}
	runtime.sessionIDMu.RLock()
	allocator := runtime.sessionIDs
	runtime.sessionIDMu.RUnlock()
	allocator.enable()
}

func (runtime *runtime) closeSessionIDs() {
	if runtime == nil {
		return
	}
	runtime.sessionIDMu.RLock()
	allocator := runtime.sessionIDs
	runtime.sessionIDMu.RUnlock()
	allocator.close()
}

func (runtime *runtime) clearSessionIDs() {
	if runtime == nil {
		return
	}
	runtime.sessionIDMu.Lock()
	allocator := runtime.sessionIDs
	runtime.sessionIDs = nil
	runtime.sessionIDMu.Unlock()
	allocator.close()
}

func cloneRuntimeInstance(instance discovery.Instance) discovery.Instance {
	instance.Services = append([]string(nil), instance.Services...)
	instance.Endpoints = maps.Clone(instance.Endpoints)
	return instance
}
