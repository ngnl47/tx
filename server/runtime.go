package server

import (
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
)

// Runtime exposes framework-owned resources to services. Services consume these
// objects but do not create or close them.
type Runtime struct {
	Config       Config
	Lifecycle    *lifecycle.WaitGroupContext
	Ingress      *lifecycle.Ingress
	Storage      *Storage
	RPC          *txrpc.Host
	Actor        *txactor.Runtime
	Transaction  *txn.Runtime
	Directory    *discovery.ServiceDirectory
	Slots        discovery.SlotStore
	Instance     discovery.Instance
	LogicalSlot  *LogicalSlot
	Capabilities Capabilities
}
