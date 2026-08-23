package server

import (
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
	instance     discovery.Instance
	logicalSlot  *LogicalSlot
	capabilities Capabilities
}
