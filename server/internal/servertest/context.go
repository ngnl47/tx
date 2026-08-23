// Package servertest provides fakes for tests of server-owned service adapters.
// It is internal so production users cannot treat fake assembly as a public API.
package servertest

import (
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	txserver "github.com/ngnl47/tx/server"
)

// Context is a configurable server.Context fake for standard service tests.
type Context struct {
	App                string
	InstanceValue      discovery.Instance
	TasksValue         lifecycle.Tasks
	AdmissionValue     lifecycle.Admission
	StorageValue       txserver.Storage
	RPCValue           txrpc.Registrar
	DirectoryValue     discovery.Directory
	ActorValue         txactor.Messenger
	TransactionValue   txn.Executor
	LogicalSlotValue   txserver.LogicalSlot
	LogicalSlotPresent bool
	Capabilities       txserver.Capabilities
	AdvancedValue      txserver.Advanced
}

func (context Context) AppID() string                  { return context.App }
func (context Context) Instance() discovery.Instance   { return context.InstanceValue }
func (context Context) Tasks() lifecycle.Tasks         { return context.TasksValue }
func (context Context) Admission() lifecycle.Admission { return context.AdmissionValue }
func (context Context) Storage() txserver.Storage      { return context.StorageValue }
func (context Context) RPC() txrpc.Registrar           { return context.RPCValue }
func (context Context) Directory() discovery.Directory { return context.DirectoryValue }
func (context Context) Actor() txactor.Messenger       { return context.ActorValue }
func (context Context) Transaction() txn.Executor      { return context.TransactionValue }
func (context Context) LogicalSlot() (txserver.LogicalSlot, bool) {
	return context.LogicalSlotValue, context.LogicalSlotPresent
}
func (context Context) Has(capabilities txserver.Capabilities) bool {
	return context.Capabilities&capabilities == capabilities
}
func (context Context) Advanced() txserver.Advanced { return context.AdvancedValue }

var _ txserver.Context = Context{}
