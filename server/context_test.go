package server

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/wal"
)

type contextWAL struct{}

type contextCommitSink struct{}

func (contextCommitSink) Prepare(context.Context, []txn.LockTarget) (txn.CommitReservation, error) {
	return contextCommitReservation{}, nil
}

type contextCommitReservation struct{}

func (contextCommitReservation) Commit([]txn.LockTarget) {}
func (contextCommitReservation) Abort()                  {}

func (contextWAL) Append(context.Context, wal.Record) error { return nil }
func (contextWAL) Read(context.Context, wal.Stream, uint64, int) ([]wal.Record, error) {
	return nil, nil
}
func (contextWAL) Replay(context.Context, wal.Stream, uint64, func(context.Context, wal.Record) error) (uint64, error) {
	return 0, nil
}
func (contextWAL) Checkpoint(context.Context, wal.Stream, uint64, func(context.Context) error) error {
	return nil
}

func TestServiceContextExposesCModsWithoutLifecycleOwnership(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	ingress := lifecycle.NewIngress(root)
	runtime := &runtime{
		config:      Config{AppID: "context-test"},
		lifecycle:   root,
		storage:     &storageRuntime{},
		rpc:         &txrpc.Host{},
		transaction: &txn.Runtime{},
		directory:   discovery.NewServiceDirectory(),
		instance:    discovery.Instance{ID: "game-1", Services: []string{"game"}, Endpoint: "127.0.0.1:9000", Status: discovery.StatusRunning},
		cmods: map[CModID]struct{}{
			CModRedisStorage: {},
			CModRPC:          {},
			CModDiscovery:    {},
			CModTransaction:  {},
			CModWAL:          {},
		},
	}
	runtime.storage.wal = contextWAL{}
	serviceContext := newServiceContext(runtime, ingress)

	if got := serviceContext.AppID(); got != "context-test" {
		t.Fatalf("AppID() = %q", got)
	}
	instance := serviceContext.Instance()
	instance.Services[0] = "mutated"
	if got := serviceContext.Instance().Services[0]; got != "game" {
		t.Fatalf("Instance() leaked mutable services: %q", got)
	}
	if !serviceContext.HasCMod(CModTransaction) {
		t.Fatal("HasCMod(transaction) = false")
	}
	if serviceContext.Admission() == nil || serviceContext.Tasks() == nil || serviceContext.Storage() == nil || serviceContext.WAL() == nil || serviceContext.RPC() == nil || serviceContext.Transaction() == nil || serviceContext.Directory() == nil {
		t.Fatal("service context omitted an assembled capability")
	}

	if _, ok := any(serviceContext.Tasks()).(interface{ Cancel() bool }); ok {
		t.Fatal("Tasks exposes lifecycle cancellation")
	}
	if _, ok := any(serviceContext.RPC()).(interface{ Stop(context.Context) error }); ok {
		t.Fatal("RPC exposes shared host shutdown")
	}
	if _, ok := any(serviceContext.Transaction()).(interface{ Shutdown(context.Context) error }); ok {
		t.Fatal("Transaction exposes runtime shutdown")
	}
	if _, ok := any(serviceContext.Directory()).(interface {
		Replace([]discovery.Instance) error
	}); ok {
		t.Fatal("Directory exposes discovery snapshot mutation")
	}
	if _, ok := any(serviceContext.WAL()).(interface{ Close(context.Context) error }); ok {
		t.Fatal("WAL exposes shared Mongo connection ownership")
	}
}

func TestAdvancedContextKeepsSharedResourceOwnershipPrivate(t *testing.T) {
	runtime := &runtime{storage: &storageRuntime{}}
	advanced := newServiceContext(runtime, nil).Advanced()
	if advanced == nil || advanced.Redis() == nil {
		t.Fatal("Advanced().Redis() = nil")
	}
	if _, ok := any(advanced.Redis()).(interface{ Close(context.Context) error }); ok {
		t.Fatal("advanced storage exposes framework close ownership")
	}
}

func TestCModContextBindsTransactionCommitSinkWithoutExposingBusinessContext(t *testing.T) {
	runtime := &runtime{transaction: txn.NewRuntime()}
	service := newServiceContext(runtime, nil)
	if _, ok := any(service).(interface {
		BindTransactionCommitSink(txn.CommitSink) error
	}); ok {
		t.Fatal("business Context exposes transaction commit sink binding")
	}

	mod := newCModContext(runtime)
	if err := mod.BindTransactionCommitSink(contextCommitSink{}); err != nil {
		t.Fatalf("BindTransactionCommitSink returned error: %v", err)
	}
	if err := mod.BindTransactionCommitSink(contextCommitSink{}); err == nil {
		t.Fatal("second BindTransactionCommitSink returned nil error")
	}
}
