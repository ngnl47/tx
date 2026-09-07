package framework

import (
	"context"
	"fmt"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	txstorage "github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txredis "github.com/ngnl47/tx/storage/redis"
	"google.golang.org/grpc"
)

var (
	_ lifecycle.Tasks      = taskAccess{}
	_ Storage              = storageAccess{}
	_ ModeStorage          = modeStorageAccess{}
	_ txrpc.Registrar      = rpcAccess{}
	_ discovery.Directory  = directoryAccess{}
	_ txactor.Messenger    = actorAccess{}
	_ txn.Executor         = transactionAccess{}
	_ Advanced             = advancedAccess{}
	_ txredis.ScriptRoutes = redisRoutesAccess{}
)

type taskAccess struct {
	context.Context
	root *lifecycle.WaitGroupContext
}

func (access taskAccess) Go(task func(context.Context)) error {
	return access.root.Go(task)
}

type storageAccess struct{ storage *storageRuntime }

func (access storageAccess) Context() context.Context {
	return readOnlyContext{Context: access.storage.Context()}
}

func (access storageAccess) RedisReader() txstorage.DocumentLoader {
	return access.storage.RedisReader()
}

func (access storageAccess) MongoReader() txstorage.DocumentLoader {
	return access.storage.MongoReader()
}

func (access storageAccess) FusionReader() txstorage.DocumentLoader {
	return access.storage.FusionReader()
}

func (access storageAccess) ChangeWriter() txstorage.ChangeApplier {
	return access.storage.ChangeWriter()
}

type modeStorageAccess struct {
	storage *storageRuntime
	mode    meta.StorageMode
}

func (access modeStorageAccess) FindDocument(ctx context.Context, spec meta.TypeSpec, shardKey any, filter txmongo.Filter, fields ...string) (map[string]any, bool, error) {
	if access.mode != meta.StorageModeMongo || access.storage == nil {
		return nil, false, fmt.Errorf("framework: storage capability %q cannot query Mongo documents", access.mode)
	}
	return access.storage.FindDocument(ctx, spec, shardKey, filter, fields...)
}

func (access modeStorageAccess) Context() context.Context {
	if access.storage == nil {
		return context.Background()
	}
	return readOnlyContext{Context: access.storage.Context()}
}

func (access modeStorageAccess) Reader() txstorage.DocumentLoader {
	if access.storage == nil {
		return nil
	}
	switch access.mode {
	case meta.StorageModeRedis:
		return access.storage.RedisReader()
	case meta.StorageModeMongo:
		return access.storage.MongoReader()
	case meta.StorageModeFusion:
		return access.storage.FusionReader()
	default:
		return nil
	}
}

func (access modeStorageAccess) ChangeWriter() txstorage.ChangeApplier {
	var next txstorage.ChangeApplier
	if access.storage != nil {
		next = access.storage.ChangeWriter()
	}
	return modeChangeWriter{mode: access.mode, next: next}
}

func (access modeStorageAccess) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, doc map[string]any, options txstorage.ApplyOptions) (bool, error) {
	if access.mode != meta.StorageModeMongo {
		return false, fmt.Errorf("framework: storage capability %q cannot backfill Mongo documents", access.mode)
	}
	if access.storage == nil {
		return false, fmt.Errorf("framework: storage capability %q backfiller is unavailable", access.mode)
	}
	backfiller := access.storage.MongoBackfiller()
	if backfiller == nil {
		return false, fmt.Errorf("framework: storage capability %q backfiller is unavailable", access.mode)
	}
	return backfiller.BackfillDocumentWithOptions(ctx, spec, key, doc, options)
}

func (access modeStorageAccess) Reserve(ctx context.Context, routeType string, shardKey any, collection, key string, size int64) (int64, error) {
	if access.mode != meta.StorageModeMongo {
		return 0, fmt.Errorf("framework: storage capability %q cannot reserve Mongo counters", access.mode)
	}
	if access.storage == nil {
		return 0, fmt.Errorf("framework: storage capability %q counters are unavailable", access.mode)
	}
	counters := access.storage.MongoCounters()
	if counters == nil {
		return 0, fmt.Errorf("framework: storage capability %q counters are unavailable", access.mode)
	}
	return counters.Reserve(ctx, routeType, shardKey, collection, key, size)
}

type modeChangeWriter struct {
	mode meta.StorageMode
	next txstorage.ChangeApplier
}

func (writer modeChangeWriter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options txstorage.ApplyOptions) error {
	if spec.Storage.Mode != writer.mode {
		return fmt.Errorf("framework: storage capability %q cannot write type using %q mode", writer.mode, spec.Storage.Mode)
	}
	if writer.next == nil {
		return fmt.Errorf("framework: storage capability %q writer is unavailable", writer.mode)
	}
	return writer.next.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options)
}

type readOnlyContext struct{ context.Context }

type rpcAccess struct{ host *txrpc.Host }

func (access rpcAccess) Register(register func(grpc.ServiceRegistrar) error) error {
	return access.host.Register(register)
}

func (access rpcAccess) Endpoint() string {
	return access.host.Endpoint()
}

type directoryAccess struct{ directory *discovery.ServiceDirectory }

func (access directoryAccess) NextInstance(service string) (discovery.Instance, bool) {
	return access.directory.NextInstance(service)
}

func (access directoryAccess) RunningInstances(service string) []discovery.Instance {
	return access.directory.RunningInstances(service)
}

func (access directoryAccess) LookupInstance(id string) (discovery.Instance, bool) {
	return access.directory.LookupInstance(id)
}

func (access directoryAccess) LookupRouteInstance(id string) (discovery.RouteInstance, bool) {
	return access.directory.LookupRouteInstance(id)
}

type actorAccess struct{ runtime *txactor.Runtime }

func (access actorAccess) VirtualPID(kind, identity string) (*protoactor.PID, error) {
	return access.runtime.VirtualPID(kind, identity)
}

func (access actorAccess) Tell(pid *protoactor.PID, delivery txactor.Delivery) error {
	return access.runtime.Tell(pid, delivery)
}

func (access actorAccess) TellAccepted(ctx context.Context, pid *protoactor.PID, delivery txactor.Delivery) error {
	return access.runtime.TellAccepted(ctx, pid, delivery)
}

func (access actorAccess) Call(ctx context.Context, pid *protoactor.PID, delivery txactor.Delivery) (txactor.Result, error) {
	return access.runtime.Call(ctx, pid, delivery)
}

func (access actorAccess) CallFuture(ctx context.Context, pid *protoactor.PID, delivery txactor.Delivery) (*txactor.Future, error) {
	return access.runtime.CallFuture(ctx, pid, delivery)
}

type transactionAccess struct{ runtime *txn.Runtime }

func (access transactionAccess) Call(ctx context.Context, procedure txn.Procedure) error {
	return access.runtime.Call(ctx, procedure)
}

type advancedAccess struct{ runtime *runtime }

func (access advancedAccess) Redis() txredis.ScriptRoutes {
	if access.runtime == nil || access.runtime.storage == nil {
		return nil
	}
	return redisRoutesAccess{storage: access.runtime.storage}
}

func (access advancedAccess) Slots() discovery.SlotStore {
	if access.runtime == nil {
		return nil
	}
	return access.runtime.slots
}

type redisRoutesAccess struct{ storage *storageRuntime }

func (access redisRoutesAccess) ScriptClientFor(ctx context.Context, routeType string, shardKey any) (txredis.ScriptClient, error) {
	return access.storage.RedisScriptClientFor(ctx, routeType, shardKey)
}
