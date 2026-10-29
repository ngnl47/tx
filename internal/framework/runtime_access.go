package framework

import (
	"context"
	"fmt"
	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/txn"
	txruntime "github.com/ngnl47/tx/runtime"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/redis/goredis"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
)

type runtimeAccess struct {
	runtime  *runtime
	service  string
	requires map[CModID]bool
}

func newRuntimeAccess(r *runtime, spec Spec) *runtimeAccess {
	p := &runtimeAccess{runtime: r, service: spec.Name, requires: map[CModID]bool{}}
	for _, id := range spec.Requires {
		p.requires[id] = true
	}
	return p
}
func (p *runtimeAccess) requireOpen() {
	if p.runtime.accessClosed.Load() {
		panic(CapabilityError{Err: fmt.Errorf("service %q runtime is closed", p.service)})
	}
}
func (p *runtimeAccess) AppID() string { p.requireOpen(); return p.runtime.config.AppID }
func (p *runtimeAccess) require(id CModID) {
	p.requireOpen()
	if (p.requires != nil && !p.requires[id]) || !p.runtime.hasCMod(id) {
		panic(CapabilityError{Err: fmt.Errorf("service %q requires declared CMod %q", p.service, id)})
	}
}
func (p *runtimeAccess) Actor() txactor.Runtime { p.require(CModActor); return p.runtime.actor }
func storageCMod(mode meta.StorageMode) CModID {
	switch mode {
	case meta.StorageModeRedis:
		return CModRedisStorage
	case meta.StorageModeMongo:
		return CModMongoStorage
	case meta.StorageModeFusion:
		return CModFusionStorage
	}
	panic(CapabilityError{Err: fmt.Errorf("unsupported storage mode %q", mode)})
}
func (p *runtimeAccess) Reader(mode meta.StorageMode) storage.DocumentLoader {
	p.require(storageCMod(mode))
	return modeStorageAccess{storage: p.runtime.storage, mode: mode}.Reader()
}
func (p *runtimeAccess) Writer() storage.ChangeApplier { return p }
func (p *runtimeAccess) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	p.require(storageCMod(spec.Storage.Mode))
	return p.runtime.storage.ChangeWriter().ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options)
}
func (p *runtimeAccess) Procedure() txn.Executor {
	p.require(CModProcedure)
	return p.runtime.transaction
}
func (p *runtimeAccess) RedisClients() goredis.ClientProvider {
	p.require(CModRedisStorage)
	return newServiceContext(p.runtime, nil).Advanced().Redis()
}

func (p *runtimeAccess) MongoBackfiller() storage.DocumentBackfiller {
	p.require(CModMongoStorage)
	return modeStorageAccess{storage: p.runtime.storage, mode: meta.StorageModeMongo}
}

// RuntimeAccess 只在启动时绑定能力，不创建连接，也不保存任何请求上下文。
func (c *serviceContext) RuntimeAccess(owner string, ids []CModID) txruntime.Provider {
	p := newRuntimeAccess(c.runtime, Spec{Name: owner, Requires: ids})
	if ids == nil {
		p.requires = nil
	}
	p.requireOpen()
	return p
}

func (p *runtimeAccess) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	p.require(storageCMod(spec.Storage.Mode))
	return p.runtime.storage.ChangeWriter().(storage.DeleteApplier).ApplyDelete(ctx, spec, key)
}
func (p *runtimeAccess) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) (bool, error) {
	p.require(storageCMod(spec.Storage.Mode))
	return p.runtime.storage.ChangeWriter().(storage.DocumentBackfiller).BackfillDocumentWithOptions(ctx, spec, key, doc, options)
}

func (p *runtimeAccess) MongoDatabase(ctx context.Context, route string, key any) (*mongodriver.Database, error) {
	p.require(CModMongoStorage)
	return p.runtime.storage.MongoDatabase(ctx, route, key)
}
