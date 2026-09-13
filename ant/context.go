package ant

import (
	"context"
	"fmt"
	"slices"
	"strings"

	framework "github.com/ngnl47/tx/internal/framework"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/sessionid"
	txstorage "github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txredis "github.com/ngnl47/tx/storage/redis"
)

// StorageReaders 是生成 data 包使用的读取契约。
type StorageReaders interface {
	// RedisReader 返回带健康闸门的 Redis 文档读取器。
	RedisReader() txstorage.DocumentLoader
	// MongoReader 返回带健康闸门的 MongoDB 文档读取器。
	MongoReader() txstorage.DocumentLoader
	// FusionReader 返回同时依赖 Redis 和 MongoDB 的 Fusion 读取器。
	FusionReader() txstorage.DocumentLoader
}

// Storage 是 Ant 管理的持久化能力，不暴露连接和关闭所有权。
type Storage interface {
	// Context 返回由进程根生命周期派生的只读上下文。
	Context() context.Context
	// ChangeWriter 返回当前能力范围内的变更写入器。
	ChangeWriter() txstorage.ChangeApplier
}

// RedisStorage 只暴露 Redis 文档读写与高级脚本路由。
type RedisStorage interface {
	Storage
	// RedisReader 返回带健康闸门的 Redis 文档读取器。
	RedisReader() txstorage.DocumentLoader
	// ScriptRoutes 返回高级 Redis Lua 脚本路由，不转移 client 所有权。
	ScriptRoutes() txredis.ScriptRoutes
}

// MongoStorage 只暴露 MongoDB 文档读写。
type MongoStorage interface {
	txmongo.DocumentFinder
	Storage
	// MongoReader 返回带健康闸门的 MongoDB 文档读取器。
	MongoReader() txstorage.DocumentLoader
	txstorage.DocumentBackfiller
	txmongo.CounterStore
}

// FusionStorage 只暴露 Fusion 文档读写。
type FusionStorage interface {
	Storage
	// FusionReader 返回同时依赖 Redis 和 MongoDB 的 Fusion 读取器。
	FusionReader() txstorage.DocumentLoader
}

// Context 只在 Service 或 CMod 构造边界读取已经声明的运行时能力。
// 能力直接返回；未声明、不可用或在 Validate 阶段读取时由框架报告装配错误。
// Service、Setup 和 CMod 构造边界将能力错误转为启动错误，普通业务 panic 不被吞掉。
type Context interface {
	// AppID 返回当前应用的稳定集群标识。
	AppID() string
	// Instance 返回当前进程已注册的实例快照。
	Instance() discovery.Instance
	// Tasks 返回当前 owner 的后台任务入口。
	Tasks() lifecycle.Tasks
	// Admission 返回进程外部工作准入状态。
	Admission() lifecycle.Admission
	// Storage 返回当前 owner 已声明的最强文档存储能力。
	Storage() Storage
	// CModRedisStorage 返回已声明的 Redis 文档和脚本路由能力。
	CModRedisStorage() RedisStorage
	// CModMongoStorage 返回已声明的 MongoDB 文档能力。
	CModMongoStorage() MongoStorage
	// CModFusionStorage 返回已声明的 Fusion 文档能力。
	CModFusionStorage() FusionStorage
	// CModRPC 返回共享 gRPC Host 的服务注册入口。
	CModRPC() txrpc.Registrar
	// CModDiscovery 返回 Watch 维护的只读服务实例目录。
	CModDiscovery() discovery.Directory
	// CModSessionID 返回已声明的 SessionID 分配能力。
	CModSessionID() SessionIDAllocator
	// CModActor 返回 Actor 消息解析和投递入口。
	CModActor() txactor.Messenger
	// CModSlots 返回 Discovery CMod 管理的只读槽位归属，不提供创建或晋升。
	CModSlots() discovery.SlotReader
	// CModSlotResolver 返回使用本进程 Watch 缓存的只读槽位解析器。
	CModSlotResolver() *discovery.SlotResolver
	// CModProcedure 返回 Ant 管理的单机 Procedure 执行器。
	CModProcedure() txn.Executor
}

type frameworkContext interface {
	AppID() string
	Instance() discovery.Instance
	Tasks() lifecycle.Tasks
	Storage() framework.Storage
	StorageFor(meta.StorageMode) framework.ModeStorage
	RPC() txrpc.Registrar
	Directory() discovery.Directory
	Actor() txactor.Messenger
	SessionIDs() sessionid.Allocator
	Transaction() txn.Executor
	HasCMod(framework.CModID) bool
	Advanced() framework.Advanced
}

type contextView struct {
	kernel     frameworkContext
	owner      string
	allowed    map[CModID]struct{}
	validating bool
}

func newContextView(kernel frameworkContext, owner string, ids []CModID) *contextView {
	allowed := make(map[CModID]struct{}, len(ids))
	for _, id := range ids {
		allowed[id] = struct{}{}
	}
	return &contextView{kernel: kernel, owner: owner, allowed: allowed}
}

func newSetupContext(kernel framework.Context) Context {
	return &contextView{kernel: kernel, owner: "setup"}
}

func (view *contextView) AppID() string {
	if view == nil || view.kernel == nil {
		return ""
	}
	return view.kernel.AppID()
}

func (view *contextView) Instance() discovery.Instance {
	if view == nil || view.kernel == nil {
		return discovery.Instance{}
	}
	return view.kernel.Instance()
}

func (view *contextView) Tasks() lifecycle.Tasks {
	if view == nil || view.kernel == nil {
		return nil
	}
	return view.kernel.Tasks()
}

func (view *contextView) Admission() lifecycle.Admission {
	if view == nil || view.kernel == nil {
		return nil
	}
	provider, ok := view.kernel.(interface {
		Admission() lifecycle.Admission
	})
	if !ok {
		return nil
	}
	return provider.Admission()
}

// Storage 返回当前 owner 已声明的最强存储能力，优先级为 Fusion、MongoDB、Redis。
func (view *contextView) Storage() Storage {
	if view != nil && view.validating {
		panic(framework.CapabilityError{Err: fmt.Errorf("ant: %s cannot read storage during Validate", view.ownerName())})
	}
	if view != nil && view.kernel != nil && view.allowed == nil {
		if value := view.kernel.Storage(); value != nil {
			return value
		}
	}
	for _, id := range []CModID{CModFusionStorage, CModMongoStorage, CModRedisStorage} {
		if view.canUse(id) && view.kernel.HasCMod(toFrameworkCModID(id)) {
			value := view.kernel.StorageFor(storageMode(id))
			if value != nil {
				return modeStorageView{storage: value}
			}
		}
	}
	panic(framework.CapabilityError{Err: fmt.Errorf("ant: %s has no declared storage CMod", view.ownerName())})
}

func (view *contextView) CModRedisStorage() RedisStorage {
	storage := view.modeStorage(CModRedisStorage, meta.StorageModeRedis)
	advanced := view.kernel.Advanced()
	if advanced == nil || advanced.Redis() == nil {
		panic(framework.CapabilityError{Err: view.unavailable(CModRedisStorage)})
	}
	return redisStorageView{modeStorageView: modeStorageView{storage: storage}, routes: advanced.Redis()}
}

type redisStorageView struct {
	modeStorageView
	routes txredis.ScriptRoutes
}

func (view redisStorageView) RedisReader() txstorage.DocumentLoader { return view.storage.Reader() }

func (view redisStorageView) ScriptRoutes() txredis.ScriptRoutes {
	return view.routes
}

func (view *contextView) CModMongoStorage() MongoStorage {
	storage := view.modeStorage(CModMongoStorage, meta.StorageModeMongo)
	return mongoStorageView{modeStorageView{storage: storage}}
}

func (view *contextView) CModFusionStorage() FusionStorage {
	storage := view.modeStorage(CModFusionStorage, meta.StorageModeFusion)
	return fusionStorageView{modeStorageView{storage: storage}}
}

func (view *contextView) CModRPC() txrpc.Registrar {
	view.require(CModRPC)
	value := view.kernel.RPC()
	if value == nil {
		panic(framework.CapabilityError{Err: view.unavailable(CModRPC)})
	}
	return value
}

func (view *contextView) CModDiscovery() discovery.Directory {
	view.require(CModDiscovery)
	value := view.kernel.Directory()
	if value == nil {
		panic(framework.CapabilityError{Err: view.unavailable(CModDiscovery)})
	}
	return value
}

func (view *contextView) CModSessionID() SessionIDAllocator {
	view.require(CModSessionID)
	value := view.kernel.SessionIDs()
	if value == nil {
		panic(framework.CapabilityError{Err: view.unavailable(CModSessionID)})
	}
	return value
}

func (view *contextView) CModActor() txactor.Messenger {
	view.require(CModActor)
	value := view.kernel.Actor()
	if value == nil {
		panic(framework.CapabilityError{Err: view.unavailable(CModActor)})
	}
	return value
}

// CModSlots 返回 Discovery CMod 管理的只读槽位归属。
func (view *contextView) CModSlots() discovery.SlotReader {
	view.require(CModDiscovery)
	advanced := view.kernel.Advanced()
	if advanced == nil || advanced.Slots() == nil {
		panic(framework.CapabilityError{Err: view.unavailable(CModDiscovery)})
	}
	return advanced.Slots()
}

// CModSlotResolver 组合已声明的槽位与成员读取能力，不取得控制面写权限。
func (view *contextView) CModSlotResolver() *discovery.SlotResolver {
	resolver, err := discovery.NewSlotResolver(view.CModSlots(), view.CModDiscovery())
	if err != nil {
		panic(framework.CapabilityError{Err: err})
	}
	return resolver
}

// CModProcedure 返回 Ant 管理的单机 Procedure 执行器。
func (view *contextView) CModProcedure() txn.Executor {
	view.require(CModProcedure)
	value := view.kernel.Transaction()
	if value == nil {
		panic(framework.CapabilityError{Err: view.unavailable(CModProcedure)})
	}
	return value
}

func (view *contextView) modeStorage(id CModID, mode meta.StorageMode) framework.ModeStorage {
	view.require(id)
	value := view.kernel.StorageFor(mode)
	if value == nil {
		panic(framework.CapabilityError{Err: view.unavailable(id)})
	}
	return value
}

type modeStorageView struct{ storage framework.ModeStorage }

func (view modeStorageView) Context() context.Context { return view.storage.Context() }

func (view modeStorageView) ChangeWriter() txstorage.ChangeApplier {
	return view.storage.ChangeWriter()
}

type mongoStorageView struct{ modeStorageView }

func (view mongoStorageView) FindDocument(ctx context.Context, spec meta.TypeSpec, shardKey any, filter txmongo.Filter, fields ...string) (map[string]any, bool, error) {
	return view.storage.FindDocument(ctx, spec, shardKey, filter, fields...)
}

func (view mongoStorageView) MongoReader() txstorage.DocumentLoader { return view.storage.Reader() }

func (view mongoStorageView) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, doc map[string]any, options txstorage.ApplyOptions) (bool, error) {
	return view.storage.BackfillDocumentWithOptions(ctx, spec, key, doc, options)
}

func (view mongoStorageView) Reserve(ctx context.Context, routeType string, shardKey any, collection, key string, size int64) (int64, error) {
	return view.storage.Reserve(ctx, routeType, shardKey, collection, key, size)
}

type fusionStorageView struct{ modeStorageView }

func (view fusionStorageView) FusionReader() txstorage.DocumentLoader { return view.storage.Reader() }

func storageMode(id CModID) meta.StorageMode {
	switch id {
	case CModRedisStorage:
		return meta.StorageModeRedis
	case CModMongoStorage:
		return meta.StorageModeMongo
	case CModFusionStorage:
		return meta.StorageModeFusion
	default:
		return ""
	}
}

func (view *contextView) require(id CModID) {
	if view != nil && view.validating {
		panic(framework.CapabilityError{Err: fmt.Errorf("ant: %s cannot read CMod %q during Validate", view.ownerName(), id)})
	}
	if view == nil || view.kernel == nil {
		panic(framework.CapabilityError{Err: fmt.Errorf("ant: %s context is unavailable", view.ownerName())})
	}
	if view.allowed == nil {
		if view.kernel.HasCMod(toFrameworkCModID(id)) {
			return
		}
		panic(framework.CapabilityError{Err: view.unavailable(id)})
	}
	if _, ok := view.allowed[id]; ok {
		return
	}
	ids := make([]string, 0, len(view.allowed))
	for current := range view.allowed {
		ids = append(ids, string(current))
	}
	slices.Sort(ids)
	panic(framework.CapabilityError{Err: fmt.Errorf("ant: %s requested CMod %q but declared CMods are [%s]", view.ownerName(), id, strings.Join(ids, ","))})
}

func (view *contextView) canUse(id CModID) bool {
	if view == nil || view.kernel == nil {
		return false
	}
	if view.allowed == nil {
		return true
	}
	_, ok := view.allowed[id]
	return ok
}

func (view *contextView) unavailable(id CModID) error {
	return fmt.Errorf("ant: %s CMod %q is unavailable", view.ownerName(), id)
}

func (view *contextView) ownerName() string {
	if view == nil || strings.TrimSpace(view.owner) == "" {
		return "unknown owner"
	}
	return view.owner
}
