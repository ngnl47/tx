// Package runtime 提供当前服务已装配的基建入口，不创建连接或管理业务状态。
package runtime

import (
	"context"
	"fmt"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/redis/goredis"
)

// Provider 由启动装配或服务边界绑定能力；实现校验已启用能力、声明与资源生命周期。
// 项目可捕获 Provider 建立业务入口；tx 不维护进程全局默认实例。
type Provider interface {
	AppID() string
	Actor() actor.Runtime
	Reader(meta.StorageMode) storage.DocumentLoader
	MongoBackfiller() storage.DocumentBackfiller
	Writer() storage.ChangeApplier
	Procedure() txn.Executor
	RedisClients() goredis.ClientProvider
}
type contextKey struct{}

// Bind 由框架在服务启动、Actor 激活和后台任务边界调用；绑定不会跨 RPC 传输。
func Bind(ctx context.Context, provider Provider) context.Context {
	if provider == nil {
		panic("runtime: provider is nil")
	}
	return context.WithValue(ctx, contextKey{}, provider)
}

// ProviderFrom 返回本地作用域；未绑定时返回 nil，不回退其他应用。
func ProviderFrom(ctx context.Context) Provider {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(contextKey{}).(Provider)
	return p
}
func require(ctx context.Context) Provider {
	p := ProviderFrom(ctx)
	if p == nil {
		panic(fmt.Errorf("runtime: service context is required"))
	}
	return p
}

// Actor 返回所属服务的共享 Actor 调用入口。
func Actor(ctx context.Context) actor.Runtime { return require(ctx).Actor() }

// RedisReader、MongoReader 与 FusionReader 保留显式读取模式选择。
func RedisReader(ctx context.Context) storage.DocumentLoader {
	return require(ctx).Reader(meta.StorageModeRedis)
}
func MongoReader(ctx context.Context) storage.DocumentLoader {
	return require(ctx).Reader(meta.StorageModeMongo)
}
func FusionReader(ctx context.Context) storage.DocumentLoader {
	return require(ctx).Reader(meta.StorageModeFusion)
}

// Writer 按 GD 元数据检查服务能力并写入，不代表已最终落盘。
func Writer(ctx context.Context) storage.ChangeApplier { return require(ctx).Writer() }

// Procedure 返回本服务共享的事务执行器，事务与锁范围仍由业务定义。
func Procedure(ctx context.Context) txn.Executor { return require(ctx).Procedure() }

// RedisClients 返回业务 Redis 命令入口，连接生命周期仍归宿主。
func RedisClients(ctx context.Context) goredis.ClientProvider { return require(ctx).RedisClients() }

// MongoBackfiller 只在记录不存在时插入，供建角与启动初始化使用。
func MongoBackfiller(ctx context.Context) storage.DocumentBackfiller {
	return require(ctx).MongoBackfiller()
}

// AppID 返回当前应用标识，不读取其他服务的模块状态。
func AppID(ctx context.Context) string { return require(ctx).AppID() }
