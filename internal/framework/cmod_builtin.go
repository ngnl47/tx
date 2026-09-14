package framework

import (
	"context"
)

const (
	// CModTaskQueue 提供持久任务投递与可选消费。
	CModTaskQueue CModID = "task-queue"
	// CModRedisStorage 提供 Redis 文档读写与脚本路由能力。
	CModRedisStorage CModID = "redis-storage"
	// CModMongoStorage 提供 Mongo 文档读写能力。
	CModMongoStorage CModID = "mongo-storage"
	// CModFusionStorage 组合 Redis 热数据、Mongo 冷数据与延迟回写能力。
	CModFusionStorage CModID = "fusion-storage"
	// CModRPC 提供当前进程共享的 gRPC Host。
	CModRPC CModID = "rpc"
	// CModDiscovery 提供 etcd 注册、发现和稳定槽位目录。
	CModDiscovery CModID = "discovery"
	// CModSessionID 提供绑定 Discovery 运行身份的 SessionID 分配能力。
	CModSessionID CModID = "session-id"
	// CModActor 提供基于服务发现的 Actor 投递能力。
	CModActor CModID = "actor"
)

type builtInCMod struct {
	spec     CModSpec
	validate func(CModContext) error
	start    func(CModContext) error
	drain    func(context.Context) error
	stop     func(context.Context) error
}

func (mod *builtInCMod) Spec() CModSpec { return mod.spec }

func (mod *builtInCMod) Validate(ctx CModContext) error {
	if mod.validate == nil {
		return nil
	}
	return mod.validate(ctx)
}

func (mod *builtInCMod) Start(ctx CModContext) error {
	if mod.start == nil {
		return nil
	}
	return mod.start(ctx)
}

func (mod *builtInCMod) Drain(ctx context.Context) error {
	if mod.drain == nil {
		return nil
	}
	return mod.drain(ctx)
}

func (mod *builtInCMod) Stop(ctx context.Context) error {
	if mod.stop == nil {
		return nil
	}
	return mod.stop(ctx)
}

func (server *Server) builtInCMods(options RunOptions) []CMod {
	return []CMod{
		server.redisCMod(),
		server.mongoCMod(),
		server.fusionCMod(),
		server.rpcCMod(options),
		server.discoveryCMod(),
		server.sessionIDCMod(),
		server.actorCMod(),
		server.tasksCMod(),
	}
}
