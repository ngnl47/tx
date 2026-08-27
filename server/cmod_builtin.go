package server

import (
	"context"
)

const (
	CModRedisStorage  CModID = "redis-storage"
	CModMongoStorage  CModID = "mongo-storage"
	CModFusionStorage CModID = "fusion-storage"
	CModRPC           CModID = "rpc"
	CModDiscovery     CModID = "discovery"
	CModActor         CModID = "actor"
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
		server.discoveryCMod(options),
		server.actorCMod(),
	}
}
