package server

import (
	"context"
	"errors"
)

func (server *Server) fusionCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModFusionStorage, Requires: []CModID{CModRedisStorage, CModMongoStorage}},
		validate: func(CModContext) error {
			if server.types == nil || server.bindStorage == nil {
				return errors.New("generated data binding is required")
			}
			if err := server.config.Runtime.DelayTask.validate(); err != nil {
				return err
			}
			return server.config.Runtime.Fusion.Validate(server.types)
		},
		start: func(CModContext) error {
			store := server.runtime.storage
			if store == nil {
				return errors.New("storage runtime is not configured")
			}
			if err := store.startFusion(server.runtime.lifecycle, server.config.Storage.Redis, server.config.Runtime.storageOptions(), server.types); err != nil {
				return err
			}
			return store.startDelayTaskDispatcher(server.runtime.lifecycle)
		},
		drain: func(ctx context.Context) error {
			if server.runtime == nil || server.runtime.storage == nil {
				return nil
			}
			drainCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
			defer cancel()
			return server.runtime.storage.drainDelayTaskDispatcher(drainCtx)
		},
	}
}
