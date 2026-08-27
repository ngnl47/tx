package framework

import (
	"context"
	"errors"
)

func (server *Server) fusionCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{
			ID:       CModFusionStorage,
			Requires: []CModID{CModRedisStorage, CModMongoStorage},
			Components: []ComponentSpec{
				{Role: "scheduler", Name: "delay"},
			},
		},
		validate: func(CModContext) error {
			if server.types == nil || server.bindStorage == nil {
				return errors.New("generated data binding is required")
			}
			if err := server.config.Runtime.DelayTask.validate(); err != nil {
				return err
			}
			if err := server.config.Runtime.Scheduler.validate(); err != nil {
				return err
			}
			return server.config.Runtime.Fusion.Validate(server.types)
		},
		start: func(CModContext) error {
			store := server.runtime.storage
			if store == nil {
				return errors.New("storage runtime is not configured")
			}
			if err := store.StartFusion(server.runtime.lifecycle, server.config.Storage.Redis, server.config.Runtime.storageOptions(), server.types); err != nil {
				return err
			}
			if err := store.StartDelayTaskDispatcher(server.runtime.lifecycle); err != nil {
				return err
			}
			if err := store.PrepareDelayScheduler(server.config.Runtime.storageOptions()); err != nil {
				return err
			}
			return store.StartDelayScheduler(server.runtime.lifecycle)
		},
		drain: func(ctx context.Context) error {
			if server.runtime == nil || server.runtime.storage == nil {
				return nil
			}
			drainCtx, cancel := context.WithTimeout(ctx, effectiveDrainTimeout(server.config.Runtime.Lifecycle.DrainTimeout))
			defer cancel()
			return server.runtime.storage.DrainDelayTaskDispatcher(drainCtx)
		},
	}
}
