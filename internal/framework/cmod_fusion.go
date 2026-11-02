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
			if server.types == nil {
				return errors.New("generated type registry is required")
			}
			if err := server.config.Runtime.DelayTask.validate(); err != nil {
				return err
			}
			if err := server.config.Runtime.Scheduler.validate(); err != nil {
				return err
			}
			return server.config.Runtime.Fusion.Validate(server.types)
		},
		start: func(c CModContext) error {
			store := server.runtime.storage
			if store == nil {
				return errors.New("storage runtime is not configured")
			}
			if err := store.StartFusion(c.Tasks(), server.config.Runtime.storageOptions(), server.types); err != nil {
				return err
			}
			if err := store.StartDelayTaskDispatcher(c.Tasks()); err != nil {
				return err
			}
			if err := store.PrepareDelayScheduler(server.config.Runtime.storageOptions()); err != nil {
				return err
			}
			return store.StartDelayScheduler(c.Tasks())
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
