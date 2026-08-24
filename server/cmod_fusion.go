package server

import "errors"

func (server *Server) fusionCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModFusionStorage, Requires: []CModID{CModRedisStorage, CModMongoStorage}},
		validate: func(CModContext) error {
			if server.types == nil || server.bindStorage == nil {
				return errors.New("generated data binding is required")
			}
			return server.config.Runtime.DelayTask.validate()
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
	}
}
