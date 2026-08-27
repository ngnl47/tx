package framework

import (
	"context"

	txstorage "github.com/ngnl47/tx/storage"
)

func (server *Server) redisCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModRedisStorage},
		validate: func(CModContext) error {
			if err := validateStorageRoutes("redis", server.config.Storage.Redis); err != nil {
				return err
			}
			return server.config.Runtime.Health.validate()
		},
		start: func(CModContext) error {
			store := server.runtime.ensureStorage()
			if err := store.StartRedis(server.runtime.lifecycle, server.config.Storage.Redis); err != nil {
				return err
			}
			return startStorageHealth(server.runtime.lifecycle, store, server.config.Runtime.Health, txstorage.BackendRedis)
		},
		stop: func(context.Context) error {
			if server.runtime == nil || server.runtime.storage == nil {
				return nil
			}
			return server.runtime.storage.CloseRedis()
		},
	}
}
