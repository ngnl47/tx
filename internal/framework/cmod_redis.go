package framework

import (
	"context"
	"fmt"

	txstorage "github.com/ngnl47/tx/storage"
)

func (server *Server) redisCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModRedisStorage},
		validate: func(CModContext) error {
			if len(server.config.Storage.Redis.Stores) == 0 {
				return fmt.Errorf("framework: redis stores are required")
			}
			if err := server.config.Storage.Redis.Validate(); err != nil {
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
