package server

import (
	"context"

	txstorage "github.com/ngnl47/tx/storage"
)

func (server *Server) mongoCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModMongoStorage},
		validate: func(CModContext) error {
			if err := validateStorageRoutes("mongo", server.config.Storage.MongoDB); err != nil {
				return err
			}
			return server.config.Runtime.Health.validate()
		},
		start: func(CModContext) error {
			store := server.runtime.ensureStorage()
			if err := store.startMongo(server.runtime.lifecycle, server.config.Storage.MongoDB); err != nil {
				return err
			}
			if err := store.initializeMongoIndexes(server.runtime.lifecycle, server.config.Storage.MongoDB, server.types); err != nil {
				return err
			}
			return startStorageHealth(server.runtime.lifecycle, store, server.config.Runtime.Health, txstorage.BackendMongo)
		},
		stop: func(ctx context.Context) error {
			if server.runtime == nil || server.runtime.storage == nil {
				return nil
			}
			return server.runtime.storage.closeMongo(ctx)
		},
	}
}
