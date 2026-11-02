package framework

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
		start: func(c CModContext) error {
			store := server.runtime.ensureStorage()
			if err := store.StartMongo(c.Tasks(), server.config.Storage.MongoDB); err != nil {
				return err
			}
			if err := store.InitializeMongoIndexes(c.Tasks(), server.config.Storage.MongoDB, server.types); err != nil {
				return err
			}
			return startStorageHealth(c.Tasks(), store, server.config.Runtime.Health, txstorage.BackendMongo)
		},
		stop: func(ctx context.Context) error {
			if server.runtime == nil || server.runtime.storage == nil {
				return nil
			}
			return server.runtime.storage.CloseMongo(ctx)
		},
	}
}
