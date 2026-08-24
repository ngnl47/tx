package server

import (
	"context"
	"errors"
)

func (server *Server) transactionCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModTransaction, Requires: []CModID{CModFusionStorage}},
		validate: func(CModContext) error {
			config := server.config.Runtime.Transaction.Runtime
			if config.FlushInterval <= 0 {
				return errors.New("transaction flush interval must be greater than 0")
			}
			if config.ShutdownFlushTimeout <= 0 {
				return errors.New("transaction shutdown flush timeout must be greater than 0")
			}
			return nil
		},
		start: func(CModContext) error { return server.startTransaction() },
		drain: func(ctx context.Context) error {
			if server.runtime == nil || server.runtime.transaction == nil {
				return nil
			}
			return server.runtime.transaction.Shutdown(context.WithoutCancel(ctx))
		},
	}
}
