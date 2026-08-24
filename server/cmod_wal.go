package server

import "errors"

func (server *Server) walCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModWAL, Requires: []CModID{CModMongoStorage}},
		validate: func(CModContext) error {
			return server.config.Storage.WAL.validate(server.config.Storage.MongoDB)
		},
		start: func(CModContext) error {
			if server.runtime == nil || server.runtime.storage == nil {
				return errors.New("storage runtime is not configured")
			}
			return server.runtime.storage.startWAL(server.runtime.lifecycle, server.config.Storage.WAL, server.config.Storage.MongoDB)
		},
	}
}
