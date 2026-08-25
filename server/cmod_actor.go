package server

import (
	"context"

	txactor "github.com/ngnl47/tx/modules/actor"
)

func (server *Server) actorCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModActor},
		validate: func(CModContext) error {
			_, err := txactor.New(server.config.Actor.Runtime, server.runtime.actorKinds...)
			return err
		},
		start: func(CModContext) error { return server.startActor() },
		stop: func(context.Context) error {
			if server.runtime == nil || server.runtime.actor == nil {
				return nil
			}
			return server.runtime.actor.Shutdown()
		},
	}
}
