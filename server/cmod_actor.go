package server

import "context"

func (server *Server) actorCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModActor, Requires: []CModID{CModRPC, CModDiscovery}},
		validate: func(CModContext) error {
			_, err := server.config.Actor.Runtime.Normalize()
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
