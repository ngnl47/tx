package framework

import (
	"context"
	"errors"
)

func (server *Server) sessionIDCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModSessionID, Requires: []CModID{CModDiscovery}},
		start: func(CModContext) error {
			if server == nil || server.discovery == nil || server.runtime == nil {
				return ErrSessionIDUnavailable
			}
			runtime, err := newSessionIDRuntime(server.discovery)
			if err != nil {
				return errors.Join(ErrSessionIDUnavailable, err)
			}
			server.runtime.setSessionIDs(runtime)
			return nil
		},
		drain: func(context.Context) error {
			if server != nil && server.runtime != nil {
				server.runtime.closeSessionIDs()
			}
			return nil
		},
		stop: func(context.Context) error {
			if server != nil && server.runtime != nil {
				server.runtime.clearSessionIDs()
			}
			return nil
		},
	}
}
