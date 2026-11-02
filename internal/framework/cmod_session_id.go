package framework

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/sessionid"
)

func (server *Server) sessionIDCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModSessionID, Requires: []CModID{CModDiscovery}},
		start: func(CModContext) error {
			if server == nil || server.discovery == nil || server.runtime == nil {
				return ErrSessionIDUnavailable
			}
			allocator, err := sessionid.NewEtcdAllocator(server.discovery.client, server.discovery.sessionIDKey)
			if err != nil {
				return errors.Join(ErrSessionIDUnavailable, err)
			}
			server.runtime.setSessionIDs(newSessionIDRuntime(allocator, server.discovery))
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
