package framework

import (
	"context"
	"fmt"
)

func (server *Server) discoveryCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModDiscovery, After: []CModID{CModRPC, CModActor}},
		validate: func(CModContext) error {
			if _, err := server.config.Discovery.Etcd.Normalize(); err != nil {
				return fmt.Errorf("discovery config: %w", err)
			}
			return nil
		},
		start: func(CModContext) error { return server.startDiscovery() },
		stop: func(ctx context.Context) error {
			if server.discovery == nil {
				return nil
			}
			return server.discovery.Close(context.WithoutCancel(ctx))
		},
	}
}
