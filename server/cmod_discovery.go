package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

func (server *Server) discoveryCMod(options RunOptions) CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModDiscovery},
		validate: func(CModContext) error {
			if _, err := server.config.Discovery.Etcd.Normalize(); err != nil {
				return fmt.Errorf("discovery config: %w", err)
			}
			if strings.TrimSpace(options.InstanceID) == "" {
				return errors.New("instance ID is required")
			}
			return nil
		},
		start: func(CModContext) error { return server.startDiscovery(options) },
		stop: func(ctx context.Context) error {
			if server.discovery == nil {
				return nil
			}
			return errors.Join(
				server.discovery.markStopping(context.WithoutCancel(ctx)),
				server.discovery.close(context.WithoutCancel(ctx)),
			)
		},
	}
}
