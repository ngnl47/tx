package framework

import (
	"context"
	"strings"

	txrpc "github.com/ngnl47/tx/modules/rpc"
)

func (server *Server) rpcCMod(options RunOptions) CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModRPC},
		start: func(CModContext) error {
			listen := strings.TrimSpace(options.RPCListen)
			if listen == "" {
				listen = strings.TrimSpace(options.InstanceEndpoint)
			}
			if listen == "" {
				listen = "127.0.0.1:0"
			}
			host, err := txrpc.NewHost(listen)
			if err != nil {
				return err
			}
			server.runtime.rpc = host
			if strings.TrimSpace(options.InstanceEndpoint) == "" {
				server.runtime.setInstanceEndpoint(host.Endpoint())
			}
			return nil
		},
		stop: func(ctx context.Context) error {
			if server.runtime == nil || server.runtime.rpc == nil {
				return nil
			}
			return server.runtime.rpc.Stop(ctx)
		},
	}
}
