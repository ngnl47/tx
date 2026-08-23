package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/modules/control"
	"google.golang.org/grpc"
)

// controlServer exposes process drain operations on a separate operational
// endpoint. It is not a business service and is never registered in discovery.
type controlServer struct {
	listener net.Listener
	server   *grpc.Server
	stopOnce sync.Once
}

// controlServerOptions declares the operational surfaces exposed on one
// endpoint. Lifecycle is required; Slots is available only when discovery is
// assembled for this process.
type controlServerOptions struct {
	Lifecycle control.LifecycleController
	Slots     control.SlotController
}

// startControlServer starts the optional operational gRPC server. An empty
// endpoint means the control surface is disabled by the project deployment.
func startControlServer(root *lifecycle.WaitGroupContext, endpoint string, options controlServerOptions) (*controlServer, error) {
	if strings.TrimSpace(endpoint) == "" {
		return nil, nil
	}
	if root == nil {
		return nil, errors.New("server: lifecycle is not configured")
	}
	if options.Lifecycle == nil {
		return nil, control.ErrControllerNil
	}
	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("server: listen control %s: %w", endpoint, err)
	}
	server := grpc.NewServer()
	if err := control.RegisterLifecycleService(server, options.Lifecycle); err != nil {
		_ = listener.Close()
		return nil, err
	}
	if options.Slots != nil {
		if err := control.RegisterSlotService(server, options.Slots); err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	result := &controlServer{listener: listener, server: server}
	if err := root.Go(func(ctx context.Context) {
		stop := context.AfterFunc(ctx, result.stop)
		defer stop()
		if serveErr := server.Serve(listener); serveErr != nil && ctx.Err() == nil {
			txlog.Error("control gRPC service stopped unexpectedly", "err", serveErr)
		}
	}); err != nil {
		result.stop()
		return nil, err
	}
	return result, nil
}

func (s *controlServer) endpoint() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *controlServer) stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.server != nil {
			s.server.Stop()
		}
		if s.listener != nil {
			_ = s.listener.Close()
		}
	})
}
