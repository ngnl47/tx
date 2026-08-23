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

// controlServer 通过独立运维 endpoint 暴露进程排水操作。
// 它不是业务服务，也不会注册到服务发现。
type controlServer struct {
	listener net.Listener
	server   *grpc.Server
	stopOnce sync.Once
}

// controlServerOptions 声明同一 endpoint 暴露的运维接口。
// Lifecycle 必须提供；只有当前进程装配 discovery 后才能提供 Slots。
type controlServerOptions struct {
	Lifecycle control.LifecycleController
	Slots     control.SlotController
}

// startControlServer 启动可选的运维 gRPC 服务端。endpoint 为空表示项目部署禁用控制面。
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
