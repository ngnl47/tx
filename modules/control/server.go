package control

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"google.golang.org/grpc"
)

// ServerOptions 声明同一控制端口暴露的运维接口。
// Lifecycle 必须提供；Handoff、Slots 和只读 StorageIO 按宿主能力选择提供。
type ServerOptions struct {
	Lifecycle LifecycleController
	Handoff   HandoffController
	Slots     SlotController
	StorageIO StorageIOProvider
}

// Server 在独立端口暴露进程控制面。它不承载业务协议，也不注册到服务发现。
type Server struct {
	listener net.Listener
	grpc     *grpc.Server
	stopOnce sync.Once
}

// StartServer 启动控制面 gRPC 服务。endpoint 为空表示禁用控制面。
// 服务任务登记到 tasks，进程生命周期取消时会同步关闭监听端口。
func StartServer(tasks lifecycle.Tasks, endpoint string, options ServerOptions) (*Server, error) {
	if strings.TrimSpace(endpoint) == "" {
		return nil, nil
	}
	if tasks == nil {
		return nil, lifecycle.ErrStopping
	}
	if options.Lifecycle == nil {
		return nil, ErrControllerNil
	}

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("control: listen %s: %w", endpoint, err)
	}
	grpcServer := grpc.NewServer()
	if options.Handoff != nil {
		if err := RegisterHandoffService(grpcServer, options.Handoff); err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	if options.StorageIO != nil {
		if err := RegisterStorageIOService(grpcServer, options.StorageIO); err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	if err := RegisterLifecycleService(grpcServer, options.Lifecycle); err != nil {
		_ = listener.Close()
		return nil, err
	}
	if options.Slots != nil {
		if err := RegisterSlotService(grpcServer, options.Slots); err != nil {
			_ = listener.Close()
			return nil, err
		}
	}

	server := &Server{listener: listener, grpc: grpcServer}
	if err := tasks.Go(func(ctx context.Context) {
		stop := context.AfterFunc(ctx, server.Stop)
		defer stop()
		if serveErr := grpcServer.Serve(listener); serveErr != nil && ctx.Err() == nil {
			txlog.Error("control gRPC service stopped unexpectedly", "err", serveErr)
		}
	}); err != nil {
		server.Stop()
		return nil, err
	}
	return server, nil
}

// Endpoint 返回控制面实际监听地址；Server 为空时返回空字符串。
func (server *Server) Endpoint() string {
	if server == nil || server.listener == nil {
		return ""
	}
	return server.listener.Addr().String()
}

// Stop 立即停止控制端口。该方法可以并发、重复调用。
func (server *Server) Stop() {
	if server == nil {
		return
	}
	server.stopOnce.Do(func() {
		if server.grpc != nil {
			server.grpc.Stop()
		}
		if server.listener != nil {
			_ = server.listener.Close()
		}
	})
}
