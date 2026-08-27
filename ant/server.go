package ant

import (
	"context"
	"errors"

	framework "github.com/ngnl47/tx/internal/framework"
)

// Server 持有 Ant 有状态进程的能力与服务生命周期。
type Server struct {
	kernel *framework.Server
}

// Start 装配能力并启动选中的服务。
func (server *Server) Start(ctx context.Context, options RunOptions) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Start(ctx, options.toFramework())
}

// Run 启动服务、等待 ctx 取消，并执行完整排水和停止流程。
func (server *Server) Run(ctx context.Context, options RunOptions) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Run(ctx, options.toFramework())
}

// Drain 停止接收新的外部工作并等待已接纳工作完成。
func (server *Server) Drain(ctx context.Context) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Drain(ctx)
}

// Stop 执行排水并释放全部物理资源。
func (server *Server) Stop(ctx context.Context) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Stop(ctx)
}
