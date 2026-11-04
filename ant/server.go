package ant

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/lifecycle"

	framework "github.com/ngnl47/tx/internal/framework"
)

// Server 持有 Ant 有状态进程的能力与服务生命周期。
type Server struct {
	kernel *framework.Server
}

// Start 装配能力并启动选中的服务；ctx 约束启动钩子，后台任务由宿主另行管理。
// 返回成功表示选中服务启动、业务准入与可靠任务消费接线完成，不表示所有 Actor 已加载。
// 启动失败会回滚已取得资源；清理尚未完成时保留依赖，调用方应继续调用 Stop 收尾。
func (server *Server) Start(ctx context.Context, options RunOptions) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Start(ctx, options.toFramework())
}

// Run 启动服务，等待 ctx 取消或框架致命错误触发的生命周期取消，然后执行 Stop。
// 收尾尚未完成时，在首次 Stop 后最多重试三次；启动回滚未完成也进入此收尾流程。
// 重试耗尽返回错误，由进程入口非零退出并交由运维处置。
func (server *Server) Run(ctx context.Context, options RunOptions) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Run(ctx, options.toFramework())
}

// Drain 关闭服务外部准入，排空已领取的可靠任务，再关闭 Actor 准入并等待已接纳工作完成。
// 它不完成后台任务退出、最终写回或资源释放；完整关闭使用 Stop，槽位交接使用 PrepareHandoff。
func (server *Server) Drain(ctx context.Context) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Drain(ctx)
}

// PrepareHandoff 完成已接受工作与最终写回，保留资源供槽位交接；源身份必须属于本次启动。
func (server *Server) PrepareHandoff(ctx context.Context, expectedInstanceID string) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.PrepareHandoff(ctx, expectedInstanceID)
}

// Stop 执行排水、任务退出、必要写回与资源释放；正常收尾成功后条件释放本实例槽位。
// 常规排水、写回或任务退出尚未完成时保留所需资源并返回错误，允许再次调用。
// ctx 取消只限制本次等待，不证明后台工作已经退出；有限重试与进程生命周期编排使用 Run。
func (server *Server) Stop(ctx context.Context) error {
	if server == nil || server.kernel == nil {
		return errors.New("ant: server is nil")
	}
	return server.kernel.Stop(ctx)
}

// ServiceStatus 查询选中服务的阶段；停止后仍保留失败信息。
func (server *Server) ServiceStatus(name string) (lifecycle.ServiceStatus, bool) {
	if server == nil || server.kernel == nil {
		return lifecycle.ServiceStatus{}, false
	}
	return server.kernel.ServiceStatus(name)
}
