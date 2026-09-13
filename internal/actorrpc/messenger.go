package actorrpc

import (
	"context"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

// NATSMessenger 为实体间四种调用统一寻址；传输和稳定映射由组合根绑定。
// 首次寻址可能需要分配归属；传输失败不会再次寻址或重投。
// Agent 上行仍使用绑定 DirectTransport 的 RoutedCaller。
type NATSMessenger struct{ Router *EntityRouter }

// Tell 解析目标后提交到本地传输，不等待 NATS 确认或远端执行。
func (m *NATSMessenger) Tell(ctx context.Context, id actorhost.Identity, delivery *actorpb.Delivery) error {
	target, err := m.Router.Resolve(ctx, id)
	if err != nil {
		return err
	}
	return m.Router.Transport.Tell(ctx, target, delivery)
}

// TellAccepted 只等待 NATS 到达确认，不等待目标收取、业务执行或持久化。
func (m *NATSMessenger) TellAccepted(ctx context.Context, id actorhost.Identity, delivery *actorpb.Delivery) error {
	target, err := m.Router.Resolve(ctx, id)
	if err != nil {
		return err
	}
	return m.Router.Transport.TellAccepted(ctx, target, delivery)
}

// CallFuture 完成寻址后返回异步结果句柄；取消等待不撤销已接受工作。
func (m *NATSMessenger) CallFuture(ctx context.Context, id actorhost.Identity, delivery *actorpb.Delivery) (*ResultFuture, error) {
	target, err := m.Router.Resolve(ctx, id)
	if err != nil {
		return nil, err
	}
	return m.Router.Transport.CallFuture(ctx, target, delivery)
}

// Call 同步等待与 CallFuture 相同的结果协议。
func (m *NATSMessenger) Call(ctx context.Context, id actorhost.Identity, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	future, err := m.CallFuture(ctx, id, delivery)
	if err != nil {
		return nil, err
	}
	return future.Await(ctx)
}
