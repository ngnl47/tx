package framework

import (
	"context"
	"github.com/ngnl47/tx/internal/actorresult"
	"github.com/ngnl47/tx/internal/actorrpc"
	txactor "github.com/ngnl47/tx/modules/actor"
	"time"
)

// actorAccess 隔离业务可用的四种调用与框架持有的连接、宿主关闭权限。
type actorAccess struct {
	messenger    *actorrpc.NATSMessenger
	routeTimeout time.Duration
}

func (a actorAccess) Tell(ctx context.Context, id txactor.Identity, d txactor.Delivery) error {
	// 首次选址需要有限控制查询，不把这个期限传给业务初始化。
	ctx, cancel := context.WithTimeout(ctx, a.routeTimeout)
	defer cancel()
	msg, err := d.ToProto()
	if err != nil {
		return err
	}
	return a.messenger.Tell(ctx, id, msg)
}
func (a actorAccess) TellAccepted(ctx context.Context, id txactor.Identity, d txactor.Delivery) error {
	msg, err := d.ToProto()
	if err != nil {
		return err
	}
	return a.messenger.TellAccepted(ctx, id, msg)
}
func (a actorAccess) CallFuture(ctx context.Context, id txactor.Identity, d txactor.Delivery) (*txactor.Future, error) {
	msg, err := d.ToProto()
	if err != nil {
		return nil, err
	}
	f, err := a.messenger.CallFuture(ctx, id, msg)
	if err != nil {
		return nil, err
	}
	return actorresult.WrapFuture(f), nil
}
func (a actorAccess) Call(ctx context.Context, id txactor.Identity, d txactor.Delivery) (txactor.Result, error) {
	f, err := a.CallFuture(ctx, id, d)
	if err != nil {
		return txactor.Result{}, err
	}
	return f.Await(ctx)
}

// directActorAccess 仅供直连调用，不能通过这个视图切换到 NATS。
type directActorAccess struct{ caller *actorrpc.RoutedCaller }

func (a directActorAccess) Call(ctx context.Context, id txactor.Identity, d txactor.Delivery) (txactor.Result, error) {
	msg, err := d.ToProto()
	if err != nil {
		return txactor.Result{}, err
	}
	result, err := a.caller.Call(ctx, id, msg)
	return actorresult.FromProto(result, err)
}

func (a directActorAccess) Enqueue(ctx context.Context, id txactor.Identity, d txactor.Delivery) error {
	msg, err := d.ToProto()
	if err != nil {
		return err
	}
	return a.caller.Enqueue(ctx, id, msg)
}

// 激活控制是低频请求，使用有限查询期限；不传递给异步业务初始化。
func (a actorAccess) Probe(ctx context.Context, id txactor.Identity) (txactor.ActivationStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, a.routeTimeout)
	defer cancel()
	return a.messenger.Router.Probe(ctx, id)
}
func (a actorAccess) EnsureActivation(ctx context.Context, id txactor.Identity) (txactor.ActivationStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, a.routeTimeout)
	defer cancel()
	_, state, err := a.messenger.Router.Ensure(ctx, id)
	return state, err
}
func (a actorAccess) RetryActivation(ctx context.Context, id txactor.Identity) (txactor.ActivationStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, a.routeTimeout)
	defer cancel()
	_, state, err := a.messenger.Router.Retry(ctx, id)
	return state, err
}
