package actor

import (
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
)

type idlePassivationActor struct {
	inner        protoactor.Actor
	timeout      time.Duration
	timer        *time.Timer
	lastActivity time.Time
}

type idlePassivationTick struct{}

func (idlePassivationTick) NotInfluenceReceiveTimeout() {}

func idlePassivationProducer(producer func() protoactor.Actor, timeout time.Duration) func() protoactor.Actor {
	return func() protoactor.Actor {
		return &idlePassivationActor{inner: producer(), timeout: timeout}
	}
}

func (actor *idlePassivationActor) Receive(ctx protoactor.Context) {
	switch ctx.Message().(type) {
	case *protoactor.Started:
		actor.inner.Receive(ctx)
		actor.start(ctx)
	case idlePassivationTick:
		actor.passivateIfIdle(ctx)
	case *protoactor.ReceiveTimeout:
		actor.inner.Receive(ctx)
	case *protoactor.Restarting, *protoactor.Stopping, *protoactor.Stopped:
		actor.stopTimer()
		actor.inner.Receive(ctx)
	default:
		actor.inner.Receive(ctx)
		switch ctx.Message().(type) {
		case protoactor.NotInfluenceReceiveTimeout, protoactor.SystemMessage, protoactor.AutoReceiveMessage:
			return
		}
		actor.touch()
	}
}

func (actor *idlePassivationActor) start(ctx protoactor.Context) {
	actor.lastActivity = time.Now()
	system := ctx.ActorSystem()
	self := ctx.Self()
	actor.timer = time.AfterFunc(actor.timeout, func() {
		system.Root.Send(self, idlePassivationTick{})
	})
}

func (actor *idlePassivationActor) touch() {
	if actor.timer == nil {
		return
	}
	actor.lastActivity = time.Now()
	actor.timer.Reset(actor.timeout)
}

func (actor *idlePassivationActor) passivateIfIdle(ctx protoactor.Context) {
	if actor.timer == nil {
		return
	}
	remaining := actor.timeout - time.Since(actor.lastActivity)
	if remaining > 0 {
		actor.timer.Reset(remaining)
		return
	}
	actor.stopTimer()
	ctx.Poison(ctx.Self())
}

func (actor *idlePassivationActor) stopTimer() {
	if actor.timer == nil {
		return
	}
	actor.timer.Stop()
	actor.timer = nil
}
