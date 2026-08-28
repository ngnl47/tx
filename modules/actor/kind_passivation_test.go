package actor

import (
	"sync/atomic"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
)

func TestKindRejectsInvalidIdlePassivation(t *testing.T) {
	if _, err := NewKind("role", func() protoactor.Actor { return fixedHostProbeActor{} }, WithIdlePassivation(0)); err == nil {
		t.Fatal("NewKind accepted zero idle passivation")
	}
	if _, err := NewFixedKind("role", func() protoactor.Actor { return fixedHostProbeActor{} }, WithIdlePassivation(-time.Second)); err == nil {
		t.Fatal("NewFixedKind accepted negative idle passivation")
	}
}

func TestIdlePassivationRestartsAfterUserMessage(t *testing.T) {
	const idleTimeout = 80 * time.Millisecond
	var stopped atomic.Int32
	kind, err := NewKind("role", func() protoactor.Actor {
		return passivationProbeActor{stopped: &stopped}
	}, WithIdlePassivation(idleTimeout))
	if err != nil {
		t.Fatalf("NewKind returned error: %v", err)
	}
	system := protoactor.NewActorSystem()
	pid := system.Root.Spawn(kind.props)
	t.Cleanup(func() { system.Root.Stop(pid) })

	time.Sleep(idleTimeout / 2)
	system.Root.Send(pid, "keep-alive")
	time.Sleep(idleTimeout * 3 / 4)
	if got := stopped.Load(); got != 0 {
		t.Fatalf("Actor stopped %d times before the renewed idle timeout", got)
	}
	waitForAtomicValue(t, &stopped, 1, 3*idleTimeout)
}

func TestIdlePassivationPreservesInnerReceiveTimeout(t *testing.T) {
	const idleTimeout = 120 * time.Millisecond
	var receiveTimeouts atomic.Int32
	var stopped atomic.Int32
	kind, err := NewKind("role", func() protoactor.Actor {
		return ownReceiveTimeoutActor{receiveTimeouts: &receiveTimeouts, stopped: &stopped}
	}, WithIdlePassivation(idleTimeout))
	if err != nil {
		t.Fatalf("NewKind returned error: %v", err)
	}
	system := protoactor.NewActorSystem()
	pid := system.Root.Spawn(kind.props)
	t.Cleanup(func() { system.Root.Stop(pid) })

	waitForAtomicValue(t, &receiveTimeouts, 1, idleTimeout/2)
	if got := stopped.Load(); got != 0 {
		t.Fatalf("Actor stopped %d times before its idle timeout", got)
	}
	waitForAtomicValue(t, &stopped, 1, 3*idleTimeout)
}

type passivationProbeActor struct {
	stopped *atomic.Int32
}

func (actor passivationProbeActor) Receive(ctx protoactor.Context) {
	if _, ok := ctx.Message().(*protoactor.Stopped); ok {
		actor.stopped.Add(1)
	}
}

type ownReceiveTimeoutActor struct {
	receiveTimeouts *atomic.Int32
	stopped         *atomic.Int32
}

func (actor ownReceiveTimeoutActor) Receive(ctx protoactor.Context) {
	switch ctx.Message().(type) {
	case *protoactor.Started:
		ctx.SetReceiveTimeout(20 * time.Millisecond)
	case *protoactor.ReceiveTimeout:
		actor.receiveTimeouts.Add(1)
		ctx.CancelReceiveTimeout()
	case *protoactor.Stopped:
		actor.stopped.Add(1)
	}
}

func waitForAtomicValue(t *testing.T, value *atomic.Int32, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if value.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("atomic value = %d, want %d", value.Load(), want)
}
