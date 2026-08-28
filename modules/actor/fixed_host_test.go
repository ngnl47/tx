package actor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/ngnl47/tx/lifecycle"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

func TestFixedHostReusesEntityActorByRouteKey(t *testing.T) {
	var starts atomic.Int32
	childProps := protoactor.PropsFromProducer(func() protoactor.Actor {
		starts.Add(1)
		return fixedHostProbeActor{}
	})
	hostProps, err := newFixedHostProps("role", childProps)
	if err != nil {
		t.Fatalf("newFixedHostProps() error = %v", err)
	}
	system := protoactor.NewActorSystem()
	host := system.Root.Spawn(hostProps)
	t.Cleanup(func() { system.Root.Stop(host) })

	first := requestFixedHostProbe(t, system, host, actorpb.RouteKeyKind_ROUTE_KEY_UINT64, 470001, "")
	second := requestFixedHostProbe(t, system, host, actorpb.RouteKeyKind_ROUTE_KEY_UINT64, 470001, "")
	third := requestFixedHostProbe(t, system, host, actorpb.RouteKeyKind_ROUTE_KEY_UINT64, 470002, "")

	if first != second {
		t.Fatalf("same route key child = %q, want %q", second, first)
	}
	if first == third {
		t.Fatalf("different route keys shared child %q", first)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("child starts = %d, want 2", got)
	}
}

func TestFixedHostRecreatesIdleEntityActor(t *testing.T) {
	const idleTimeout = 30 * time.Millisecond
	var starts atomic.Int32
	kind, err := NewFixedKind("role", func() protoactor.Actor {
		starts.Add(1)
		return fixedHostProbeActor{}
	}, WithIdlePassivation(idleTimeout))
	if err != nil {
		t.Fatalf("NewFixedKind returned error: %v", err)
	}
	hostProps, err := newFixedHostProps("role", kind.props)
	if err != nil {
		t.Fatalf("newFixedHostProps returned error: %v", err)
	}

	system := protoactor.NewActorSystem()
	host := system.Root.Spawn(hostProps)
	t.Cleanup(func() { system.Root.Stop(host) })

	first := requestFixedHostProbe(t, system, host, actorpb.RouteKeyKind_ROUTE_KEY_UINT64, 470001, "")
	waitForAtomicValue(t, &starts, 1, time.Second)
	time.Sleep(3 * idleTimeout)
	second := requestFixedHostProbe(t, system, host, actorpb.RouteKeyKind_ROUTE_KEY_UINT64, 470001, "")

	if first == second {
		t.Fatalf("idle route key reused stopped child %q", first)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("child starts = %d, want 2", got)
	}
}

func TestFixedHostUsesRuntimeIdlePassivationForKind(t *testing.T) {
	const idleTimeout = 30 * time.Millisecond
	var starts atomic.Int32
	kind, err := NewFixedKind("role", func() protoactor.Actor {
		starts.Add(1)
		return fixedHostProbeActor{}
	})
	if err != nil {
		t.Fatalf("NewFixedKind returned error: %v", err)
	}
	mailboxes := &mailboxLifecycle{}
	mailboxes.bind(lifecycle.NewWaitGroupContext(context.Background()))
	_, fixed, err := configureKinds(Config{
		ClusterName:     "test",
		Host:            "127.0.0.1",
		MailboxCapacity: 8,
		IdlePassivation: map[string]time.Duration{"role": idleTimeout},
	}, []Kind{kind}, mailboxes)
	if err != nil {
		t.Fatalf("configureKinds returned error: %v", err)
	}
	hostProps, err := newFixedHostProps("role", fixed[0].props)
	if err != nil {
		t.Fatalf("newFixedHostProps returned error: %v", err)
	}

	system := protoactor.NewActorSystem()
	host := system.Root.Spawn(hostProps)
	t.Cleanup(func() { system.Root.Stop(host) })

	first := requestFixedHostProbe(t, system, host, actorpb.RouteKeyKind_ROUTE_KEY_UINT64, 470001, "")
	time.Sleep(3 * idleTimeout)
	second := requestFixedHostProbe(t, system, host, actorpb.RouteKeyKind_ROUTE_KEY_UINT64, 470001, "")
	if first == second || starts.Load() != 2 {
		t.Fatalf("runtime idle passivation children=(%q,%q) starts=%d, want two children", first, second, starts.Load())
	}
}

func TestFixedKindPIDUsesStableKindHost(t *testing.T) {
	first, err := FixedKindPID("127.0.0.1:9201", "role")
	if err != nil {
		t.Fatalf("FixedKindPID() error = %v", err)
	}
	second, err := FixedKindPID("127.0.0.1:9201", "role")
	if err != nil {
		t.Fatalf("FixedKindPID() second error = %v", err)
	}
	other, err := FixedKindPID("127.0.0.1:9201", "world")
	if err != nil {
		t.Fatalf("FixedKindPID() other error = %v", err)
	}
	if first.Address != second.Address || first.Id != second.Id {
		t.Fatalf("same kind PID = %v and %v", first, second)
	}
	if first.Id == other.Id {
		t.Fatalf("different kinds shared host ID %q", first.Id)
	}
}

func TestFixedHostDoesNotActivateEntityAfterDrain(t *testing.T) {
	var starts atomic.Int32
	childProps := protoactor.PropsFromProducer(func() protoactor.Actor {
		starts.Add(1)
		return fixedHostProbeActor{}
	})
	hostProps, err := newFixedHostProps("role", childProps)
	if err != nil {
		t.Fatalf("newFixedHostProps() error = %v", err)
	}
	mailboxes := &mailboxLifecycle{}
	mailboxes.bind(lifecycle.NewWaitGroupContext(context.Background()))
	mailboxes.beginDrain()
	hostProps = hostProps.Clone(protoactor.WithMailbox(observedBoundedMailbox(Config{
		ClusterName:     "test",
		Host:            "127.0.0.1",
		MailboxCapacity: 8,
	}, "role.host", mailboxes)))
	system := protoactor.NewActorSystem()
	host := system.Root.Spawn(hostProps)
	t.Cleanup(func() { system.Root.Stop(host) })

	system.Root.Send(host, &actorpb.Delivery{
		MessageType:     "test.command",
		CommandId:       1,
		RouteKeyKind:    actorpb.RouteKeyKind_ROUTE_KEY_UINT64,
		RouteKeyNumeric: 470001,
	})
	time.Sleep(20 * time.Millisecond)

	if got := starts.Load(); got != 0 {
		t.Fatalf("child starts after drain = %d, want 0", got)
	}
	if got := mailboxes.pending(); got != 0 {
		t.Fatalf("pending deliveries after drain = %d, want 0", got)
	}
}

type fixedHostProbeActor struct{}

func (fixedHostProbeActor) Receive(ctx protoactor.Context) {
	if _, ok := ctx.Message().(*actorpb.Delivery); ok {
		ctx.Respond(ctx.Self().Id)
	}
}

func requestFixedHostProbe(
	t *testing.T,
	system *protoactor.ActorSystem,
	host *protoactor.PID,
	kind actorpb.RouteKeyKind,
	numeric uint64,
	text string,
) string {
	t.Helper()
	result, err := system.Root.RequestFuture(host, &actorpb.Delivery{
		MessageType:     "test.command",
		CommandId:       1,
		RouteKeyKind:    kind,
		RouteKeyNumeric: numeric,
		RouteKeyString:  text,
	}, time.Second).Result()
	if err != nil {
		t.Fatalf("request fixed host: %v", err)
	}
	child, ok := result.(string)
	if !ok {
		t.Fatalf("fixed host response = %T, want string", result)
	}
	return child
}
