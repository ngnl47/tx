package actor

import (
	"context"
	"testing"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/ngnl47/tx/lifecycle"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

type actorSystemContextStub struct {
	system *protoactor.ActorSystem
}

func (stub actorSystemContextStub) ActorSystem() *protoactor.ActorSystem {
	return stub.system
}

func TestLifecycleContextReadsActorSystemExtension(t *testing.T) {
	system := protoactor.NewActorSystem()
	t.Cleanup(system.Shutdown)
	want := context.WithValue(context.Background(), actorLifecycleContextKey{}, "tx")
	system.Extensions.Register(&actorLifecycleExtension{context: want})
	if got := LifecycleContext(actorSystemContextStub{system: system}); got != want {
		t.Fatalf("LifecycleContext() = %p, want %p", got, want)
	}
}

func BenchmarkDeliveryLifecycleAccounting(b *testing.B) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	accounting := &mailboxLifecycle{}
	accounting.bind(root)
	message := &actorpb.Delivery{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !tracksLifecycle(message) {
			b.Fatal("Delivery was not tracked")
		}
		if err := accounting.begin(); err != nil {
			b.Fatal(err)
		}
		accounting.end()
	}
}

type actorLifecycleContextKey struct{}
