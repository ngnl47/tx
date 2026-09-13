package actor

import (
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	clusterTest "github.com/asynkron/protoactor-go/cluster/clusterproviders/test"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/disthash"
	"github.com/asynkron/protoactor-go/remote"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

func BenchmarkVirtualPIDExisting(b *testing.B) {
	b.Run("Serial", func(b *testing.B) {
		runtime := newVirtualBenchmarkRuntime(b)
		if _, err := runtime.VirtualPID("role", "role-470001"); err != nil {
			b.Fatalf("prewarm VirtualPID() error = %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if _, err := runtime.VirtualPID("role", "role-470001"); err != nil {
				b.Fatalf("VirtualPID() error = %v", err)
			}
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		runtime := newVirtualBenchmarkRuntime(b)
		if _, err := runtime.VirtualPID("role", "role-470001"); err != nil {
			b.Fatalf("prewarm VirtualPID() error = %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := runtime.VirtualPID("role", "role-470001"); err != nil {
					b.Errorf("VirtualPID() error = %v", err)
					return
				}
			}
		})
	})
}

func BenchmarkVirtualPIDColdActivation(b *testing.B) {
	b.Run("Serial", func(b *testing.B) {
		runtime := newVirtualBenchmarkRuntime(b)
		identities := benchmarkIdentities(b.N)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := runtime.VirtualPID("role", identities[i]); err != nil {
				b.Fatalf("VirtualPID() error = %v", err)
			}
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		runtime := newVirtualBenchmarkRuntime(b)
		identities := benchmarkIdentities(b.N)
		var next atomic.Uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				index := int(next.Add(1) - 1)
				if _, err := runtime.VirtualPID("role", identities[index]); err != nil {
					b.Errorf("VirtualPID() error = %v", err)
					return
				}
			}
		})
	})
}

func BenchmarkFixedActorExisting(b *testing.B) {
	b.Run("Serial", func(b *testing.B) {
		system, host := newFixedBenchmarkHost(b, false, nil)
		requestFixedBenchmark(b, system, host, 470001)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			requestFixedBenchmark(b, system, host, 470001)
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		system, host := newFixedBenchmarkHost(b, false, nil)
		requestFixedBenchmark(b, system, host, 470001)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				requestFixedBenchmark(b, system, host, 470001)
			}
		})
	})
}

func BenchmarkFixedActorIdlePassivationExisting(b *testing.B) {
	kind, err := NewFixedKind("role", func() protoactor.Actor {
		return &benchmarkFixedActor{}
	}, WithIdlePassivation(time.Hour))
	if err != nil {
		b.Fatalf("NewFixedKind() error = %v", err)
	}
	hostProps, err := newFixedHostProps("role", testKindProps(b, kind))
	if err != nil {
		b.Fatalf("newFixedHostProps() error = %v", err)
	}
	system := protoactor.NewActorSystem()
	host := system.Root.Spawn(hostProps)
	b.Cleanup(func() {
		system.Root.StopFuture(host).Wait()
		system.Shutdown()
	})
	requestFixedBenchmark(b, system, host, 470001)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		requestFixedBenchmark(b, system, host, 470001)
	}
}

func BenchmarkFixedActorColdActivation(b *testing.B) {
	b.Run("Serial", func(b *testing.B) {
		system, host := newFixedBenchmarkHost(b, false, nil)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			requestFixedBenchmark(b, system, host, uint64(i+1))
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		system, host := newFixedBenchmarkHost(b, false, nil)
		var next atomic.Uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				requestFixedBenchmark(b, system, host, next.Add(1))
			}
		})
	})
}

func BenchmarkFixedActorChurn(b *testing.B) {
	var stopped atomic.Uint64
	system, host := newFixedBenchmarkHost(b, true, &stopped)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		requestFixedBenchmark(b, system, host, uint64(i+1))
	}
	for stopped.Load() < uint64(b.N) {
		runtime.Gosched()
	}
}

func newVirtualBenchmarkRuntime(b *testing.B) *Runtime {
	b.Helper()
	system := protoactor.NewActorSystem()
	provider := clusterTest.NewTestProvider(clusterTest.NewInMemAgent())
	config := cluster.Configure(
		"tx-actor-benchmark",
		provider,
		disthash.New(),
		remote.Configure("127.0.0.1", 0),
		cluster.WithKinds(cluster.NewKind("role", protoactor.PropsFromProducer(func() protoactor.Actor {
			return benchmarkVirtualActor{}
		}))),
	)
	actorCluster := cluster.New(system, config)
	actorCluster.StartMember()
	runtime := &Runtime{cluster: actorCluster, started: true}
	b.Cleanup(func() {
		if err := runtime.Shutdown(); err != nil {
			b.Errorf("Shutdown() error = %v", err)
		}
	})
	return runtime
}

func benchmarkIdentities(count int) []string {
	identities := make([]string, count)
	for i := range identities {
		identities[i] = "role-" + strconv.Itoa(i+1)
	}
	return identities
}

func newFixedBenchmarkHost(b *testing.B, stopAfterReply bool, stopped *atomic.Uint64) (*protoactor.ActorSystem, *protoactor.PID) {
	b.Helper()
	childProps := protoactor.PropsFromProducer(func() protoactor.Actor {
		return &benchmarkFixedActor{stopAfterReply: stopAfterReply, stopped: stopped}
	})
	hostProps, err := newFixedHostProps("role", childProps)
	if err != nil {
		b.Fatalf("newFixedHostProps() error = %v", err)
	}
	system := protoactor.NewActorSystem()
	host := system.Root.Spawn(hostProps)
	b.Cleanup(func() {
		system.Root.StopFuture(host).Wait()
		system.Shutdown()
	})
	return system, host
}

func requestFixedBenchmark(b *testing.B, system *protoactor.ActorSystem, host *protoactor.PID, routeKey uint64) {
	b.Helper()
	result, err := system.Root.RequestFuture(host, &actorpb.Delivery{
		MessageType:     "benchmark.command",
		CommandId:       routeKey,
		RouteKeyKind:    actorpb.RouteKeyKind_ROUTE_KEY_UINT64,
		RouteKeyNumeric: routeKey,
	}, 10*time.Second).Result()
	if err != nil {
		b.Fatalf("request fixed Actor: %v", err)
	}
	if _, ok := result.(benchmarkFixedReply); !ok {
		b.Fatalf("fixed Actor response = %T, want benchmarkFixedReply", result)
	}
}

type benchmarkVirtualActor struct{}

func (benchmarkVirtualActor) Receive(protoactor.Context) {}

type benchmarkFixedActor struct {
	stopAfterReply bool
	stopped        *atomic.Uint64
}

func (actor *benchmarkFixedActor) Receive(ctx protoactor.Context) {
	switch ctx.Message().(type) {
	case *actorpb.Delivery:
		ctx.Respond(benchmarkFixedReply{})
		if actor.stopAfterReply {
			ctx.Stop(ctx.Self())
		}
	case *protoactor.Stopped:
		if actor.stopped != nil {
			actor.stopped.Add(1)
		}
	}
}

type benchmarkFixedReply struct{}
