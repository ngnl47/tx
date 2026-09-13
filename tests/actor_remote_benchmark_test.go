package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	txprotocol "github.com/ngnl47/tx/protocol"
)

func BenchmarkActorCallLoopback(b *testing.B) {
	etcdEndpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if etcdEndpoint == "" {
		b.Skip("set TX_ETCD_ENDPOINT to run the Actor Remote loopback benchmark")
	}

	payload := bytes.Repeat([]byte{0x7a}, 256)
	delivery := txactor.Delivery{
		MessageType: "benchmark.ping",
		CommandID:   470001,
		RouteKey:    txprotocol.Uint64RouteKey(470001),
		Payload:     payload,
	}
	fixture := newActorRemoteBenchmarkFixture(b, etcdEndpoint)
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
	b.Cleanup(cancel)

	for _, benchmark := range []struct {
		name string
		call func(*actorRemoteBenchmarkFixture, context.Context, txactor.Delivery) (txactor.Result, error)
	}{
		{
			name: "Local",
			call: func(fixture *actorRemoteBenchmarkFixture, ctx context.Context, delivery txactor.Delivery) (txactor.Result, error) {
				return fixture.target.Call(ctx, fixture.pid, delivery)
			},
		},
		{
			name: "Remote",
			call: func(fixture *actorRemoteBenchmarkFixture, ctx context.Context, delivery txactor.Delivery) (txactor.Result, error) {
				return fixture.caller.Call(ctx, fixture.pid, delivery)
			},
		},
	} {
		b.Run(benchmark.name+"/Serial", func(b *testing.B) {
			requireActorRemoteBenchmarkCall(b, benchmark.call, fixture, ctx, delivery)

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for b.Loop() {
				requireActorRemoteBenchmarkCall(b, benchmark.call, fixture, ctx, delivery)
			}
		})
		b.Run(benchmark.name+"/Parallel", func(b *testing.B) {
			requireActorRemoteBenchmarkCall(b, benchmark.call, fixture, ctx, delivery)

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := actorRemoteBenchmarkCall(benchmark.call, fixture, ctx, delivery); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

type actorRemoteBenchmarkFixture struct {
	target *txactor.Runtime
	caller *txactor.Runtime
	pid    *protoactor.PID
}

func newActorRemoteBenchmarkFixture(b *testing.B, etcdEndpoint string) *actorRemoteBenchmarkFixture {
	b.Helper()
	prefix := fmt.Sprintf("/tx/tests/actor-remote-benchmark-%d", time.Now().UnixNano())
	targetRoot := lifecycle.NewWaitGroupContext(context.Background())
	callerRoot := lifecycle.NewWaitGroupContext(context.Background())

	kind := txactor.MustFixedKind("benchmark-echo", func() protoactor.Actor {
		return actorRemoteBenchmarkEcho{}
	})
	target, err := txactor.New(actorRemoteBenchmarkConfig(etcdEndpoint, prefix), kind)
	if err != nil {
		b.Fatalf("new target Runtime: %v", err)
	}
	if err := target.Start(targetRoot); err != nil {
		b.Fatalf("start target Runtime: %v", err)
	}
	caller, err := txactor.New(actorRemoteBenchmarkConfig(etcdEndpoint, prefix))
	if err != nil {
		_ = target.Shutdown()
		targetRoot.Cancel()
		targetRoot.WaitDoneAndWaitGroupWait()
		b.Fatalf("new caller Runtime: %v", err)
	}
	if err := caller.Start(callerRoot); err != nil {
		_ = caller.Shutdown()
		callerRoot.Cancel()
		callerRoot.WaitDoneAndWaitGroupWait()
		_ = target.Shutdown()
		targetRoot.Cancel()
		targetRoot.WaitDoneAndWaitGroupWait()
		b.Fatalf("start caller Runtime: %v", err)
	}
	b.Cleanup(func() {
		if err := caller.Shutdown(); err != nil {
			b.Errorf("shutdown caller Runtime: %v", err)
		}
		callerRoot.Cancel()
		callerRoot.WaitDoneAndWaitGroupWait()
		if err := target.Shutdown(); err != nil {
			b.Errorf("shutdown target Runtime: %v", err)
		}
		targetRoot.Cancel()
		targetRoot.WaitDoneAndWaitGroupWait()
	})

	endpoint, err := target.Endpoint()
	if err != nil {
		b.Fatalf("target Endpoint: %v", err)
	}
	pid, err := txactor.FixedKindPID(endpoint, "benchmark-echo")
	if err != nil {
		b.Fatalf("FixedKindPID: %v", err)
	}
	return &actorRemoteBenchmarkFixture{target: target, caller: caller, pid: pid}
}

func actorRemoteBenchmarkConfig(etcdEndpoint, prefix string) txactor.Config {
	return txactor.Config{
		ClusterName: "tx-actor-remote-benchmark",
		Host:        "127.0.0.1",
		Port:        0,
		LogLevel:    txactor.LogLevelError,
		Etcd: txactor.EtcdConfig{
			Endpoints:   []string{etcdEndpoint},
			Prefix:      prefix,
			DialTimeout: time.Second,
		},
	}
}

func requireActorRemoteBenchmarkCall(
	b *testing.B,
	call func(*actorRemoteBenchmarkFixture, context.Context, txactor.Delivery) (txactor.Result, error),
	fixture *actorRemoteBenchmarkFixture,
	ctx context.Context,
	delivery txactor.Delivery,
) {
	b.Helper()
	if err := actorRemoteBenchmarkCall(call, fixture, ctx, delivery); err != nil {
		b.Fatal(err)
	}
}

func actorRemoteBenchmarkCall(
	call func(*actorRemoteBenchmarkFixture, context.Context, txactor.Delivery) (txactor.Result, error),
	fixture *actorRemoteBenchmarkFixture,
	ctx context.Context,
	delivery txactor.Delivery,
) error {
	result, err := call(fixture, ctx, delivery)
	if err != nil {
		return fmt.Errorf("Actor Call: %w", err)
	}
	if result.MessageType != "benchmark.pong" || result.CommandID != delivery.CommandID+1 || !bytes.Equal(result.Payload, delivery.Payload) {
		return fmt.Errorf("Actor Call result = %#v", result)
	}
	return nil
}

type actorRemoteBenchmarkEcho struct{}

func (actorRemoteBenchmarkEcho) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	delivery, err := txactor.DeliveryFromProto(message)
	if err != nil {
		panic(err)
	}
	if !delivery.WantsResult() {
		return
	}
	if err := txactor.Respond(ctx, delivery, txactor.Result{
		MessageType: "benchmark.pong",
		CommandID:   delivery.CommandID + 1,
		Payload:     append([]byte(nil), delivery.Payload...),
	}, nil); err != nil {
		panic(err)
	}
}
