package actorrpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BenchmarkDirectCallTransport 在相同本地宿主和复用连接上比较 Unary、短窗口合批与长流的吞吐成本。
func BenchmarkDirectCallTransport(b *testing.B) {
	local := actorhost.New(256, time.Second, func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) {
			return &actorpb.Result{MessageType: "pong"}, nil
		}}, nil
	})
	var calls atomic.Int64
	var streams atomic.Int64
	host, err := rpc.NewHost("127.0.0.1:0", grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/tx.rpc.v1.DeliveryService/Call" {
			calls.Add(1)
		}
		return handler(ctx, request)
	}), grpc.StreamInterceptor(func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == "/tx.rpc.v1.DeliveryService/CallBatch" {
			calls.Add(1)
		}
		if info.FullMethod == "/tx.rpc.v1.DeliveryService/CallStream" {
			streams.Add(1)
		}
		return handler(server, stream)
	}))
	if err != nil {
		b.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return rpc.RegisterDeliveryService(registrar, nil, NewDirectReceiver("game-a", local))
	}); err != nil {
		b.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		b.Fatal(err)
	}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "game-a", Services: []string{"game"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning}}); err != nil {
		b.Fatal(err)
	}
	dialer, err := rpc.NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		b.Fatal(err)
	}
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = sender.Close()
		_ = host.Stop(context.Background())
		root.Cancel()
		root.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = local.Drain(ctx)
	})
	for _, candidate := range []struct {
		name   string
		caller Caller
	}{
		{"Single", &DirectTransport{Sender: sender}},
		{"Batch1ms100", NewBatchingDirectTransport(sender)},
		{"Stream", &StreamingDirectTransport{Sender: sender}},
	} {
		for _, parallelism := range []int{16, 64} {
			b.Run(fmt.Sprintf("%s/P%d", candidate.name, parallelism), func(b *testing.B) {
				before := calls.Load()
				var workers atomic.Int64
				b.SetParallelism(parallelism)
				b.ReportAllocs()
				b.RunParallel(func(p *testing.PB) {
					target := actorhost.Target{Instance: "game-a", Identity: actorhost.Identity{Kind: "role", Key: fmt.Sprint(workers.Add(1))}}
					delivery := &actorpb.Delivery{MessageType: "ping", Payload: make([]byte, 256)}
					for p.Next() {
						ctx, cancel := context.WithTimeout(b.Context(), 5*time.Second)
						result, err := candidate.caller.Call(ctx, target, delivery)
						cancel()
						if err != nil || result == nil || result.MessageType != "pong" {
							b.Errorf("直连调用失败: result=%v err=%v", result, err)
							return
						}
					}
				})
				if candidate.name == "Stream" {
					b.ReportMetric(float64(streams.Load()), "streams")
				} else {
					b.ReportMetric(float64(b.N)/float64(calls.Load()-before), "items/rpc")
				}
			})
		}
	}
}
