package actorrpc

import (
	"context"
	"errors"
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

// 一个进程的多个实体类型共用同一 DeliveryService 和宿主，不能按 Type 重复注册服务。
func TestDirectSharedHostKinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	slowRelease := make(chan struct{}, 1)
	defer close(slowRelease)
	local := actorhost.New(128, time.Second, func(c txactor.Context) (*txactor.Behavior, error) {
		if c.Identity.Type == "deny" {
			return nil, actorhost.ErrActivationDenied
		}
		count := 0
		return &txactor.Behavior{Receive: func(_ txactor.Context, _ any) (any, error) {
			if c.Identity.Type == "slow" {
				<-slowRelease
			}
			count++
			return &actorpb.Result{MessageType: c.Identity.Type, CommandHash: uint32(count)}, nil
		}}, nil
	})
	defer local.Drain(ctx)
	host, err := rpc.NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() { host.Stop(context.Background()); root.Cancel(); root.Wait() }()
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return rpc.RegisterDeliveryService(registrar, nil, NewDirectReceiver("a", local))
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		t.Fatal(err)
	}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{ID: "a", Services: []string{"game", "world"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning},
		{ID: "b", Services: []string{"game"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning},
	}); err != nil {
		t.Fatal(err)
	}
	dialer, err := rpc.NewGRPCDialer(insecure.NewCredentials(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	transport := DirectTransport{Sender: sender}
	for round := 1; round <= 2; round++ {
		for _, kind := range []string{"role", "world"} {
			result, err := transport.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Type: kind, ID: "same-key"}}, &actorpb.Delivery{MessageType: "ping"})
			if err != nil {
				t.Fatal(err)
			}
			if result.MessageType != kind || result.CommandHash != uint32(round) {
				t.Fatalf("实体状态串扰: %+v", result)
			}
		}
	}
	if _, err := transport.Call(ctx, actorhost.Target{Instance: "b", Identity: txactor.Identity{Type: "role", ID: "stale"}}, &actorpb.Delivery{MessageType: "ping"}); !errors.Is(err, actorhost.ErrNotOwner) {
		t.Fatalf("错误归属未保留分类: %v", err)
	}
	local.BeginDrain()
	for _, kind := range []string{"role", "world"} {
		_, err := transport.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Type: kind, ID: "same-key"}}, &actorpb.Delivery{MessageType: "ping"})
		if !errors.Is(err, actorhost.ErrDraining) {
			t.Fatalf("排水未覆盖 %s: %v", kind, err)
		}
	}
}
