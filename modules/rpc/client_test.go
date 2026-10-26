package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type countingDialer struct {
	Dialer
	calls int
}

func (d *countingDialer) Dial(ctx context.Context, instance discovery.Instance) (Connection, error) {
	d.calls++
	return d.Dialer.Dial(ctx, instance)
}

// 生成的普通 gRPC 客户端与 Actor 直连共用实例连接，替换节点后不使用旧地址。
func TestRoutedClientSharesSenderConnections(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	root := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { root.Cancel(); root.Wait() }()
	start := func() *Host {
		host, err := NewHost("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Stop(context.Background()) })
		if err := host.Register(func(r grpc.ServiceRegistrar) error {
			healthpb.RegisterHealthServer(r, health.NewServer())
			return RegisterDeliveryService(r, nil, CallReceiverFunc(func(context.Context, Delivery) (Result, error) {
				return Result{MessageType: "reply"}, nil
			}))
		}); err != nil {
			t.Fatal(err)
		}
		if err := host.Start(root); err != nil {
			t.Fatal(err)
		}
		return host
	}
	directory := discovery.NewServiceDirectory()
	replace := func(host *Host) {
		if err := directory.Replace([]discovery.Instance{{ID: "game", Services: []string{"game"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning}}); err != nil {
			t.Fatal(err)
		}
	}
	replace(start())
	dialer, _ := NewGRPCDialer(insecure.NewCredentials())
	count := &countingDialer{Dialer: dialer}
	sender, err := NewSender(directory, count)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	client := healthpb.NewHealthClient(sender.Client(ServiceRoute("game")))
	check := func() {
		if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	check()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	watch, err := client.Watch(watchCtx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	cancelWatch()
	if _, err := sender.Call(ctx, Delivery{Route: InstanceRoute("game"), MessageType: "query"}); err != nil {
		t.Fatal(err)
	}
	if count.calls != 1 {
		t.Fatalf("同实例重复建连：%d", count.calls)
	}
	replace(start())
	check()
	if count.calls != 2 {
		t.Fatalf("地址替换未重新建连：%d", count.calls)
	}
	if err := sender.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); !errors.Is(err, ErrSenderClosed) {
		t.Fatalf("关闭后调用：%v", err)
	}
}
