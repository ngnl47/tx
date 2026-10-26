package framework

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// 进程级协议只需 RPC 和 Discovery，不借用 Actor 初始化或业务存储。
func TestGeneratedRPCClientWithoutActor(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ETCD_ENDPOINT 验证普通进程 RPC 装配")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	service := testService{
		spec: Spec{Name: "query", Requires: []CModID{CModRPC, CModDiscovery}},
		start: func(runtime Context) error {
			return runtime.RPC().Register(func(registrar grpc.ServiceRegistrar) error {
				healthpb.RegisterHealthServer(registrar, health.NewServer())
				return nil
			})
		},
	}
	app, err := New(Config{AppID: "rpc-client-test", Discovery: DiscoveryConfig{Etcd: discovery.EtcdConfig{
		Endpoints: []string{endpoint}, Prefix: fmt.Sprintf("/tx/tests/rpc-client-%d", time.Now().UnixNano()), LeaseTTL: time.Second, DialTimeout: time.Second,
	}}}, WithService(service))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := app.Start(ctx, RunOptions{Services: []string{"query"}, RPCListen: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if app.runtime.actor != nil || app.Context().Storage() != nil {
		t.Fatal("普通 RPC 不应创建 Actor 或存储")
	}
	for len(app.Context().Directory().RunningInstances("query")) == 0 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	client := healthpb.NewHealthClient(app.Context().RPC().Client(rpc.ServiceRoute("query")))
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err == nil {
		t.Fatal("关停后仍接受调用")
	}
}
