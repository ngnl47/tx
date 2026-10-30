package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestHostServesRegisteredServicesOnOneEndpoint(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewHost returned error: %v", err)
	}
	var delivered Delivery
	var called Delivery
	expectedResult := Result{
		MessageType: "Login_ServerListResp",
		CommandHash: 10153,
		Payload:     []byte("result-body"),
	}
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		healthpb.RegisterHealthServer(registrar, health.NewServer())
		return nil
	}); err != nil {
		t.Fatalf("register health service: %v", err)
	}
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return RegisterDeliveryService(registrar, ReceiverFunc(func(_ context.Context, delivery Delivery) error {
			delivered = delivery
			return nil
		}), CallReceiverFunc(func(_ context.Context, delivery Delivery) (Result, error) {
			called = delivery
			return expectedResult, nil
		}))
	}); err != nil {
		t.Fatalf("register delivery service: %v", err)
	}
	if err := host.Start(root, nil); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := host.Stop(context.Background()); err != nil {
			t.Errorf("Stop returned error: %v", err)
		}
		root.Cancel()
		root.Wait()
	})

	connection, err := grpc.NewClient(host.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient returned error: %v", err)
	}
	defer connection.Close()
	if _, err := healthpb.NewHealthClient(connection).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health Check returned error: %v", err)
	}
	client := rpcpb.NewDeliveryServiceClient(connection)
	if _, err := client.Deliver(context.Background(), &rpcpb.Delivery{
		Route:       &rpcpb.Route{Kind: rpcpb.RouteKind_ROUTE_KIND_SERVICE, Service: "game"},
		MessageType: "role.sync",
		CommandHash: 47001,
		Payload:     []byte("bon-body"),
	}); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}
	if delivered.MessageType != "role.sync" || delivered.CommandHash != 47001 || string(delivered.Payload) != "bon-body" {
		t.Fatalf("delivered = %#v", delivered)
	}
	result, err := client.Call(context.Background(), &rpcpb.Delivery{
		Route:       &rpcpb.Route{Kind: rpcpb.RouteKind_ROUTE_KIND_SERVICE, Service: "login"},
		MessageType: "login.request",
		CommandHash: 10152,
		Payload:     []byte("query-body"),
	})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if called.Route.Kind != RouteService || called.Route.Service != "login" || called.MessageType != "login.request" || called.CommandHash != 10152 || string(called.Payload) != "query-body" {
		t.Fatalf("called = %#v", called)
	}
	if result.GetMessageType() != expectedResult.MessageType || result.GetCommandHash() != expectedResult.CommandHash || string(result.GetPayload()) != string(expectedResult.Payload) {
		t.Fatalf("result = %#v, want %#v", result, expectedResult)
	}
}

func TestHostRejectsRegistrationAfterStart(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewHost returned error: %v", err)
	}
	if err := host.Start(root, nil); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = host.Stop(context.Background())
		root.Cancel()
		root.Wait()
	})

	if err := host.Register(func(grpc.ServiceRegistrar) error { return nil }); err == nil {
		t.Fatal("Register after Start error = nil, want error")
	}
}

func TestHostReportsUnexpectedServeFailure(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { root.Cancel(); root.Wait() }()
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 1)
	if err := host.Start(root, func(err error) { failures <- err }); err != nil {
		t.Fatal(err)
	}
	// 绕过正常 Stop 关闭 listener，模拟 Serve 的终止性 Accept 错误。
	if err := host.listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("未传递错误")
		}
	case <-time.After(time.Second):
		t.Fatal("未通知宿主")
	}
	_ = host.Stop(t.Context())
}

func TestHostNormalStopDoesNotReportFailure(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { root.Cancel(); root.Wait() }()
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 1)
	if err := host.Start(root, func(err error) { failures <- err }); err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		t.Fatalf("正常关闭误报: %v", err)
	default:
	}
}
