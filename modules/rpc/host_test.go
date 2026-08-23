package rpc

import (
	"context"
	"testing"

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
		}))
	}); err != nil {
		t.Fatalf("register delivery service: %v", err)
	}
	if err := host.Start(root); err != nil {
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
		CommandId:   47001,
		Payload:     []byte("bon-body"),
	}); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}
	if delivered.MessageType != "role.sync" || delivered.CommandID != 47001 || string(delivered.Payload) != "bon-body" {
		t.Fatalf("delivered = %#v", delivered)
	}
}

func TestHostRejectsRegistrationAfterStart(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewHost returned error: %v", err)
	}
	if err := host.Start(root); err != nil {
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
