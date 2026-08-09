package tests

import (
	"context"
	"net"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestRPCResolveServiceRouteUsesRunningInstance(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{
			ID:       "battle-starting",
			Services: []string{"battle"},
			Endpoint: "127.0.0.1:9200",
			Status:   discovery.StatusStarting,
		},
		{
			ID:       "battle-running",
			Services: []string{"battle"},
			Endpoint: "127.0.0.1:9201",
			Status:   discovery.StatusRunning,
		},
	}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}

	instance, err := rpc.Resolve(directory, rpc.ServiceRoute("battle"))
	if err != nil {
		t.Fatalf("resolve route: %v", err)
	}
	if instance.ID != "battle-running" {
		t.Fatalf("instance id = %q, want battle-running", instance.ID)
	}
}

func TestRPCDeliveryServicePassesOpaquePayloadToReceiver(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()

	var received rpc.Delivery
	if err := rpc.RegisterDeliveryService(server, rpc.ReceiverFunc(func(_ context.Context, delivery rpc.Delivery) error {
		received = delivery
		return nil
	})); err != nil {
		t.Fatalf("register delivery service: %v", err)
	}
	go func() {
		_ = server.Serve(listener)
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial delivery service: %v", err)
	}
	defer conn.Close()

	payload := []byte{0x92, 0xa4, 'r', 'o', 'l', 'e', 0xcd, 0x03, 0xe9}
	ack, err := rpcpb.NewDeliveryServiceClient(conn).Deliver(ctx, &rpcpb.Delivery{
		Route: &rpcpb.Route{
			Kind:       rpcpb.RouteKind_ROUTE_KIND_SESSION,
			Service:    "msgq",
			InstanceId: "msgq-2",
			RoleId:     470001,
			SessionId:  99,
		},
		MessageType: "Role_UpdateNotify",
		CommandId:   72001,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !ack.GetAccepted() {
		t.Fatalf("delivery rejected with code %q", ack.GetErrorCode())
	}
	if received.Route.Kind != rpc.RouteSession || received.Route.InstanceID != "msgq-2" {
		t.Fatalf("received route = %#v", received.Route)
	}
	if string(received.Payload) != string(payload) {
		t.Fatalf("received payload = %x, want %x", received.Payload, payload)
	}
}
