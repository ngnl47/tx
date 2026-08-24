package tests

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRPCNextInstanceRouteUsesRunningInstance(t *testing.T) {
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
			Kind:       rpcpb.RouteKind_ROUTE_KIND_INSTANCE,
			InstanceId: "battle-2",
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
	if received.Route.Kind != rpc.RouteInstance || received.Route.InstanceID != "battle-2" {
		t.Fatalf("received route = %#v", received.Route)
	}
	if string(received.Payload) != string(payload) {
		t.Fatalf("received payload = %x, want %x", received.Payload, payload)
	}
}

func TestRPCAdmissionRejectsNewDeliveryAndDrainsAcceptedReceiver(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()

	root := lifecycle.NewWaitGroupContext(context.Background())
	ingress := lifecycle.NewIngress(root)
	started := make(chan struct{})
	release := make(chan struct{})
	receiver, err := rpc.NewAdmissionReceiver(rpc.ReceiverFunc(func(context.Context, rpc.Delivery) error {
		close(started)
		<-release
		return nil
	}), ingress)
	if err != nil {
		t.Fatalf("new admission receiver: %v", err)
	}
	if err := rpc.RegisterDeliveryService(server, receiver); err != nil {
		t.Fatalf("register delivery service: %v", err)
	}
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial delivery service: %v", err)
	}
	defer conn.Close()
	client := rpcpb.NewDeliveryServiceClient(conn)
	request := &rpcpb.Delivery{
		Route:       &rpcpb.Route{Kind: rpcpb.RouteKind_ROUTE_KIND_SERVICE, Service: "game"},
		MessageType: "role.update",
		CommandId:   72001,
	}

	accepted := make(chan error, 1)
	go func() {
		_, callErr := client.Deliver(context.Background(), request)
		accepted <- callErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("accepted receiver did not start")
	}

	ingress.BeginDrain()
	if _, err := client.Deliver(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("delivery while draining error = %v, want Unavailable", err)
	}
	drained := make(chan error, 1)
	go func() {
		drained <- ingress.Wait()
	}()
	select {
	case err := <-drained:
		t.Fatalf("ingress drained before accepted receiver completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	if err := <-accepted; err != nil {
		t.Fatalf("accepted delivery returned error: %v", err)
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("ingress Wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ingress did not drain after accepted receiver completed")
	}
	if _, err := ingress.Enter(); !errors.Is(err, lifecycle.ErrDraining) {
		t.Fatalf("ingress reopened after drain: %v", err)
	}
}

func TestRPCSenderResolvesDirectoryAndDelivers(t *testing.T) {
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

	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "battle-1",
		Services: []string{"battle"},
		Endpoint: "bufnet",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}

	dialer, err := rpc.NewGRPCDialer(insecure.NewCredentials(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("new gRPC dialer: %v", err)
	}
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	defer sender.Close()

	payload := []byte{0x81, 0xa2, 'h', 'p', 0x19, 0x63}
	if err := sender.Send(context.Background(), rpc.Delivery{
		Route:       rpc.ServiceRoute("battle"),
		MessageType: "Battle_Notify",
		CommandID:   8001,
		Payload:     payload,
	}); err != nil {
		t.Fatalf("send delivery: %v", err)
	}
	if received.Route.Service != "battle" {
		t.Fatalf("received service = %q, want battle", received.Route.Service)
	}
	if string(received.Payload) != string(payload) {
		t.Fatalf("received payload = %x, want %x", received.Payload, payload)
	}
}

func TestRPCSenderReusesEndpointConnection(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "battle-1",
		Services: []string{"battle"},
		Endpoint: "127.0.0.1:9200",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}

	connection := &recordingConnection{}
	dialer := &recordingDialer{connection: connection}
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}

	for range 2 {
		if err := sender.Send(context.Background(), rpc.Delivery{
			Route:       rpc.ServiceRoute("battle"),
			MessageType: "Battle_Notify",
		}); err != nil {
			t.Fatalf("send delivery: %v", err)
		}
	}
	if dialer.calls != 1 {
		t.Fatalf("dial calls = %d, want 1", dialer.calls)
	}
	if len(connection.deliveries) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(connection.deliveries))
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("close sender: %v", err)
	}
	if connection.closeCalls != 1 {
		t.Fatalf("connection close calls = %d, want 1", connection.closeCalls)
	}
}

type recordingDialer struct {
	calls      int
	connection *recordingConnection
}

func (d *recordingDialer) Dial(context.Context, discovery.Instance) (rpc.Connection, error) {
	d.calls++
	return d.connection, nil
}

type recordingConnection struct {
	deliveries []rpc.Delivery
	closeCalls int
}

func (c *recordingConnection) Deliver(_ context.Context, delivery rpc.Delivery) error {
	c.deliveries = append(c.deliveries, delivery)
	return nil
}

func (c *recordingConnection) Close() error {
	c.closeCalls++
	return nil
}
