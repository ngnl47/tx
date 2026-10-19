package tests

import (
	"context"
	"errors"
	"net"
	"sync"
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
	}), nil); err != nil {
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
		CommandHash: 72001,
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

func TestRPCDeliveryServiceCallPassesDeliveryToCallReceiverAndReturnsResult(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()

	var received rpc.Delivery
	expected := rpc.Result{
		MessageType: "Login_ServerListResp",
		CommandHash: 10153,
		Payload:     []byte{0x83, 0xa5, 's', 'e', 'r', 'v', 'e', 0x91, 0x01},
	}
	if err := rpc.RegisterDeliveryService(server, nil, rpc.CallReceiverFunc(func(_ context.Context, delivery rpc.Delivery) (rpc.Result, error) {
		received = delivery
		return expected, nil
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

	payload := []byte{0x82, 0xa6, 'a', 'c', 'c', 'o', 'u', 'n', 't', 0xa4, 'g', 'u', 'e', 's', 't'}
	result, err := rpcpb.NewDeliveryServiceClient(conn).Call(ctx, &rpcpb.Delivery{
		Route: &rpcpb.Route{
			Kind:    rpcpb.RouteKind_ROUTE_KIND_SERVICE,
			Service: "login",
		},
		MessageType: "Login_ServerListReq",
		CommandHash: 10152,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if received.Route.Kind != rpc.RouteService || received.Route.Service != "login" {
		t.Fatalf("received route = %#v", received.Route)
	}
	if received.MessageType != "Login_ServerListReq" || received.CommandHash != 10152 {
		t.Fatalf("received delivery = %#v", received)
	}
	if string(received.Payload) != string(payload) {
		t.Fatalf("received payload = %x, want %x", received.Payload, payload)
	}
	if result.GetMessageType() != expected.MessageType {
		t.Fatalf("result message type = %q, want %q", result.GetMessageType(), expected.MessageType)
	}
	if result.GetCommandHash() != expected.CommandHash {
		t.Fatalf("result command hash = %d, want %d", result.GetCommandHash(), expected.CommandHash)
	}
	if string(result.GetPayload()) != string(expected.Payload) {
		t.Fatalf("result payload = %x, want %x", result.GetPayload(), expected.Payload)
	}
}

func TestRPCDeliveryServiceReturnsUnimplementedForMissingReceiver(t *testing.T) {
	tests := []struct {
		name         string
		receiver     rpc.Receiver
		callReceiver rpc.CallReceiver
		invoke       func(rpcpb.DeliveryServiceClient) error
	}{
		{
			name: "Deliver",
			callReceiver: rpc.CallReceiverFunc(func(context.Context, rpc.Delivery) (rpc.Result, error) {
				return rpc.Result{MessageType: "unused"}, nil
			}),
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Deliver(context.Background(), validRPCDeliveryRequest())
				return err
			},
		},
		{
			name: "Call",
			receiver: rpc.ReceiverFunc(func(context.Context, rpc.Delivery) error {
				return nil
			}),
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Call(context.Background(), validRPCDeliveryRequest())
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newRPCDeliveryServiceClient(t, test.receiver, test.callReceiver)
			if err := test.invoke(client); status.Code(err) != codes.Unimplemented {
				t.Fatalf("error = %v, want Unimplemented", err)
			}
		})
	}
}

func TestRPCDeliveryServiceRejectsInvalidDelivery(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(rpcpb.DeliveryServiceClient) error
	}{
		{
			name: "Deliver",
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Deliver(context.Background(), &rpcpb.Delivery{})
				return err
			},
		},
		{
			name: "Call",
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Call(context.Background(), &rpcpb.Delivery{})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newRPCDeliveryServiceClient(t,
				rpc.ReceiverFunc(func(context.Context, rpc.Delivery) error {
					return errors.New("invalid delivery reached receiver")
				}),
				rpc.CallReceiverFunc(func(context.Context, rpc.Delivery) (rpc.Result, error) {
					return rpc.Result{}, errors.New("invalid delivery reached call receiver")
				}),
			)
			if err := test.invoke(client); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestRPCDeliveryServiceRejectsInvalidResult(t *testing.T) {
	tests := []struct {
		name   string
		result rpc.Result
	}{
		{name: "empty message type"},
		{name: "blank message type", result: rpc.Result{MessageType: " \t"}},
		{name: "leading whitespace", result: rpc.Result{MessageType: " Login_ServerListResp"}},
		{name: "trailing whitespace", result: rpc.Result{MessageType: "Login_ServerListResp "}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newRPCDeliveryServiceClient(t, nil, rpc.CallReceiverFunc(func(context.Context, rpc.Delivery) (rpc.Result, error) {
				return test.result, nil
			}))
			_, err := client.Call(context.Background(), validRPCDeliveryRequest())
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Call error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestRPCDeliveryServiceMapsReceiverErrorsToInternal(t *testing.T) {
	expectedErr := errors.New("receiver failed")
	tests := []struct {
		name         string
		receiver     rpc.Receiver
		callReceiver rpc.CallReceiver
		invoke       func(rpcpb.DeliveryServiceClient) error
	}{
		{
			name: "Deliver",
			receiver: rpc.ReceiverFunc(func(context.Context, rpc.Delivery) error {
				return expectedErr
			}),
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Deliver(context.Background(), validRPCDeliveryRequest())
				return err
			},
		},
		{
			name: "Call",
			callReceiver: rpc.CallReceiverFunc(func(context.Context, rpc.Delivery) (rpc.Result, error) {
				return rpc.Result{}, expectedErr
			}),
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Call(context.Background(), validRPCDeliveryRequest())
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newRPCDeliveryServiceClient(t, test.receiver, test.callReceiver)
			if err := test.invoke(client); status.Code(err) != codes.Internal {
				t.Fatalf("error = %v, want Internal", err)
			}
		})
	}
}

func TestRPCDeliveryServiceMapsAdmissionStoppingToUnavailable(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(rpcpb.DeliveryServiceClient) error
	}{
		{
			name: "Deliver",
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Deliver(context.Background(), validRPCDeliveryRequest())
				return err
			},
		},
		{
			name: "Call",
			invoke: func(client rpcpb.DeliveryServiceClient) error {
				_, err := client.Call(context.Background(), validRPCDeliveryRequest())
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := lifecycle.NewWaitGroupContext(context.Background())
			ingress := lifecycle.NewIngress(root)
			receiver, err := rpc.NewAdmissionReceiver(rpc.ReceiverFunc(func(context.Context, rpc.Delivery) error {
				return errors.New("stopping admission entered receiver")
			}), ingress)
			if err != nil {
				t.Fatalf("new admission receiver: %v", err)
			}
			callReceiver, err := rpc.NewAdmissionCallReceiver(rpc.CallReceiverFunc(func(context.Context, rpc.Delivery) (rpc.Result, error) {
				return rpc.Result{}, errors.New("stopping admission entered call receiver")
			}), ingress)
			if err != nil {
				t.Fatalf("new admission call receiver: %v", err)
			}
			client := newRPCDeliveryServiceClient(t, receiver, callReceiver)

			root.Cancel()
			root.WaitDoneAndWaitGroupWait()
			if err := test.invoke(client); status.Code(err) != codes.Unavailable {
				t.Fatalf("error = %v, want Unavailable", err)
			}
		})
	}
}

func TestRegisterDeliveryServiceRejectsMissingReceivers(t *testing.T) {
	if err := rpc.RegisterDeliveryService(grpc.NewServer(), nil, nil); !errors.Is(err, rpc.ErrReceiverNil) {
		t.Fatalf("RegisterDeliveryService error = %v, want %v", err, rpc.ErrReceiverNil)
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
	if err := rpc.RegisterDeliveryService(server, receiver, nil); err != nil {
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
		CommandHash: 72001,
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

func TestRPCAdmissionRejectsNewCallAndDrainsAcceptedCallReceiver(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()

	root := lifecycle.NewWaitGroupContext(context.Background())
	ingress := lifecycle.NewIngress(root)
	started := make(chan struct{})
	release := make(chan struct{})
	receiver, err := rpc.NewAdmissionCallReceiver(rpc.CallReceiverFunc(func(context.Context, rpc.Delivery) (rpc.Result, error) {
		close(started)
		<-release
		return rpc.Result{MessageType: "Login_ServerListResp"}, nil
	}), ingress)
	if err != nil {
		t.Fatalf("new admission call receiver: %v", err)
	}
	if err := rpc.RegisterDeliveryService(server, nil, receiver); err != nil {
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
		Route:       &rpcpb.Route{Kind: rpcpb.RouteKind_ROUTE_KIND_SERVICE, Service: "login"},
		MessageType: "Login_ServerListReq",
		CommandHash: 10152,
	}

	accepted := make(chan error, 1)
	var releaseOnce sync.Once
	releaseCall := func() {
		releaseOnce.Do(func() { close(release) })
	}
	go func() {
		_, callErr := client.Call(context.Background(), request)
		accepted <- callErr
	}()
	defer releaseCall()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("accepted call receiver did not start")
	}

	ingress.BeginDrain()
	if _, err := client.Call(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("call while draining error = %v, want Unavailable", err)
	}
	drained := make(chan error, 1)
	go func() {
		drained <- ingress.Wait()
	}()
	select {
	case err := <-drained:
		t.Fatalf("ingress drained before accepted call receiver completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	releaseCall()
	if err := <-accepted; err != nil {
		t.Fatalf("accepted call returned error: %v", err)
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("ingress Wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ingress did not drain after accepted call receiver completed")
	}
	if _, err := ingress.Enter(); !errors.Is(err, lifecycle.ErrDraining) {
		t.Fatalf("ingress reopened after drain: %v", err)
	}
}

func validRPCDeliveryRequest() *rpcpb.Delivery {
	return &rpcpb.Delivery{
		Route:       &rpcpb.Route{Kind: rpcpb.RouteKind_ROUTE_KIND_SERVICE, Service: "login"},
		MessageType: "Login_ServerListReq",
		CommandHash: 10152,
	}
}

func newRPCDeliveryServiceClient(t *testing.T, receiver rpc.Receiver, callReceiver rpc.CallReceiver) rpcpb.DeliveryServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	if err := rpc.RegisterDeliveryService(server, receiver, callReceiver); err != nil {
		t.Fatalf("register delivery service: %v", err)
	}
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		_ = listener.Close()
		t.Fatalf("dial delivery service: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})
	return rpcpb.NewDeliveryServiceClient(conn)
}

func TestRPCSenderResolvesDirectoryAndDelivers(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()

	var received rpc.Delivery
	if err := rpc.RegisterDeliveryService(server, rpc.ReceiverFunc(func(_ context.Context, delivery rpc.Delivery) error {
		received = delivery
		return nil
	}), nil); err != nil {
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
		CommandHash: 8001,
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

func TestRPCSenderCallResolvesDirectoryAndReturnsResult(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	defer server.Stop()

	var received rpc.Delivery
	expected := rpc.Result{
		MessageType: "Login_ServerListResp",
		CommandHash: 10153,
		Payload:     []byte{0x91, 0x82, 0xa2, 'i', 'd', 0x01, 0xa4, 'n', 'a', 'm', 'e', 0xa4, 'z', 'o', 'n', 'e'},
	}
	if err := rpc.RegisterDeliveryService(server, nil, rpc.CallReceiverFunc(func(_ context.Context, delivery rpc.Delivery) (rpc.Result, error) {
		received = delivery
		return expected, nil
	})); err != nil {
		t.Fatalf("register delivery service: %v", err)
	}
	go func() {
		_ = server.Serve(listener)
	}()

	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "login-1",
		Services: []string{"login"},
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

	payload := []byte{0x82, 0xa6, 'p', 'l', 'a', 't', 'f', 'o', 'r', 'm', 0xa3, 'i', 'o', 's'}
	result, err := sender.Call(context.Background(), rpc.Delivery{
		Route:       rpc.ServiceRoute("login"),
		MessageType: "Login_ServerListReq",
		CommandHash: 10152,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("sender call: %v", err)
	}
	if received.Route.Kind != rpc.RouteService || received.Route.Service != "login" {
		t.Fatalf("received route = %#v", received.Route)
	}
	if received.MessageType != "Login_ServerListReq" || received.CommandHash != 10152 {
		t.Fatalf("received delivery = %#v", received)
	}
	if string(received.Payload) != string(payload) {
		t.Fatalf("received payload = %x, want %x", received.Payload, payload)
	}
	if result.MessageType != expected.MessageType {
		t.Fatalf("result message type = %q, want %q", result.MessageType, expected.MessageType)
	}
	if result.CommandHash != expected.CommandHash {
		t.Fatalf("result command hash = %d, want %d", result.CommandHash, expected.CommandHash)
	}
	if string(result.Payload) != string(expected.Payload) {
		t.Fatalf("result payload = %x, want %x", result.Payload, expected.Payload)
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
	results    []rpc.Result
	closeCalls int
}

func (c *recordingConnection) Deliver(_ context.Context, delivery rpc.Delivery) error {
	c.deliveries = append(c.deliveries, delivery)
	return nil
}

func (c *recordingConnection) Call(_ context.Context, delivery rpc.Delivery) (rpc.Result, error) {
	c.deliveries = append(c.deliveries, delivery)
	if len(c.results) == 0 {
		return rpc.Result{}, nil
	}
	result := c.results[0]
	c.results = c.results[1:]
	return result, nil
}

func (c *recordingConnection) Close() error {
	c.closeCalls++
	return nil
}
