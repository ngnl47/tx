package rpc

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
)

type contradictoryDeliveryClient struct {
	rpcpb.DeliveryServiceClient
}

func (*contradictoryDeliveryClient) Deliver(context.Context, *rpcpb.Delivery, ...grpc.CallOption) (*rpcpb.DeliveryAck, error) {
	return &rpcpb.DeliveryAck{Accepted: true, ErrorCode: "unexpected-error"}, nil
}

func TestGRPCConnectionRejectsContradictoryDeliveryAck(t *testing.T) {
	connection := &grpcConnection{client: &contradictoryDeliveryClient{}}
	err := connection.Deliver(context.Background(), Delivery{
		Route:       ServiceRoute("game"),
		MessageType: "role.save",
	})
	if !errors.Is(err, ErrDeliveryRejected) {
		t.Fatalf("Deliver error = %v, want %v", err, ErrDeliveryRejected)
	}
}

type resultDeliveryClient struct {
	rpcpb.DeliveryServiceClient
	result *rpcpb.Result
}

func (client *resultDeliveryClient) Call(context.Context, *rpcpb.Delivery, ...grpc.CallOption) (*rpcpb.Result, error) {
	return client.result, nil
}

func TestGRPCConnectionRejectsInvalidCallResult(t *testing.T) {
	tests := []struct {
		name   string
		result *rpcpb.Result
	}{
		{name: "nil response"},
		{name: "empty message type", result: &rpcpb.Result{}},
		{name: "blank message type", result: &rpcpb.Result{MessageType: " \t"}},
		{name: "leading whitespace", result: &rpcpb.Result{MessageType: " Login_ServerListResp"}},
		{name: "trailing whitespace", result: &rpcpb.Result{MessageType: "Login_ServerListResp "}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &grpcConnection{client: &resultDeliveryClient{result: test.result}}
			_, err := connection.Call(context.Background(), Delivery{
				Route:       ServiceRoute("login"),
				MessageType: "Login_ServerListReq",
			})
			if !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("Call error = %v, want %v", err, ErrInvalidResult)
			}
		})
	}
}

func TestSenderCallReturnsConnectionResult(t *testing.T) {
	directory := senderTestDirectory(t)
	expected := Result{
		MessageType: "Login_ServerListResp",
		CommandHash: 10153,
		Payload:     []byte{0x92, 0xa7, 's', 'e', 'r', 'v', 'e', 'r', 's', 0x91, 0x01},
	}
	connection := &senderConnectionFake{results: []Result{expected}}
	sender, err := NewSender(directory, &senderDialerFake{connection: connection})
	if err != nil {
		t.Fatalf("NewSender error = %v", err)
	}

	result, err := sender.Call(context.Background(), Delivery{
		Route:       ServiceRoute("game"),
		MessageType: "Login_ServerList",
	})
	if err != nil {
		t.Fatalf("Call error = %v", err)
	}
	if result.MessageType != expected.MessageType || result.CommandHash != expected.CommandHash || !bytes.Equal(result.Payload, expected.Payload) {
		t.Fatalf("Call result = %#v, want %#v", result, expected)
	}
	if connection.callCount != 1 {
		t.Fatalf("connection Call count = %d, want 1", connection.callCount)
	}
}

func TestSenderCallReusesEndpointConnection(t *testing.T) {
	directory := senderTestDirectory(t)
	connection := &senderConnectionFake{
		results: []Result{
			{MessageType: "Login_ServerListResp", CommandHash: 1},
			{MessageType: "Login_ServerListResp", CommandHash: 2},
		},
	}
	dialer := &senderDialerFake{connection: connection}
	sender, err := NewSender(directory, dialer)
	if err != nil {
		t.Fatalf("NewSender error = %v", err)
	}

	for range 2 {
		if _, err := sender.Call(context.Background(), Delivery{
			Route:       ServiceRoute("game"),
			MessageType: "Login_ServerList",
		}); err != nil {
			t.Fatalf("Call error = %v", err)
		}
	}
	if dialer.calls != 1 {
		t.Fatalf("dial calls = %d, want 1", dialer.calls)
	}
	if connection.callCount != 2 {
		t.Fatalf("connection Call count = %d, want 2", connection.callCount)
	}
}

func TestSenderCallDoesNotRetryImplicitly(t *testing.T) {
	directory := senderTestDirectory(t)
	expectedErr := errors.New("call failed")
	connection := &senderConnectionFake{callErr: expectedErr}
	sender, err := NewSender(directory, &senderDialerFake{connection: connection})
	if err != nil {
		t.Fatalf("NewSender error = %v", err)
	}

	_, err = sender.Call(context.Background(), Delivery{
		Route:       ServiceRoute("game"),
		MessageType: "Login_ServerList",
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Call error = %v, want %v", err, expectedErr)
	}
	if connection.callCount != 1 {
		t.Fatalf("connection Call count = %d, want 1", connection.callCount)
	}
}

func senderTestDirectory(t *testing.T) discovery.Directory {
	t.Helper()
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "game-1",
		Services: []string{"game"},
		Endpoint: "127.0.0.1:9200",
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("Replace directory error = %v", err)
	}
	return directory
}

type senderDialerFake struct {
	calls      int
	connection *senderConnectionFake
}

func (dialer *senderDialerFake) Dial(context.Context, discovery.Instance) (Connection, error) {
	dialer.calls++
	return dialer.connection, nil
}

type senderConnectionFake struct {
	results   []Result
	callErr   error
	callCount int
}

func (*senderConnectionFake) Deliver(context.Context, Delivery) error {
	return nil
}

func (connection *senderConnectionFake) Call(_ context.Context, _ Delivery) (Result, error) {
	connection.callCount++
	if connection.callErr != nil {
		return Result{}, connection.callErr
	}
	result := connection.results[0]
	connection.results = connection.results[1:]
	return result, nil
}

func (*senderConnectionFake) Close() error {
	return nil
}
