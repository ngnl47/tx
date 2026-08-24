package rpc

import (
	"context"
	"errors"
	"testing"

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
