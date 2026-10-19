package tests

import (
	"bytes"
	"testing"

	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/protobuf/proto"
)

func TestRPCDeliveryKeepsRouteMetadataAndOpaquePayload(t *testing.T) {
	payload := []byte{0x92, 0xa4, 'r', 'o', 'l', 'e', 0xcd, 0x03, 0xe9}
	delivery := &rpcpb.Delivery{
		Route: &rpcpb.Route{
			Kind:       rpcpb.RouteKind_ROUTE_KIND_INSTANCE,
			InstanceId: "battle-2",
		},
		MessageType: "Role_UpdateNotify",
		CommandHash: 72001,
		Payload:     payload,
	}

	wire, err := proto.Marshal(delivery)
	if err != nil {
		t.Fatalf("marshal delivery: %v", err)
	}

	var received rpcpb.Delivery
	if err := proto.Unmarshal(wire, &received); err != nil {
		t.Fatalf("unmarshal delivery: %v", err)
	}

	if received.GetRoute().GetKind() != rpcpb.RouteKind_ROUTE_KIND_INSTANCE {
		t.Fatalf("route kind = %v, want instance", received.GetRoute().GetKind())
	}
	if received.GetRoute().GetInstanceId() != "battle-2" {
		t.Fatalf("instance id = %q, want battle-2", received.GetRoute().GetInstanceId())
	}
	if !bytes.Equal(received.GetPayload(), payload) {
		t.Fatalf("payload = %x, want %x", received.GetPayload(), payload)
	}
}

func TestRPCResultKeepsMetadataAndOpaquePayload(t *testing.T) {
	payload := []byte{0x83, 0xa4, 'l', 'i', 's', 't', 0x92, 0x01, 0x02}
	result := &rpcpb.Result{
		MessageType: "Login_ServerListResp",
		CommandHash: 10153,
		Payload:     payload,
	}

	wire, err := proto.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}

	var received rpcpb.Result
	if err := proto.Unmarshal(wire, &received); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	if received.GetMessageType() != "Login_ServerListResp" {
		t.Fatalf("message type = %q, want Login_ServerListResp", received.GetMessageType())
	}
	if received.GetCommandHash() != 10153 {
		t.Fatalf("command hash = %d, want 10153", received.GetCommandHash())
	}
	if !bytes.Equal(received.GetPayload(), payload) {
		t.Fatalf("payload = %x, want %x", received.GetPayload(), payload)
	}
}
