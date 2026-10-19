package tests

import (
	"errors"
	"testing"

	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	txprotocol "github.com/ngnl47/tx/protocol"
	"google.golang.org/protobuf/proto"
)

// 直接验证当前公共协议，不依赖历史 ProtoActor 运行时副本。
func TestActorDeliveryRoundTripCopiesPayload(t *testing.T) {
	identity := txprotocol.SessionIdentity{
		SessionPrincipal: txprotocol.SessionPrincipal{
			AccountID:       47,
			BorrowAccountID: 48,
			ServerID:        1,
			RoleID:          470001,
			SessionID:       99,
			AccessMode:      txprotocol.AccessModeBorrow,
		},
		ConnectionID: 7,
	}
	delivery := txactor.Delivery{
		MessageType: "role.rename",
		CommandHash: 470001,
		RouteKey:    txprotocol.Int64RouteKey(470001),
		Payload:     []byte{1, 2, 3},
		Identity:    &identity,
	}
	message, err := delivery.ToProto()
	if err != nil {
		t.Fatalf("ToProto() error = %v", err)
	}
	delivery.Payload[0] = 9
	if message.GetPayload()[0] != 1 {
		t.Fatalf("proto payload = %v, want defensive copy", message.GetPayload())
	}
	wire, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	decodedMessage := &actorpb.Delivery{}
	if err := proto.Unmarshal(wire, decodedMessage); err != nil {
		t.Fatalf("proto.Unmarshal() error = %v", err)
	}
	decoded, err := txactor.DeliveryFromProto(decodedMessage)
	if err != nil {
		t.Fatalf("DeliveryFromProto() error = %v", err)
	}
	decodedMessage.Payload[1] = 8
	key, ok := decoded.RouteKey.Int64()
	if decoded.MessageType != "role.rename" || decoded.CommandHash != 470001 || !ok || key != 470001 || string(decoded.Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("decoded delivery = %#v", decoded)
	}
	if decoded.Identity == nil || *decoded.Identity != identity {
		t.Fatalf("decoded identity = %#v, want %#v", decoded.Identity, identity)
	}
}

func TestActorDeliveryRoundTripPreservesCanCreateRole(t *testing.T) {
	identity := txprotocol.SessionIdentity{
		SessionPrincipal: txprotocol.SessionPrincipal{
			AccountID:     47,
			ServerID:      1,
			RoleID:        470001,
			SessionID:     99,
			CanCreateRole: true,
		},
		ConnectionID: 7,
	}
	message, err := (txactor.Delivery{MessageType: "role.create", Identity: &identity}).ToProto()
	if err != nil {
		t.Fatalf("ToProto() error = %v", err)
	}
	wire, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	decodedMessage := &actorpb.Delivery{}
	if err := proto.Unmarshal(wire, decodedMessage); err != nil {
		t.Fatalf("proto.Unmarshal() error = %v", err)
	}
	decoded, err := txactor.DeliveryFromProto(decodedMessage)
	if err != nil {
		t.Fatalf("DeliveryFromProto() error = %v", err)
	}
	if decoded.Identity == nil || *decoded.Identity != identity {
		t.Fatalf("decoded identity = %#v, want %#v", decoded.Identity, identity)
	}
}

func TestActorDeliveryAllowsOptionalInternalIdentity(t *testing.T) {
	identity := txprotocol.SessionIdentity{
		SessionPrincipal: txprotocol.SessionPrincipal{
			AccountID: 47,
			ServerID:  1,
			RoleID:    470001,
			SessionID: 99,
		},
		ConnectionID: 7,
	}
	if _, err := (txactor.Delivery{MessageType: "role.rename"}).ToProto(); err != nil {
		t.Fatalf("internal ToProto() error = %v", err)
	}
	message, err := (txactor.Delivery{MessageType: "role.rename", Identity: &identity}).ToProto()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := txactor.DeliveryFromProto(message)
	if err != nil || decoded.Identity == nil || *decoded.Identity != identity {
		t.Fatalf("internal identity round trip = %#v, %v", decoded, err)
	}
}

func TestActorDeliveryRejectsInvalidMessage(t *testing.T) {
	if _, err := (txactor.Delivery{}).ToProto(); !errors.Is(err, txactor.ErrInvalidDelivery) {
		t.Fatalf("ToProto() error = %v, want ErrInvalidDelivery", err)
	}
	if _, err := txactor.DeliveryFromProto(&actorpb.Delivery{MessageType: " role.rename"}); !errors.Is(err, txactor.ErrInvalidDelivery) {
		t.Fatalf("DeliveryFromProto() error = %v, want ErrInvalidDelivery", err)
	}
	if _, err := txactor.DeliveryFromProto(&actorpb.Delivery{
		MessageType: "role.rename",
		Mode:        actorpb.DeliveryMode_DELIVERY_MODE_CALL,
	}); !errors.Is(err, txactor.ErrInvalidDelivery) {
		t.Fatalf("DeliveryFromProto() missing request ID error = %v, want ErrInvalidDelivery", err)
	}
	if _, err := txactor.DeliveryFromProto(&actorpb.Delivery{
		MessageType: "role.rename",
		Mode:        actorpb.DeliveryMode_DELIVERY_MODE_TELL,
		RequestId:   1,
	}); !errors.Is(err, txactor.ErrInvalidDelivery) {
		t.Fatalf("DeliveryFromProto() unexpected request ID error = %v, want ErrInvalidDelivery", err)
	}
}
