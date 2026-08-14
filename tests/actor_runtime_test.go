package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestActorConfigNormalize(t *testing.T) {
	config, err := (txactor.Config{
		ClusterName: " game ",
		Host:        " 127.0.0.1 ",
		Port:        9200,
		Etcd: txactor.EtcdConfig{
			Endpoints: []string{" 127.0.0.1:2379 "},
			Prefix:    " tx/tests/actor ",
		},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if config.ClusterName != "game" || config.Host != "127.0.0.1" {
		t.Fatalf("Normalize() cluster/host = %q/%q", config.ClusterName, config.Host)
	}
	if config.Etcd.Prefix != "/tx/tests/actor" {
		t.Fatalf("Normalize() etcd prefix = %q, want /tx/tests/actor", config.Etcd.Prefix)
	}
	if config.Etcd.DialTimeout <= 0 {
		t.Fatalf("Normalize() dial timeout = %v, want positive value", config.Etcd.DialTimeout)
	}
}

func TestActorConfigRejectsInvalidValues(t *testing.T) {
	tests := []txactor.Config{
		{Host: "127.0.0.1", Port: 9200, Etcd: txactor.EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}}},
		{ClusterName: "game", Port: 9200, Etcd: txactor.EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}}},
		{ClusterName: "game", Host: "127.0.0.1", Port: -1, Etcd: txactor.EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}}},
		{ClusterName: "game", Host: "127.0.0.1", Port: 9200},
	}
	for _, config := range tests {
		if _, err := config.Normalize(); !errors.Is(err, txactor.ErrInvalidConfig) {
			t.Fatalf("Normalize() error = %v, want ErrInvalidConfig", err)
		}
	}
}

func TestActorRuntimeRejectsVirtualPIDBeforeStart(t *testing.T) {
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "game",
		Host:        "127.0.0.1",
		Port:        9200,
		Etcd:        txactor.EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := runtime.VirtualPID("role", "470001"); !errors.Is(err, txactor.ErrRuntimeNotStarted) {
		t.Fatalf("VirtualPID() error = %v, want ErrRuntimeNotStarted", err)
	}
}

func TestActorDeliveryRoundTripCopiesPayload(t *testing.T) {
	delivery := txactor.Delivery{
		MessageType: "role.rename",
		CommandID:   470001,
		Payload:     []byte{1, 2, 3},
	}
	message, err := delivery.ToProto()
	if err != nil {
		t.Fatalf("ToProto() error = %v", err)
	}
	delivery.Payload[0] = 9
	if message.GetPayload()[0] != 1 {
		t.Fatalf("proto payload = %v, want defensive copy", message.GetPayload())
	}
	decoded, err := txactor.DeliveryFromProto(message)
	if err != nil {
		t.Fatalf("DeliveryFromProto() error = %v", err)
	}
	message.Payload[1] = 8
	if decoded.MessageType != "role.rename" || decoded.CommandID != 470001 || string(decoded.Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("decoded delivery = %#v", decoded)
	}
}

func TestActorDeliveryRejectsInvalidMessage(t *testing.T) {
	if _, err := (txactor.Delivery{}).ToProto(); !errors.Is(err, txactor.ErrInvalidDelivery) {
		t.Fatalf("ToProto() error = %v, want ErrInvalidDelivery", err)
	}
	if _, err := txactor.DeliveryFromProto(&actorpb.Delivery{MessageType: " role.rename"}); !errors.Is(err, txactor.ErrInvalidDelivery) {
		t.Fatalf("DeliveryFromProto() error = %v, want ErrInvalidDelivery", err)
	}
}

func TestActorRuntimeLifecycleIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the actor integration test")
	}
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "tx-actor-integration",
		Host:        "127.0.0.1",
		Port:        0,
		Etcd: txactor.EtcdConfig{
			Endpoints:   []string{endpoint},
			Prefix:      fmt.Sprintf("/tx/tests/actor-%d", time.Now().UnixNano()),
			DialTimeout: time.Second,
		},
	}, cluster.NewKind("echo", protoactor.PropsFromProducer(func() protoactor.Actor {
		return actorEcho{}
	})))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Shutdown(); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
		ctx.Cancel()
		ctx.WaitDoneAndWaitGroupWait()
	})

	pid, err := runtime.VirtualPID("echo", "role-470001")
	if err != nil {
		t.Fatalf("VirtualPID() error = %v", err)
	}
	system, err := runtime.ActorSystem()
	if err != nil {
		t.Fatalf("ActorSystem() error = %v", err)
	}
	delivery, err := (txactor.Delivery{MessageType: "role.ping", Payload: []byte{7}}).ToProto()
	if err != nil {
		t.Fatalf("ToProto() error = %v", err)
	}
	response, err := system.Root.RequestFuture(pid, delivery, 5*time.Second).Result()
	if err != nil {
		t.Fatalf("RequestFuture() error = %v", err)
	}
	ack, ok := response.(*wrapperspb.StringValue)
	if !ok || ack.GetValue() != "role.ping:7" {
		t.Fatalf("response = %#v, want role.ping:7", response)
	}
}

type actorEcho struct{}

func (actorEcho) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	delivery, err := txactor.DeliveryFromProto(message)
	if err != nil {
		return
	}
	payload := ""
	if len(delivery.Payload) > 0 {
		payload = fmt.Sprint(delivery.Payload[0])
	}
	ctx.Respond(wrapperspb.String(delivery.MessageType + ":" + payload))
}
