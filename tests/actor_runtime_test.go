package tests

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	txprotocol "github.com/ngnl47/tx/protocol"
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
	if config.MailboxCapacity != txactor.DefaultMailboxCapacity {
		t.Fatalf("Normalize() mailbox capacity = %d, want %d", config.MailboxCapacity, txactor.DefaultMailboxCapacity)
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

func TestActorRuntimeRejectsTellBeforeStart(t *testing.T) {
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "game",
		Host:        "127.0.0.1",
		Port:        9200,
		Etcd:        txactor.EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	pid, err := txactor.FixedPID("127.0.0.1:9201", "battle-1001")
	if err != nil {
		t.Fatalf("FixedPID() error = %v", err)
	}
	if err := runtime.Tell(pid, txactor.Delivery{MessageType: "battle.tick"}); !errors.Is(err, txactor.ErrRuntimeNotStarted) {
		t.Fatalf("Tell() error = %v, want ErrRuntimeNotStarted", err)
	}
}

func TestActorRuntimeRejectsConfirmBeforeStart(t *testing.T) {
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "game",
		Host:        "127.0.0.1",
		Port:        9200,
		Etcd:        txactor.EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	pid, err := txactor.FixedPID("127.0.0.1:9201", "battle-1001")
	if err != nil {
		t.Fatalf("FixedPID() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.TellConfirmed(ctx, pid, txactor.Delivery{MessageType: "battle.tick"}); !errors.Is(err, txactor.ErrRuntimeNotStarted) {
		t.Fatalf("Confirm() error = %v, want ErrRuntimeNotStarted", err)
	}
}

func TestActorDeliveryRoundTripCopiesPayload(t *testing.T) {
	delivery := txactor.Delivery{
		MessageType: "role.rename",
		CommandID:   470001,
		RouteKey:    txprotocol.Int64RouteKey(470001),
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
	key, ok := decoded.RouteKey.Int64()
	if decoded.MessageType != "role.rename" || decoded.CommandID != 470001 || !ok || key != 470001 || string(decoded.Payload) != string([]byte{1, 2, 3}) {
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
	received := make(chan txactor.Delivery, 3)
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
		return actorEcho{received: received}
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
	select {
	case receivedDelivery := <-received:
		if receivedDelivery.MessageType != "role.ping" || string(receivedDelivery.Payload) != string([]byte{7}) {
			t.Fatalf("request delivery = %#v", receivedDelivery)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait actor request delivery timed out")
	}
	confirmCtx, cancelConfirm := context.WithTimeout(ctx, 5*time.Second)
	defer cancelConfirm()
	if err := runtime.TellConfirmed(confirmCtx, pid, txactor.Delivery{MessageType: "role.accept", CommandID: 8, Payload: []byte{8}}); err != nil {
		t.Fatalf("Confirm() error = %v", err)
	}
	select {
	case receivedDelivery := <-received:
		if receivedDelivery.MessageType != "role.accept" || receivedDelivery.CommandID != 8 || string(receivedDelivery.Payload) != string([]byte{8}) {
			t.Fatalf("confirmed delivery = %#v", receivedDelivery)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait actor confirmed delivery timed out")
	}
	if err := runtime.Tell(pid, txactor.Delivery{MessageType: "role.notify", CommandID: 7, Payload: []byte{9}}); err != nil {
		t.Fatalf("Tell() error = %v", err)
	}
	select {
	case receivedDelivery := <-received:
		if receivedDelivery.MessageType != "role.notify" || receivedDelivery.CommandID != 7 || string(receivedDelivery.Payload) != string([]byte{9}) {
			t.Fatalf("received delivery = %#v", receivedDelivery)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait actor tell delivery timed out")
	}
}

func TestActorRuntimeLogsMailboxOverflow(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the actor integration test")
	}
	logs := make(chan slog.Record, 4)
	txlog.SetDefault(slog.New(actorLogHandler{records: logs}))
	t.Cleanup(func() {
		txlog.SetDefault(nil)
	})

	ctx := lifecycle.NewWaitGroupContext(context.Background())
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	runtime, err := txactor.New(txactor.Config{
		ClusterName:     "tx-actor-overflow",
		Host:            "127.0.0.1",
		Port:            0,
		MailboxCapacity: 2,
		Etcd: txactor.EtcdConfig{
			Endpoints:   []string{endpoint},
			Prefix:      fmt.Sprintf("/tx/tests/actor-overflow-%d", time.Now().UnixNano()),
			DialTimeout: time.Second,
		},
	}, cluster.NewKind("blocked", protoactor.PropsFromProducer(func() protoactor.Actor {
		return blockingActor{started: started, release: release}
	})))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		close(release)
		if err := runtime.Shutdown(); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
		ctx.Cancel()
		ctx.WaitDoneAndWaitGroupWait()
	})

	pid, err := runtime.VirtualPID("blocked", "role-470001")
	if err != nil {
		t.Fatalf("VirtualPID() error = %v", err)
	}
	if err := runtime.Tell(pid, txactor.Delivery{MessageType: "role.block"}); err != nil {
		t.Fatalf("Tell() first message error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("wait blocked actor start timed out")
	}
	for commandID := uint64(1); commandID <= 8; commandID++ {
		if err := runtime.Tell(pid, txactor.Delivery{MessageType: "role.overflow", CommandID: commandID}); err != nil {
			t.Fatalf("Tell() overflow message error = %v", err)
		}
	}
	select {
	case record := <-logs:
		if record.Message != "actor mailbox overflow" {
			t.Fatalf("log message = %q, want actor mailbox overflow", record.Message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait actor mailbox overflow log timed out")
	}
}

type actorEcho struct {
	received chan<- txactor.Delivery
}

type blockingActor struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (actor blockingActor) Receive(ctx protoactor.Context) {
	if _, ok := ctx.Message().(*actorpb.Delivery); !ok {
		return
	}
	select {
	case actor.started <- struct{}{}:
	default:
	}
	<-actor.release
}

type actorLogHandler struct {
	records chan<- slog.Record
}

func (handler actorLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (handler actorLogHandler) Handle(_ context.Context, record slog.Record) error {
	select {
	case handler.records <- record:
	default:
	}
	return nil
}

func (handler actorLogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return handler
}

func (handler actorLogHandler) WithGroup(string) slog.Handler {
	return handler
}

func (actor actorEcho) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	delivery, err := txactor.DeliveryFromProto(message)
	if err != nil {
		return
	}
	if actor.received != nil {
		actor.received <- delivery
	}
	payload := ""
	if len(delivery.Payload) > 0 {
		payload = fmt.Sprint(delivery.Payload[0])
	}
	if ctx.Sender() != nil && !message.GetRequireAcceptance() {
		ctx.Respond(wrapperspb.String(delivery.MessageType + ":" + payload))
	}
}
