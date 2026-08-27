package tests

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	txprotocol "github.com/ngnl47/tx/protocol"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	if config.MaxPendingCalls != txactor.DefaultMaxPendingCalls {
		t.Fatalf("Normalize() max pending calls = %d, want %d", config.MaxPendingCalls, txactor.DefaultMaxPendingCalls)
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

func TestActorRuntimeRejectsAcceptedBeforeStart(t *testing.T) {
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
	if err := runtime.TellAccepted(ctx, pid, txactor.Delivery{MessageType: "battle.tick"}); !errors.Is(err, txactor.ErrRuntimeNotStarted) {
		t.Fatalf("TellAccepted() error = %v, want ErrRuntimeNotStarted", err)
	}
}

func TestActorRuntimeRequiresCallDeadline(t *testing.T) {
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
	if _, err := runtime.CallFuture(context.Background(), pid, txactor.Delivery{MessageType: "battle.tick"}); !errors.Is(err, txactor.ErrRequestDeadlineRequired) {
		t.Fatalf("CallFuture() error = %v, want ErrRequestDeadlineRequired", err)
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

func TestActorRuntimeLifecycleIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the actor integration test")
	}
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	received := make(chan txactor.Delivery, 3)
	acceptedStarted := make(chan struct{}, 1)
	acceptedRelease := make(chan struct{})
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "tx-actor-integration",
		Host:        "127.0.0.1",
		Port:        0,
		Etcd: txactor.EtcdConfig{
			Endpoints:   []string{endpoint},
			Prefix:      fmt.Sprintf("/tx/tests/actor-%d", time.Now().UnixNano()),
			DialTimeout: time.Second,
		},
	}, txactor.MustKind("echo", func() protoactor.Actor {
		return actorEcho{received: received, acceptedStarted: acceptedStarted, acceptedRelease: acceptedRelease}
	}))
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
	callCtx, cancelCall := context.WithTimeout(ctx, 5*time.Second)
	defer cancelCall()
	result, err := runtime.Call(callCtx, pid, txactor.Delivery{MessageType: "role.ping", CommandID: 7, Payload: []byte{7}})
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if result.MessageType != "role.pong" || result.CommandID != 8 || string(result.Payload) != "role.ping:7" {
		t.Fatalf("Call() result = %#v", result)
	}
	select {
	case receivedDelivery := <-received:
		if receivedDelivery.MessageType != "role.ping" || string(receivedDelivery.Payload) != string([]byte{7}) {
			t.Fatalf("request delivery = %#v", receivedDelivery)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait actor request delivery timed out")
	}
	futureCtx, cancelFuture := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFuture()
	future, err := runtime.CallFuture(futureCtx, pid, txactor.Delivery{MessageType: "role.future", CommandID: 9, Payload: []byte{9}})
	if err != nil {
		t.Fatalf("CallFuture() error = %v", err)
	}
	futureResult, err := future.Await(futureCtx)
	if err != nil {
		t.Fatalf("Future.Await() error = %v", err)
	}
	if futureResult.MessageType != "role.pong" || futureResult.CommandID != 10 || string(futureResult.Payload) != "role.future:9" {
		t.Fatalf("Future.Await() result = %#v", futureResult)
	}
	select {
	case receivedDelivery := <-received:
		if receivedDelivery.MessageType != "role.future" || string(receivedDelivery.Payload) != string([]byte{9}) {
			t.Fatalf("future delivery = %#v", receivedDelivery)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait actor future delivery timed out")
	}
	acceptCtx, cancelAccept := context.WithTimeout(ctx, 5*time.Second)
	defer cancelAccept()
	if err := runtime.TellAccepted(acceptCtx, pid, txactor.Delivery{MessageType: "role.accept", CommandID: 8, Payload: []byte{8}}); err != nil {
		t.Fatalf("TellAccepted() error = %v", err)
	}
	select {
	case <-acceptedStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("TellAccepted waited for business completion")
	}
	close(acceptedRelease)
	select {
	case receivedDelivery := <-received:
		if receivedDelivery.MessageType != "role.accept" || receivedDelivery.CommandID != 8 || string(receivedDelivery.Payload) != string([]byte{8}) {
			t.Fatalf("accepted delivery = %#v", receivedDelivery)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait actor accepted delivery timed out")
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

func TestActorLifecycleDoesNotRewriteEtcdTopology(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the actor integration test")
	}
	basePrefix := fmt.Sprintf("/tx/tests/actor-churn-%d", time.Now().UnixNano())
	actorPrefix := basePrefix + "/actors"
	discoveryPrefix := basePrefix + "/discovery"
	slotPrefix := discoveryPrefix + "/slots/"

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("create etcd client: %v", err)
	}
	defer client.Close()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, cleanupErr := client.Delete(cleanupCtx, basePrefix, clientv3.WithPrefix()); cleanupErr != nil {
			t.Errorf("cleanup etcd prefix: %v", cleanupErr)
		}
	}()

	registry, err := discovery.NewEtcdRegistry(context.Background(), discovery.EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      discoveryPrefix,
		LeaseTTL:    3 * time.Second,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewEtcdRegistry() error = %v", err)
	}
	defer registry.Close()
	assignment := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "game-runtime-1",
		Generation:        1,
	}
	if err := registry.Slots().CreateSlot(context.Background(), assignment); err != nil {
		t.Fatalf("CreateSlot() error = %v", err)
	}

	root := lifecycle.NewWaitGroupContext(context.Background())
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "tx-actor-churn-integration",
		Host:        "127.0.0.1",
		Port:        0,
		Etcd: txactor.EtcdConfig{
			Endpoints:   []string{endpoint},
			Prefix:      actorPrefix,
			DialTimeout: time.Second,
		},
	},
		txactor.MustKind("virtual-churn", func() protoactor.Actor { return actorChurnProbe{} }),
		txactor.MustFixedKind("fixed-churn", func() protoactor.Actor { return actorChurnProbe{} }),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Start(root); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if shutdownErr := runtime.Shutdown(); shutdownErr != nil {
			t.Errorf("Shutdown() error = %v", shutdownErr)
		}
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	}()

	memberBefore := readEtcdKeySnapshot(t, client, actorPrefix)
	slotBefore := readEtcdKeySnapshot(t, client, slotPrefix)
	if len(memberBefore) != 1 {
		t.Fatalf("actor member keys = %d, want 1", len(memberBefore))
	}
	if len(slotBefore) != 1 {
		t.Fatalf("slot keys = %d, want 1", len(slotBefore))
	}

	endpointAddress, err := runtime.Endpoint()
	if err != nil {
		t.Fatalf("Endpoint() error = %v", err)
	}
	fixedHost, err := txactor.FixedKindPID(endpointAddress, "fixed-churn")
	if err != nil {
		t.Fatalf("FixedKindPID() error = %v", err)
	}
	for index := uint64(1); index <= 64; index++ {
		virtual, resolveErr := runtime.VirtualPID("virtual-churn", fmt.Sprintf("role-%d", index))
		if resolveErr != nil {
			t.Fatalf("VirtualPID(%d) error = %v", index, resolveErr)
		}
		callCtx, cancelCall := context.WithTimeout(root, 5*time.Second)
		if _, callErr := runtime.Call(callCtx, virtual, txactor.Delivery{MessageType: "actor.churn", CommandID: index}); callErr != nil {
			cancelCall()
			t.Fatalf("Call virtual Actor %d error = %v", index, callErr)
		}
		if _, callErr := runtime.Call(callCtx, fixedHost, txactor.Delivery{
			MessageType: "actor.churn",
			CommandID:   index,
			RouteKey:    txprotocol.Uint64RouteKey(index),
		}); callErr != nil {
			cancelCall()
			t.Fatalf("Call fixed Actor %d error = %v", index, callErr)
		}
		cancelCall()
	}

	memberAfter := readEtcdKeySnapshot(t, client, actorPrefix)
	slotAfter := readEtcdKeySnapshot(t, client, slotPrefix)
	if !reflect.DeepEqual(memberAfter, memberBefore) {
		t.Fatalf("actor churn changed member keys: before=%#v after=%#v", memberBefore, memberAfter)
	}
	if !reflect.DeepEqual(slotAfter, slotBefore) {
		t.Fatalf("actor churn changed slot keys: before=%#v after=%#v", slotBefore, slotAfter)
	}
}

func TestActorRuntimeStartRequiresWaitGroupContext(t *testing.T) {
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "tx-actor-lifecycle-required",
		Host:        "127.0.0.1",
		Port:        0,
		Etcd: txactor.EtcdConfig{
			Endpoints:   []string{"127.0.0.1:1"},
			Prefix:      "/tx/tests/actor-lifecycle-required",
			DialTimeout: time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Start(context.Background()); !errors.Is(err, txactor.ErrLifecycleContextRequired) {
		t.Fatalf("Start() error = %v, want %v", err, txactor.ErrLifecycleContextRequired)
	}
}

func TestActorRuntimeDrainWaitsForQueuedMessages(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the actor integration test")
	}
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	processed := make(chan uint64, 2)
	runtime, err := txactor.New(txactor.Config{
		ClusterName:     "tx-actor-lifecycle",
		Host:            "127.0.0.1",
		Port:            0,
		MailboxCapacity: 4,
		Etcd: txactor.EtcdConfig{
			Endpoints:   []string{endpoint},
			Prefix:      fmt.Sprintf("/tx/tests/actor-lifecycle-%d", time.Now().UnixNano()),
			DialTimeout: time.Second,
		},
	}, txactor.MustKind("blocked", func() protoactor.Actor {
		return &lifecycleBlockingActor{started: started, release: release, processed: processed}
	}))
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
	})

	pid, err := runtime.VirtualPID("blocked", "role-470001")
	if err != nil {
		t.Fatalf("VirtualPID() error = %v", err)
	}
	if err := runtime.Tell(pid, txactor.Delivery{MessageType: "role.first", CommandID: 1}); err != nil {
		t.Fatalf("Tell() first message error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("wait first actor message timed out")
	}
	if err := runtime.Tell(pid, txactor.Delivery{MessageType: "role.second", CommandID: 2}); err != nil {
		t.Fatalf("Tell() second message error = %v", err)
	}

	drained := make(chan error, 1)
	go func() {
		drained <- runtime.Drain(context.Background())
	}()
	select {
	case err := <-drained:
		t.Fatalf("Drain() returned while Actor mailbox still had accepted work: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	for commandID := uint64(1); commandID <= 2; commandID++ {
		select {
		case got := <-processed:
			if got != commandID {
				t.Fatalf("processed command ID = %d, want %d", got, commandID)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("wait command %d timed out", commandID)
		}
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain() did not return after queued Actor work completed")
	}
	if err := runtime.Tell(pid, txactor.Delivery{MessageType: "role.third", CommandID: 3}); !errors.Is(err, txactor.ErrRuntimeDraining) {
		t.Fatalf("Tell() after Drain error = %v, want %v", err, txactor.ErrRuntimeDraining)
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
	}, txactor.MustKind("blocked", func() protoactor.Actor {
		return blockingActor{started: started, release: release}
	}))
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
	received        chan<- txactor.Delivery
	acceptedStarted chan<- struct{}
	acceptedRelease <-chan struct{}
}

type actorChurnProbe struct{}

func (actorChurnProbe) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	delivery, err := txactor.DeliveryFromProto(message)
	if err != nil {
		return
	}
	if delivery.WantsResult() {
		if err := txactor.Respond(ctx, delivery, txactor.Result{MessageType: "actor.churn.done", CommandID: delivery.CommandID}, nil); err != nil {
			panic(err)
		}
	}
	ctx.Stop(ctx.Self())
}

type etcdKeyState struct {
	Value       string
	ModRevision int64
}

func readEtcdKeySnapshot(t *testing.T, client *clientv3.Client, prefix string) map[string]etcdKeyState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("read etcd prefix %q: %v", prefix, err)
	}
	snapshot := make(map[string]etcdKeyState, len(response.Kvs))
	for _, item := range response.Kvs {
		snapshot[string(item.Key)] = etcdKeyState{
			Value:       string(item.Value),
			ModRevision: item.ModRevision,
		}
	}
	return snapshot
}

type blockingActor struct {
	started chan<- struct{}
	release <-chan struct{}
}

type lifecycleBlockingActor struct {
	started   chan<- struct{}
	release   <-chan struct{}
	processed chan<- uint64
	first     bool
}

func (actor *lifecycleBlockingActor) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	if !actor.first {
		actor.first = true
		actor.started <- struct{}{}
		<-actor.release
	}
	actor.processed <- message.GetCommandId()
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
	if delivery.MessageType == "role.accept" && actor.acceptedRelease != nil {
		actor.acceptedStarted <- struct{}{}
		<-actor.acceptedRelease
	}
	payload := ""
	if len(delivery.Payload) > 0 {
		payload = fmt.Sprint(delivery.Payload[0])
	}
	if delivery.WantsResult() {
		if err := txactor.Respond(ctx, delivery, txactor.Result{
			MessageType: "role.pong",
			CommandID:   delivery.CommandID + 1,
			Payload:     []byte(delivery.MessageType + ":" + payload),
		}, nil); err != nil {
			panic(err)
		}
	}
}
