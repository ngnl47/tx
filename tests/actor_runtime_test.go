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
	response, err := system.Root.RequestFuture(pid, "ping", 5*time.Second).Result()
	if err != nil {
		t.Fatalf("RequestFuture() error = %v", err)
	}
	if response != "pong" {
		t.Fatalf("response = %#v, want pong", response)
	}
}

type actorEcho struct{}

func (actorEcho) Receive(ctx protoactor.Context) {
	if message, ok := ctx.Message().(string); ok && message == "ping" {
		ctx.Respond("pong")
	}
}
