package msgq

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ngnl47/tx/internal/instanceconn"
	"github.com/ngnl47/tx/modules/discovery"
)

type rotatingSessionDialer struct{ calls atomic.Int64 }

func (dialer *rotatingSessionDialer) DialSession(_ context.Context, _ discovery.Instance) (SessionClient, error) {
	dialer.calls.Add(1)
	return &rotatingSessionClient{}, nil
}

type rotatingSessionClient struct {
	SessionClient
	closed atomic.Bool
}

func (client *rotatingSessionClient) Close() error {
	client.closed.Store(true)
	return nil
}

func TestSessionClientRouterRotatesClientWhenInstanceChangesAtSameEndpoint(t *testing.T) {
	dialer := &rotatingSessionDialer{}
	directory := discovery.NewServiceDirectory()
	router := &SessionClientRouter{dialer: dialer, connections: instanceconn.New[SessionClient](directory)}
	first := discovery.Instance{ID: "msgq-old", Endpoint: "127.0.0.1:19220", Services: []string{"msgq"}, Status: discovery.StatusRunning}
	second := discovery.Instance{ID: "msgq-new", Endpoint: first.Endpoint}
	second.Services, second.Status = first.Services, first.Status
	if err := directory.Replace([]discovery.Instance{first}); err != nil {
		t.Fatal(err)
	}
	old, err := router.connection(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	reused, err := router.connection(context.Background(), first)
	if err != nil || reused != old || dialer.calls.Load() != 1 {
		t.Fatalf("same instance reuse = %v, %v, dials %d", reused == old, err, dialer.calls.Load())
	}
	if err := directory.Replace([]discovery.Instance{second}); err != nil {
		t.Fatal(err)
	}
	newClient, err := router.connection(context.Background(), second)
	if err != nil || newClient == old || dialer.calls.Load() != 2 {
		t.Fatalf("new instance client = %v, %v, dials %d", newClient != old, err, dialer.calls.Load())
	}
	if !old.(*rotatingSessionClient).closed.Load() {
		t.Fatal("old instance client remained open")
	}
	if err := router.Close(); err != nil || !newClient.(*rotatingSessionClient).closed.Load() {
		t.Fatalf("current instance close = %v", err)
	}
}
