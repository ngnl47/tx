package msgq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

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
	closed       atomic.Bool
	closeStarted chan struct{}
	closeFinish  chan struct{}
}

func (client *rotatingSessionClient) Close() error {
	if client.closeStarted != nil {
		close(client.closeStarted)
		<-client.closeFinish
	}
	client.closed.Store(true)
	return nil
}

func TestSessionClientRouterRotatesClientWhenInstanceChangesAtSameEndpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dialer := &rotatingSessionDialer{}
		directory := discovery.NewServiceDirectory()
		router := &SessionClientRouter{dialer: dialer, connections: instanceconn.New[SessionClient](directory)}
		defer router.Close()
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
		previous := old.(*rotatingSessionClient)
		previous.closeStarted, previous.closeFinish = make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(previous.closeFinish) })
		defer release()
		if err := directory.Replace([]discovery.Instance{second}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-previous.closeStarted:
		default:
			t.Fatal("目录更新后未开始退役关闭")
		}
		newClient, err := router.connection(context.Background(), second)
		if err != nil || newClient == old || dialer.calls.Load() != 2 {
			t.Fatalf("new instance client = %v, %v, dials %d", newClient != old, err, dialer.calls.Load())
		}
		if previous.closed.Load() {
			t.Fatal("旧连接的受控关闭应仍在等待")
		}
		release()
		synctest.Wait()
		if !previous.closed.Load() {
			t.Fatal("old instance client remained open")
		}
		if err := router.Close(); err != nil || !newClient.(*rotatingSessionClient).closed.Load() {
			t.Fatalf("current instance close = %v", err)
		}
	})
}
