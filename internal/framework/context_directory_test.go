package framework

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/ngnl47/tx/internal/instanceconn"
	"github.com/ngnl47/tx/modules/discovery"
)

type contextDirectoryConnection struct{ closed atomic.Bool }

func (c *contextDirectoryConnection) Close() error { c.closed.Store(true); return nil }

func TestServiceContextDirectoryRetiresConnectionsAfterValidUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		directory := discovery.NewServiceDirectory()
		instance := discovery.Instance{ID: "old", Endpoint: "127.0.0.1:19000", Services: []string{"msgq"}, Status: discovery.StatusRunning}
		if err := directory.Replace([]discovery.Instance{instance}); err != nil {
			t.Fatal(err)
		}
		view := newServiceContext(&runtime{directory: directory}, nil).Directory()
		cache := instanceconn.New[*contextDirectoryConnection](view)
		defer cache.Close()
		connection := &contextDirectoryConnection{}
		if _, err := cache.Get(t.Context(), instance, func(context.Context, discovery.Instance) (*contextDirectoryConnection, error) { return connection, nil }); err != nil {
			t.Fatal(err)
		}
		directory.Invalidate()
		synctest.Wait()
		if connection.closed.Load() {
			t.Fatal("只读目录视图把失联误判为退役")
		}
		instance.ID, instance.Endpoint = "new", "127.0.0.1:19001"
		if err := directory.Replace([]discovery.Instance{instance}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !connection.closed.Load() {
			t.Fatal("只读目录视图未传递有效成员退役")
		}
	})
}
