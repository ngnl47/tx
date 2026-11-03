package instanceconn

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/ngnl47/tx/modules/discovery"
)

type testConnection struct{ closed atomic.Bool }

func (c *testConnection) Close() error { c.closed.Store(true); return nil }

func TestCacheRotatesRestartedInstanceAndRejectsLateOldDial(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	old := discovery.Instance{ID: "old", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
	newInstance := old
	newInstance.ID = "new"
	if err := directory.Replace([]discovery.Instance{old}); err != nil {
		t.Fatal(err)
	}
	cache := New[*testConnection](directory)
	started := make(chan struct{})
	finish := make(chan struct{})
	late := make(chan error, 1)
	lateConnection := &testConnection{}
	go func() {
		_, err := cache.Get(context.Background(), old, func(context.Context, discovery.Instance) (*testConnection, error) {
			close(started)
			<-finish
			return lateConnection, nil
		})
		late <- err
	}()
	<-started
	if err := directory.Replace([]discovery.Instance{newInstance}); err != nil {
		t.Fatal(err)
	}
	current := &testConnection{}
	got, err := cache.Get(context.Background(), newInstance, func(context.Context, discovery.Instance) (*testConnection, error) { return current, nil })
	if err != nil || got != current {
		t.Fatalf("new instance connection = %v, %v", got, err)
	}
	close(finish)
	if err := <-late; !errors.Is(err, ErrStaleInstance) {
		t.Fatalf("late old dial = %v", err)
	}
	if !lateConnection.closed.Load() || current.closed.Load() {
		t.Fatal("late old connection replaced current connection")
	}
	reused, err := cache.Get(context.Background(), newInstance, func(context.Context, discovery.Instance) (*testConnection, error) {
		t.Fatal("unexpected redial")
		return nil, nil
	})
	if err != nil || reused != current {
		t.Fatalf("reuse = %v, %v", reused, err)
	}
	if err := cache.Close(); err != nil || !current.closed.Load() {
		t.Fatalf("close = %v", err)
	}
}

func TestCacheRetiresPreviousIdentity(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	first := discovery.Instance{ID: "first", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
	second := first
	second.ID = "second"
	if err := directory.Replace([]discovery.Instance{first}); err != nil {
		t.Fatal(err)
	}
	// 自定义目录未提供变化通知时，同址身份轮换仍应同步清理旧连接。
	cache := New[*testConnection](struct{ discovery.Directory }{directory})
	defer cache.Close()
	previous := &testConnection{}
	if _, err := cache.Get(context.Background(), first, func(context.Context, discovery.Instance) (*testConnection, error) { return previous, nil }); err != nil {
		t.Fatal(err)
	}
	if err := directory.Replace([]discovery.Instance{second}); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), second, func(context.Context, discovery.Instance) (*testConnection, error) { return &testConnection{}, nil }); err != nil {
		t.Fatal(err)
	}
	if !previous.closed.Load() {
		t.Fatal("previous identity connection remained open")
	}
}

func TestCacheCloseDuringDialClosesLateConnection(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	instance := discovery.Instance{ID: "game-a", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
	if err := directory.Replace([]discovery.Instance{instance}); err != nil {
		t.Fatal(err)
	}
	cache := New[*testConnection](directory)
	started, finish := make(chan struct{}), make(chan struct{})
	created := &testConnection{}
	result := make(chan error, 1)
	go func() {
		_, err := cache.Get(context.Background(), instance, func(context.Context, discovery.Instance) (*testConnection, error) {
			close(started)
			<-finish
			return created, nil
		})
		result <- err
	}()
	<-started
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	close(finish)
	if err := <-result; !errors.Is(err, ErrClosed) || !created.closed.Load() {
		t.Fatalf("late dial after close = %v, closed=%v", err, created.closed.Load())
	}
}

func TestCacheRetiresChangedEndpointWithoutAnotherRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		directory := discovery.NewServiceDirectory()
		old := discovery.Instance{ID: "old", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
		if err := directory.Replace([]discovery.Instance{old}); err != nil {
			t.Fatal(err)
		}
		cache := New[*testConnection](directory)
		defer cache.Close()
		previous := &testConnection{}
		if _, err := cache.Get(t.Context(), old, func(context.Context, discovery.Instance) (*testConnection, error) { return previous, nil }); err != nil {
			t.Fatal(err)
		}
		current := old
		current.ID, current.Endpoint = "new", "127.0.0.1:19001"
		if err := directory.Replace([]discovery.Instance{current}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !previous.closed.Load() {
			t.Fatal("换址退役且没有后续请求时，旧连接仍未关闭")
		}
	})
}

func TestCacheRetirementRequiresValidSnapshotAndAbsentMember(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		directory := discovery.NewServiceDirectory()
		instance := discovery.Instance{ID: "game-a", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
		if err := directory.Replace([]discovery.Instance{instance}); err != nil {
			t.Fatal(err)
		}
		cache := New[*testConnection](directory)
		defer cache.Close()
		connection := &testConnection{}
		if _, err := cache.Get(t.Context(), instance, func(context.Context, discovery.Instance) (*testConnection, error) { return connection, nil }); err != nil {
			t.Fatal(err)
		}
		for _, status := range []discovery.Status{discovery.StatusDraining, discovery.StatusStopping, discovery.StatusStarting} {
			instance.Status = status
			if err := directory.Replace([]discovery.Instance{instance}); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if connection.closed.Load() {
				t.Fatalf("仍在目录中的 %s 实例连接被提前关闭", status)
			}
		}
		directory.Invalidate()
		synctest.Wait()
		if connection.closed.Load() {
			t.Fatal("目录失效被误判为成员退役")
		}
		if err := directory.Replace([]discovery.Instance{{ID: "invalid"}}); err == nil {
			t.Fatal("无效快照被接受")
		}
		synctest.Wait()
		if connection.closed.Load() {
			t.Fatal("无效快照导致旧连接被关闭")
		}
		if err := directory.Replace(nil); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !connection.closed.Load() {
			t.Fatal("恢复后的有效快照未回收缺席成员连接")
		}
	})
}

type blockingCloseConnection struct {
	started chan struct{}
	finish  chan struct{}

	calls atomic.Int64
}

func (c *blockingCloseConnection) Close() error {
	if c.calls.Add(1) == 1 {
		close(c.started)
	}
	<-c.finish
	return nil
}

func TestCacheCloseWaitsForRetirementWithoutBlockingDirectory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		directory := discovery.NewServiceDirectory()
		instance := discovery.Instance{ID: "game-a", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
		if err := directory.Replace([]discovery.Instance{instance}); err != nil {
			t.Fatal(err)
		}
		cache := New[*blockingCloseConnection](directory)
		connection := &blockingCloseConnection{started: make(chan struct{}), finish: make(chan struct{})}
		released := false
		defer func() {
			if !released {
				close(connection.finish)
			}
			_ = cache.Close()
		}()
		if _, err := cache.Get(t.Context(), instance, func(context.Context, discovery.Instance) (*blockingCloseConnection, error) { return connection, nil }); err != nil {
			t.Fatal(err)
		}
		if err := directory.Replace(nil); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-connection.started:
		default:
			t.Fatal("有效快照删除成员后未开始退役清理")
		}
		directory.Invalidate()
		if err := directory.Replace(nil); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 2)
		go func() { done <- cache.Close() }()
		go func() { done <- cache.Close() }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("退役清理未结束时 Close 提前返回: %v", err)
		default:
		}
		if connection.calls.Load() != 1 {
			t.Fatalf("同一连接被重复关闭: %d", connection.calls.Load())
		}
		close(connection.finish)
		released = true
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
	})
}
