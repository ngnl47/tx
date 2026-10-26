package actorplacement

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	txactor "github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
)

func TestCanceledSlotLoadStillBelongsToLifecycle(t *testing.T) {
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { tasks.Cancel(); tasks.Wait() }()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	cache, err := NewCache(tasks, func(context.Context, txactor.Identity) (string, error) {
		close(entered)
		<-release // 模拟尚未响应取消的存储读取。
		return "slggame-1001", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	id := txactor.Identity{Type: "role", ID: "i:1"}
	go func() { _, err := cache.Resolve(ctx, id); result <- err }()
	<-entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("请求取消未退出")
	}
	tasks.Cancel()
	stopped := make(chan struct{})
	go func() { tasks.Wait(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("实际查询未结束，生命周期却已收尾")
	case <-time.After(20 * time.Millisecond):
	}
	finish()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("查询结束后未收尾")
	}
	if _, err := cache.Resolve(t.Context(), id); !errors.Is(err, lifecycle.ErrStopping) {
		t.Fatalf("取消的结果被缓存: %v", err)
	}
}

func TestSharedQueryWaiterCancelsIndependently(t *testing.T) {
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { tasks.Cancel(); tasks.Wait() }()
	entered, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	cache, err := NewCache(tasks, func(context.Context, txactor.Identity) (string, error) {
		reads.Add(1)
		close(entered)
		<-release
		return "group", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	id := txactor.Identity{Type: "role", ID: "7"}
	first := make(chan error, 1)
	go func() { _, err := cache.Resolve(t.Context(), id); first <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = cache.Resolve(ctx, id)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 1 {
		t.Fatal("同身份并发查询未合并")
	}
}

func TestQueryDeadlineDoesNotForgetBlockedWork(t *testing.T) {
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { tasks.Cancel(); tasks.Wait() }()
	release := make(chan struct{})
	defer close(release)
	var reads atomic.Int32
	cache, err := NewCache(tasks, func(context.Context, txactor.Identity) (string, error) { reads.Add(1); <-release; return "group", nil })
	if err != nil {
		t.Fatal(err)
	}
	id := txactor.Identity{Type: "role", ID: "7"}
	done := make(chan error, 1)
	go func() { _, err := cache.Resolve(t.Context(), id); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(QueryTimeout + time.Second):
		t.Fatal("冷查询未按框架期限退出等待")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, _ = cache.Resolve(ctx, id)
	if reads.Load() != 1 {
		t.Fatal("超时后错误释放 pending，启动了第二次实际查询")
	}
}
