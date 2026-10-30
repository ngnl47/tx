package actorplacement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	txactor "github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
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

func TestFirstWaiterCancellationDoesNotCancelSharedQuery(t *testing.T) {
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { tasks.Cancel(); tasks.Wait() }()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	var reads atomic.Int32
	var query context.Context
	cache, err := NewCache(tasks, func(ctx context.Context, _ txactor.Identity) (string, error) {
		reads.Add(1)
		query = ctx
		close(entered)
		<-release
		return "group", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	id := txactor.Identity{Type: "role", ID: "7"}
	firstCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := cache.Resolve(firstCtx, id); first <- err }()
	<-entered
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if query.Err() != nil {
		t.Fatalf("首个等待者取消影响共享查询: %v", query.Err())
	}
	finish()
	for range 2 {
		if got, err := cache.Resolve(t.Context(), id); err != nil || got != "group" {
			t.Fatalf("后续等待者应取得共享结果: %s %v", got, err)
		}
	}
	if reads.Load() != 1 {
		t.Fatal("共享查询未复用或成功结果未缓存")
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

type warningCounter struct {
	slog.Handler
	warnings atomic.Int32
}

func (h *warningCounter) Handle(_ context.Context, record slog.Record) error {
	if record.Level == slog.LevelWarn {
		h.warnings.Add(1)
	}
	return nil
}

func TestColdQueryWarningDoesNotRejectOrSpam(t *testing.T) {
	logger := &warningCounter{Handler: slog.NewTextHandler(io.Discard, nil)}
	previous := txlog.Default()
	txlog.SetDefault(slog.New(logger))
	t.Cleanup(func() { txlog.SetDefault(previous) })
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { tasks.Cancel(); tasks.Wait() }()
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	const queries = queryWarningThreshold + 2
	entered := make(chan struct{}, queries)
	cache, err := NewCache(tasks, func(_ context.Context, id txactor.Identity) (string, error) {
		if id.ID != "hot" {
			entered <- struct{}{}
			<-release
		}
		return "group", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	hot := txactor.Identity{Type: "role", ID: "hot"}
	if _, err := cache.Resolve(t.Context(), hot); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, queries)
	for i := range queries {
		id := txactor.Identity{Type: "role", ID: fmt.Sprint(i)}
		go func() { _, err := cache.Resolve(t.Context(), id); results <- err }()
		select {
		case <-entered:
		case err := <-results:
			t.Fatalf("超过告警阈值不应拒绝查询: %v", err)
		case <-time.After(time.Second):
			t.Fatal("查询未启动")
		}
	}
	if got, err := cache.Resolve(t.Context(), hot); err != nil || got != "group" {
		t.Fatalf("告警阈值影响缓存命中: %s %v", got, err)
	}
	finish()
	for range queries {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := logger.warnings.Load(); got != 1 {
		t.Fatalf("一次超阈值突发应仅告警一次: %d", got)
	}
}
