package goakteval

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

// 只等待 Call 返回会漏掉超时后仍执行、以及已超时但仍在排队的业务。
func TestDrainWaitsForTimedOutAndQueuedWork(t *testing.T) {
	first, second := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	var completed atomic.Int32
	r, err := Start(context.Background(), "drain-queued", 4, 64, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(_ context.Context, d *actorpb.Delivery) (*actorpb.Result, error) {
			if d.Identity.SessionId == 1 {
				close(first)
				<-releaseFirst
			} else {
				close(second)
				<-releaseSecond
			}
			completed.Add(1)
			return &actorpb.Result{MessageType: "done"}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	call := func(session int64) chan error {
		result := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, e := r.Call(ctx, request(session))
			result <- e
		}()
		return result
	}
	a := call(1)
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first not started")
	}
	b := call(2)
	if e := <-a; e == nil {
		t.Error("first must time out")
	}
	if e := <-b; e == nil {
		t.Error("queued call must time out")
	}
	shortDrain := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if e := r.Shutdown(ctx, func(context.Context) error {
			t.Error("flush started before queued business completed")
			return nil
		}); e == nil {
			t.Error("drain completed before business")
		}
		if !r.system.Running() {
			t.Error("drain failure stopped system")
		}
	}
	shortDrain()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := r.Call(ctx, request(3)); !errors.Is(e, lifecycle.ErrDraining) {
		t.Errorf("new admission=%v", e)
	}
	close(releaseFirst)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("queued work lost")
	}
	shortDrain()
	close(releaseSecond)
	if err = r.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if completed.Load() != 2 {
		t.Fatalf("completed=%d", completed.Load())
	}
}

// 加载仍在进行时不能先取得一个不含该 Grain 的快照并宣布排水成功。
func TestDrainWaitsForActivation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r, err := Start(context.Background(), "drain-activation", 4, 64, time.Hour, func(context.Context, int64) (Handler, error) {
		close(entered)
		<-release
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) {
			return &actorpb.Result{MessageType: "done"}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, e := r.Call(ctx, request(1)); result <- e }()
	<-entered
	short, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	err = r.Drain(short)
	stop()
	if err == nil {
		t.Error("activation was not drained")
	}
	close(release)
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	if err = r.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

// 持久化失败后运行时必须保留且关闭准入；第二次收尾成功后才能停掉系统。
func TestShutdownRetainsSystemWhenFlushFails(t *testing.T) {
	var completed atomic.Bool
	r, err := Start(context.Background(), "drain-flush", 4, 64, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) {
			completed.Store(true)
			return &actorpb.Result{MessageType: "done"}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err = r.Call(ctx, request(1)); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("flush unavailable")
	if err = r.Shutdown(ctx, func(context.Context) error {
		if !completed.Load() {
			t.Error("flush before handler completion")
		}
		return failure
	}); !errors.Is(err, failure) {
		t.Fatalf("shutdown=%v", err)
	}
	if !r.system.Running() {
		t.Fatal("flush failure released system")
	}
	if _, err = r.Call(ctx, request(2)); !errors.Is(err, lifecycle.ErrDraining) {
		t.Errorf("new admission=%v", err)
	}
	if err = r.Shutdown(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if r.system.Running() {
		t.Fatal("successful shutdown retained system")
	}
	if err = r.Shutdown(ctx, func(context.Context) error { t.Error("flush repeated after stop"); return nil }); err != nil {
		t.Fatal(err)
	}
}
