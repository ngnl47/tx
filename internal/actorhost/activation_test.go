package actorhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestConcurrentActivationReservation(t *testing.T) {
	for name, limits := range map[string]ActivationLimits{
		"actors":          {Actors: 1},
		"initializations": {Initializations: 1},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var admitted atomic.Int32
				r := New(4, time.Second, func(Context) (*Behavior, error) {
					<-release
					return &Behavior{Receive: func(Context, any) (any, error) { return nil, nil }}, nil
				})
				r.SetActivationLimits(limits)
				var group sync.WaitGroup
				for i := range 32 {
					group.Go(func() {
						_, err := r.EnsureActivation(Identity{"world", fmt.Sprint(i)})
						if err == nil {
							admitted.Add(1)
						} else if !errors.Is(err, ErrCapacity) {
							t.Error(err)
						}
					})
				}
				group.Wait()
				if admitted.Load() != 1 {
					t.Fatalf("并发预留超额: %d", admitted.Load())
				}
				close(release)
				_ = r.Drain(context.Background())
				if got := r.ActivationUsage(); got != (ActivationUsage{}) {
					t.Fatal(got)
				}
			})
		})
	}
}

func TestActivationProbeAndCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		load, cleanup := make(chan struct{}), make(chan struct{})
		var loads atomic.Int32
		var pressure atomic.Bool
		r := New(4, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			<-load
			return &Behavior{
				Receive:           func(c Context, value any) (any, error) { c.Deactivate(); return value, nil },
				OnDeactivateAfter: func(Context) error { <-cleanup; return nil },
			}, nil
		})
		r.SetActivationLimits(ActivationLimits{Actors: 1, Initializations: 1, Admit: func() bool { return !pressure.Load() }})
		id, other := Identity{"world", "51"}, Identity{"world", "52"}
		if r.Probe(id).State != Unloaded || loads.Load() != 0 {
			t.Fatal("只读探测不能加载")
		}
		for range 10 {
			status, err := r.EnsureActivation(id)
			if err != nil || status.State != Initializing {
				t.Fatalf("共同初始化: %+v %v", status, err)
			}
		}
		synctest.Wait()
		if r.Probe(id).State != Initializing || loads.Load() != 1 {
			t.Fatal("初始化中的探测不能排队或重复加载")
		}
		if state, err := r.RetryActivation(id); err != nil || state.State != Initializing || loads.Load() != 1 {
			t.Fatalf("重试不能替换正在加载的激活: %+v %v", state, err)
		}
		if _, err := r.EnsureActivation(other); !errors.Is(err, ErrCapacity) {
			t.Fatal(err)
		}
		if got := r.ActivationUsage(); got.Actors != 1 || got.Initializing != 1 {
			t.Fatal(got)
		}
		close(load)
		synctest.Wait()
		if r.Probe(id).State != Ready || r.ActivationUsage().Initializing != 0 {
			t.Fatal("完成初始化后未释放初始化并发额度")
		}
		if _, err := r.EnsureActivation(other); !errors.Is(err, ErrCapacity) {
			t.Fatal("就绪实体仍占承载名额", err)
		}
		pressure.Store(true)
		if _, err := r.Call(context.Background(), id, "finish"); err != nil {
			t.Fatal("压力不应阻断已激活实体", err)
		}
		synctest.Wait()
		if r.Probe(id).State != Closing || r.ActivationUsage().Actors != 1 {
			t.Fatal("清理完成前归还了预算")
		}
		if state, err := r.RetryActivation(id); err != nil || state.State != Closing || loads.Load() != 1 {
			t.Fatalf("重试不能绕过清理: %+v %v", state, err)
		}
		close(cleanup)
		synctest.Wait()
		if r.ActivationUsage().Actors != 0 || r.Probe(id).State != Unloaded {
			t.Fatal("清理后预算未归还")
		}
		if _, err := r.EnsureActivation(other); !errors.Is(err, ErrCapacity) {
			t.Fatal("压力期间放入新世界", err)
		}
		pressure.Store(false)
		if _, err := r.EnsureActivation(other); err != nil {
			t.Fatal(err)
		}
		if err := r.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := r.EnsureActivation(id); !errors.Is(err, ErrDraining) {
			t.Fatal("探测重新打开了排水闸门", err)
		}
	})
}

func TestFailedActivationIsObservableAndRetryIsExplicit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var loads atomic.Int32
		r := New(4, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			return &Behavior{Receive: func(Context, any) (any, error) { return "ok", nil },
				OnInitAfter: func(Context) error { return ErrActivationDenied }}, nil
		})
		r.SetActivationLimits(ActivationLimits{Actors: 1, Initializations: 1})
		id := Identity{"room", "ended"}
		f, err := r.Submit(id, "buff")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Await(context.Background()); !errors.Is(err, ErrActivationDenied) {
			t.Fatal(err)
		}
		synctest.Wait()
		for range 3 {
			status, err := r.EnsureActivation(id)
			if err != nil || status.State != Failed || !errors.Is(status.Failure, ErrActivationDenied) {
				t.Fatalf("失败状态: %+v %v", status, err)
			}
			if _, err = r.Submit(id, "buff"); !errors.Is(err, ErrActivationDenied) {
				t.Fatal(err)
			}
		}
		if loads.Load() != 1 || r.ActivationUsage().Actors != 0 || r.ActivationUsage().Initializing != 0 {
			t.Fatal("反复初始化失败或额度泄漏")
		}
		if status, err := r.RetryActivation(id); err != nil || status.State != Initializing {
			t.Fatalf("显式重试: %+v %v", status, err)
		}
		synctest.Wait()
		if loads.Load() != 2 {
			t.Fatal(loads.Load())
		}
		_ = r.Drain(context.Background())
	})
}
