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

func TestActivationSharedAndIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var loads atomic.Int32
		release := make(chan struct{})
		r := New(32, time.Second, func(ctx Context) (*Behavior, error) {
			loads.Add(1)
			if ctx.Identity.Key == "slow" {
				<-release
			}
			value := 0
			return &Behavior{Receive: func(Context, any) (any, error) { value++; return value, nil }}, nil
		})
		defer r.Drain(context.Background())
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				if _, err := r.Call(context.Background(), Identity{"room", "slow"}, nil); err != nil {
					t.Error(err)
				}
			})
		}
		synctest.Wait()
		if got, err := r.Call(context.Background(), Identity{"room", "other"}, nil); err != nil || got != 1 {
			t.Fatalf("独立实体: %v %v", got, err)
		}
		close(release)
		wg.Wait()
		if loads.Load() != 2 {
			t.Fatalf("重复激活: %d", loads.Load())
		}
		if got, err := r.Call(context.Background(), Identity{"room", "slow"}, nil); err != nil || got != 11 {
			t.Fatalf("串行计数: %v %v", got, err)
		}
	})
}

func TestDeactivateDrainsAndRejectsOldHandle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		entered := make(chan struct{})
		var handle Context
		var events []string
		r := New(1, time.Second, func(ctx Context) (*Behavior, error) {
			handle = ctx
			return &Behavior{
				OnInitAfter:      func(Context) error { events = append(events, "init"); return nil },
				OnActivateBefore: func(Context) error { events = append(events, "before"); return nil },
				OnActivateAfter:  func(Context) error { events = append(events, "after"); return nil },
				Receive: func(ctx Context, msg any) (any, error) {
					if msg == "block" {
						close(entered)
						<-release
						ctx.Deactivate()
					}
					events = append(events, msg.(string))
					return msg, nil
				},
				OnDeactivateBefore: func(Context) error { events = append(events, "stop-before"); return errors.New("通知失败") },
				OnDeactivateAfter:  func(Context) error { events = append(events, "stop-after"); return nil },
			}, nil
		})
		first, _ := r.Submit(Identity{"room", "1"}, "block")
		<-entered
		second, _ := r.Submit(Identity{"room", "1"}, "queued")
		if _, err := r.Submit(Identity{"room", "1"}, "overflow"); !errors.Is(err, ErrMailboxFull) {
			t.Fatal(err)
		}
		close(release)
		if _, err := first.Await(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := second.Await(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		want := "[init before after block queued stop-before stop-after]"
		if got := fmt.Sprint(events); got != want {
			t.Fatalf("%s", got)
		}
		if _, err := handle.Post("old"); !errors.Is(err, ErrDeactivated) {
			t.Fatal(err)
		}
		r.Drain(context.Background())
	})
}

func TestFailuresReleaseWaiters(t *testing.T) {
	for _, mode := range []string{"denied", "temporary", "activate", "panic"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var clean atomic.Int32
				temporary := errors.New("读取失败")
				r := New(4, time.Second, func(Context) (*Behavior, error) {
					if mode == "denied" {
						return nil, ErrActivationDenied
					}
					if mode == "temporary" {
						return nil, temporary
					}
					return &Behavior{
						OnActivateAfter: func(Context) error {
							if mode == "activate" {
								return temporary
							}
							return nil
						},
						Receive:           func(Context, any) (any, error) { panic("handler") },
						OnDeactivateAfter: func(Context) error { clean.Add(1); return nil },
					}, nil
				})
				_, err := r.Call(context.Background(), Identity{"room", "1"}, nil)
				if err == nil {
					t.Fatal("失败被吞掉")
				}
				if mode == "denied" && !errors.Is(err, ErrActivationDenied) {
					t.Fatal(err)
				}
				if mode == "temporary" && !errors.Is(err, temporary) {
					t.Fatal(err)
				}
				if mode == "panic" && !errors.Is(err, ErrPanicked) {
					t.Fatal(err)
				}
				r.Drain(context.Background())
				if (mode == "activate" || mode == "panic") && clean.Load() != 1 {
					t.Fatal(clean.Load())
				}
			})
		})
	}
}

func TestBattleDeadlineAndTimerIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		end := time.Now().Add(time.Minute)
		var ends atomic.Int32
		r := New(8, time.Second, func(ctx Context) (*Behavior, error) {
			if !time.Now().Before(end.Add(5 * time.Second)) {
				return nil, ErrActivationDenied
			}
			ctx.After(time.Until(end), "end")
			ctx.After(time.Until(end.Add(5*time.Second)), "unload")
			return &Behavior{Receive: func(ctx Context, msg any) (any, error) {
				switch msg {
				case "end":
					ends.Add(1)
				case "unload":
					ctx.Deactivate()
				case "buff":
					if !time.Now().Before(end) {
						return nil, ErrActivationDenied
					}
				}
				return nil, nil
			}}, nil
		})
		id := Identity{"room", "battle-1"}
		if _, err := r.Call(context.Background(), id, "buff"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if _, err := r.Call(context.Background(), id, "buff"); !errors.Is(err, ErrActivationDenied) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if _, err := r.Call(context.Background(), id, "buff"); !errors.Is(err, ErrActivationDenied) {
			t.Fatal(err)
		}
		if ends.Load() != 1 {
			t.Fatal(ends.Load())
		}
		r.Drain(context.Background())
	})
}

func TestOldTimerCannotWakeReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var loads, ticks atomic.Int32
		var old Context
		r := New(4, time.Second, func(ctx Context) (*Behavior, error) {
			if loads.Add(1) == 1 {
				old = ctx
				ctx.After(time.Minute, "tick")
			}
			return &Behavior{Receive: func(ctx Context, m any) (any, error) {
				if m == "unload" {
					ctx.Deactivate()
				}
				if m == "tick" {
					ticks.Add(1)
				}
				return nil, nil
			}}, nil
		})
		id := Identity{"room", "replace"}
		r.Call(context.Background(), id, "unload")
		synctest.Wait()
		if _, err := r.Call(context.Background(), id, "new"); err != nil {
			t.Fatal(err)
		}
		if _, err := old.Post("tick"); !errors.Is(err, ErrDeactivated) {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if loads.Load() != 2 || ticks.Load() != 0 {
			t.Fatalf("loads=%d ticks=%d", loads.Load(), ticks.Load())
		}
		r.Drain(context.Background())
	})
}

func TestDrainWaitsForCleanupAndCancelledCallerDoesNotCancelWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		cleanup := make(chan struct{})
		finish := make(chan struct{})
		r := New(2, time.Second, func(Context) (*Behavior, error) {
			return &Behavior{
				Receive:            func(Context, any) (any, error) { close(entered); <-release; return "done", nil },
				OnDeactivateBefore: func(ctx Context) error { close(cleanup); <-finish; return nil },
			}, nil
		})
		id := Identity{"room", "drain"}
		f, _ := r.Submit(id, nil)
		<-entered
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := f.Await(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		drained := make(chan error, 1)
		go func() { drained <- r.Drain(context.Background()) }()
		synctest.Wait()
		if _, err := r.Submit(id, nil); !errors.Is(err, ErrDraining) {
			t.Fatal(err)
		}
		close(release)
		<-cleanup
		select {
		case <-drained:
			t.Fatal("清理前提前返回")
		default:
		}
		close(finish)
		if err := <-drained; err != nil {
			t.Fatal(err)
		}
		if result, err := f.Await(context.Background()); err != nil || result != "done" {
			t.Fatalf("%v %v", result, err)
		}
	})
}

func TestActivationFailureKeepsSlotUntilCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cleanup := make(chan struct{})
		release := make(chan struct{})
		var loads atomic.Int32
		r := New(4, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			return &Behavior{
				Receive:           func(Context, any) (any, error) { return nil, nil },
				OnInitAfter:       func(Context) error { return ErrActivationDenied },
				OnDeactivateAfter: func(Context) error { close(cleanup); <-release; return nil },
			}, nil
		})
		id := Identity{"room", "failed"}
		first, _ := r.Submit(id, nil)
		second, _ := r.Submit(id, nil)
		<-cleanup
		for _, f := range []*Future{first, second} {
			if _, err := f.Await(context.Background()); !errors.Is(err, ErrActivationDenied) {
				t.Fatal(err)
			}
		}
		if _, err := r.Submit(id, nil); !errors.Is(err, ErrDeactivated) {
			t.Fatal(err)
		}
		if loads.Load() != 1 {
			t.Fatal(loads.Load())
		}
		close(release)
		r.Drain(context.Background())
	})
}

func TestPanicFailsQueuedWorkWithoutNormalExitRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var notified atomic.Int32
		r := New(2, time.Second, func(Context) (*Behavior, error) {
			return &Behavior{
				Receive:            func(Context, any) (any, error) { close(entered); <-release; panic("broken state") },
				OnDeactivateBefore: func(Context) error { notified.Add(1); return nil },
			}, nil
		})
		id := Identity{"room", "panic"}
		first, _ := r.Submit(id, nil)
		<-entered
		second, _ := r.Submit(id, nil)
		close(release)
		for _, f := range []*Future{first, second} {
			if _, err := f.Await(context.Background()); !errors.Is(err, ErrPanicked) {
				t.Fatal(err)
			}
		}
		r.Drain(context.Background())
		if notified.Load() != 0 {
			t.Fatal("业务状态异常后不执行正常退出 RPC")
		}
	})
}
