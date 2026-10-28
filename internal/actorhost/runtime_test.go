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

	"github.com/ngnl47/tx/modules/discovery"
)

func TestActivationSharedAndIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var loads atomic.Int32
		release := make(chan struct{})
		r := New(32, time.Second, func(ctx Context) (*Behavior, error) {
			loads.Add(1)
			if ctx.Identity.ID == "slow" {
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
		old := handle
		if _, err := r.Call(context.Background(), Identity{"room", "1"}, "new"); err != nil {
			t.Fatal(err)
		}
		old.Deactivate()
		old.Deactivate()
		if state := r.Probe(Identity{"room", "1"}); state.State != Ready {
			t.Fatalf("旧激活重复卸载不能关闭新激活: %+v", state)
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

// 先同步关闸，框架才可等待其他入口；已接受任务与回包仍属于排水范围。
func TestBeginDrainRetainsAcceptedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release, reply := make(chan struct{}), make(chan struct{})
		var handle Context
		r := New(2, time.Second, func(ctx Context) (*Behavior, error) {
			handle = ctx
			return &Behavior{Receive: func(_ Context, msg any) (any, error) { <-release; return msg, nil }}, nil
		})
		first, err := r.Submit(Identity{"role", "1"}, "first")
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		second, err := r.SubmitWithReply(handle.Identity, "second", func(any, error) { <-reply })
		if err != nil {
			t.Fatal(err)
		}
		r.BeginDrain()
		r.BeginDrain()
		if !r.Draining() {
			t.Fatal("关闸状态未生效")
		}
		for _, id := range []Identity{handle.Identity, {"role", "2"}} {
			if _, err := r.Submit(id, nil); !errors.Is(err, ErrDraining) {
				t.Fatalf("关闸后准入: %v", err)
			}
		}
		if _, err := handle.Post(nil); !errors.Is(err, ErrDeactivated) {
			t.Fatalf("本地投递: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := r.Drain(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("排水取消: %v", err)
		}
		close(release)
		synctest.Wait()
		if value, err := first.Await(context.Background()); err != nil || value != "first" {
			t.Fatalf("已接收任务: %v %v", value, err)
		}
		drained := make(chan error, 1)
		go func() { drained <- r.Drain(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-drained:
			t.Fatalf("回包完成前排水返回: %v", err)
		default:
		}
		close(reply)
		if err := <-drained; err != nil {
			t.Fatal(err)
		}
		if value, err := second.Await(context.Background()); err != nil || value != "second" {
			t.Fatalf("已排队任务: %v %v", value, err)
		}
	})
}

// 初始化没有统一执行期限；调用方和排水超时不能提前释放仍在加载的实体。
func TestSlowInitializationSurvivesWaitAndDrainTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var callbacks []Context
		var ownershipContext context.Context
		load := func(ctx Context) error {
			callbacks = append(callbacks, ctx)
			time.Sleep(2 * time.Second)
			return ctx.Err()
		}
		r := New(2, time.Second, func(ctx Context) (*Behavior, error) {
			if err := load(ctx); err != nil {
				return nil, err
			}
			return &Behavior{
				OnInitAfter: load, OnActivateBefore: load, OnActivateAfter: load,
				Receive: func(ctx Context, message any) (any, error) {
					if _, ok := ctx.Deadline(); !ok {
						t.Error("业务处理期限被意外移除")
					}
					return message, nil
				},
			}, nil
		})
		defer r.Drain(context.Background())
		r.SetActivationLimits(ActivationLimits{Actors: 1})
		r.BindSlots("world-host", initializationSlotReader(func(ctx context.Context, slot string) (discovery.SlotAssignment, error) {
			ownershipContext = ctx
			return discovery.SlotAssignment{SlotID: slot, RuntimeInstanceID: "world-host"}, nil
		}), nil)
		future, err := r.Submit(Identity{"world", "1"}, "ready")
		if err != nil {
			t.Fatal(err)
		}
		wait, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if _, err := future.Await(wait); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("等待超时: %v", err)
		}
		drain, cancelDrain := context.WithTimeout(context.Background(), time.Second)
		defer cancelDrain()
		if err := r.Drain(drain); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("排水应等待加载完成: %v", err)
		}
		if usage := r.ActivationUsage(); usage.Initializing != 1 || usage.Actors != 1 {
			t.Fatalf("加载未结束就释放容量: %+v", usage)
		}
		if value, err := future.Await(context.Background()); err != nil || value != "ready" {
			t.Fatalf("慢初始化后的已接受消息: %v, %v", value, err)
		}
		if err := r.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, ok := ownershipContext.Deadline(); !ok || !errors.Is(ownershipContext.Err(), context.Canceled) {
			t.Fatalf("归属查询应独立限时并及时取消: deadline=%v, err=%v", ok, ownershipContext.Err())
		}
		if len(callbacks) != 4 {
			t.Fatalf("初始化回调次数: %d", len(callbacks))
		}
		for _, ctx := range callbacks {
			if _, ok := ctx.Deadline(); ok || !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("初始化回调应只在结束时取消: deadline=%v, err=%v", ok, ctx.Err())
			}
		}
		if usage := r.ActivationUsage(); usage != (ActivationUsage{}) {
			t.Fatalf("排水后容量未释放: %+v", usage)
		}
	})
}

type initializationSlotReader func(context.Context, string) (discovery.SlotAssignment, error)

func (read initializationSlotReader) ResolveSlot(ctx context.Context, slot string) (discovery.SlotAssignment, error) {
	return read(ctx, slot)
}

// 同进程调用也必须使用接收方作用域，不能继承发送方的运行时。
func TestActivationContextBelongsToReceiver(t *testing.T) {
	type key struct{}
	receiver := context.WithValue(context.Background(), key{}, "receiver")
	var stages []string
	check := func(c Context) error {
		if c.Value(key{}) != "receiver" {
			t.Errorf("接收方作用域丢失: %v", c.Value(key{}))
		}
		return nil
	}
	r := New(8, time.Second, func(c Context) (*Behavior, error) {
		_ = check(c)
		stages = append(stages, "factory")
		return &Behavior{OnInitAfter: check, Receive: func(c Context, _ any) (any, error) {
			stages = append(stages, "receive")
			return c.Value(key{}), check(c)
		}, OnDeactivateBefore: check, OnDeactivateAfter: check}, nil
	})
	r.BindContext(func(context.Context, Identity) context.Context { return receiver })
	caller := context.WithValue(t.Context(), key{}, "sender")
	got, err := r.Call(caller, Identity{Type: "role", ID: "1"}, nil)
	if err != nil || got != "receiver" {
		t.Fatalf("value=%v err=%v", got, err)
	}
	if err := r.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(stages) != 2 {
		t.Fatal(stages)
	}
}
