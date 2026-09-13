package actorhost

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestIdlePassivationKeepsBusyActorAndDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		cleanup := make(chan struct{})
		var handle Context
		loads, handled, removed := 0, 0, 0
		r := New(4, time.Minute, func(c Context) (*Behavior, error) {
			handle = c
			loads++
			return &Behavior{
				IdlePassivation: time.Second,
				Receive: func(_ Context, message any) (any, error) {
					if message == "block" {
						<-release
					}
					handled++
					return handled, nil
				},
				OnDeactivateAfter: func(Context) error { <-cleanup; removed++; return nil },
			}, nil
		})
		defer r.Drain(context.Background())
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
			select {
			case <-cleanup:
			default:
				close(cleanup)
			}
		}()
		id := Identity{"role", "1"}
		if _, err := r.Call(context.Background(), id, "first"); err != nil {
			t.Fatal(err)
		}
		blocked, err := r.Submit(id, "block")
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(2 * time.Second)
		if r.Probe(id).State != Ready {
			t.Fatal("执行期间被空闲卸载")
		}
		queued, err := r.Submit(id, "queued")
		if err != nil {
			t.Fatal(err)
		}
		close(release)
		if _, err := blocked.Await(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := queued.Await(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(500 * time.Millisecond)
		if r.Probe(id).State != Ready {
			t.Fatal("处理完成后未重置空闲时间")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if r.Probe(id).State != Closing {
			t.Fatal("退出回调结束前释放了激活")
		}
		if _, err := r.Submit(id, "premature reload"); !errors.Is(err, ErrDeactivated) {
			t.Fatalf("清理期间重新激活: %v", err)
		}
		close(cleanup)
		synctest.Wait()
		if r.Probe(id).State != Unloaded || handled != 3 || removed != 1 {
			t.Fatalf("state=%v handled=%d removed=%d", r.Probe(id), handled, removed)
		}
		old := handle
		if _, err := r.Call(context.Background(), id, "reload"); err != nil {
			t.Fatal(err)
		}
		if loads != 2 {
			t.Fatalf("loads=%d", loads)
		}
		if _, err := old.Post("stale"); !errors.Is(err, ErrDeactivated) {
			t.Fatalf("旧激活句柄: %v", err)
		}
	})
}

func TestIdlePassivationDisabled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New(4, time.Second, func(Context) (*Behavior, error) {
			return &Behavior{Receive: func(Context, any) (any, error) { return nil, nil }}, nil
		})
		defer r.Drain(context.Background())
		id := Identity{"world", "51"}
		if _, err := r.Call(context.Background(), id, nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Hour)
		if r.Probe(id).State != Ready {
			t.Fatal("未启用空闲卸载却被自动移除")
		}
	})
}
