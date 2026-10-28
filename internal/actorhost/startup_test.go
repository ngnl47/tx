package actorhost

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/lifecycle"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestStartupControlDoesNotOpenBusinessMailbox(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var phase atomic.Int32
		var loads atomic.Int32
		var self Context
		var received atomic.Int32
		r := New(8, time.Second, func(ctx Context) (*Behavior, error) {
			if ctx.Identity.Type == "world" {
				self = ctx
			}
			loads.Add(1)
			return &Behavior{Receive: func(Context, any) (any, error) { received.Add(1); return "done", nil }}, nil
		})
		defer r.Drain(context.Background())
		r.BindAdmission(func(id Identity, activation bool) error {
			if id.Type == "ready" || phase.Load() == 2 || activation && phase.Load() == 1 {
				return nil
			}
			return lifecycle.ErrNotReady
		})
		id := Identity{Type: "world", ID: "1"}
		if _, err := r.EnsureActivation(id); !errors.Is(err, lifecycle.ErrNotReady) {
			t.Fatal(err)
		}
		if loads.Load() != 0 {
			t.Fatal("装配前执行了工厂")
		}
		phase.Store(1)
		if _, err := r.EnsureActivation(id); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if r.Probe(id).State != Ready {
			t.Fatal("控制加载未完成")
		}
		if _, err := self.Post(nil); !errors.Is(err, lifecycle.ErrNotReady) {
			t.Fatal("Post 绕过启动准入", err)
		}
		self.After(time.Millisecond, nil)
		time.Sleep(50 * time.Millisecond)
		if received.Load() != 0 {
			t.Fatal("启动期间执行了定时消息")
		}
		if _, err := r.Submit(id, nil); !errors.Is(err, lifecycle.ErrNotReady) {
			t.Fatal("启动中业务未被拒绝", err)
		}
		if _, err := r.Call(t.Context(), Identity{Type: "ready", ID: "2"}, nil); err != nil {
			t.Fatal(err)
		}
		phase.Store(2)
		time.Sleep(20 * time.Millisecond)
		if received.Load() != 2 {
			t.Fatal("定时消息没有在启动后送达", received.Load())
		}
		if got, err := r.Call(t.Context(), id, nil); err != nil || got != "done" {
			t.Fatal(got, err)
		}
		if !errors.Is(RemoteErrorFrom(lifecycle.ErrNotReady), lifecycle.ErrNotReady) {
			t.Fatal("远端错误丢失准入分类")
		}
	})
}
