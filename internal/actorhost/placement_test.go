package actorhost

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestPlacementChangeRetiresOldActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		old := NewPlacement("a", map[string][]string{"role": {"a"}})
		next := NewPlacement("a", map[string][]string{"role": {"b"}})
		started := make(chan struct{})
		release := make(chan struct{})
		cleaning := make(chan struct{})
		cleaned := make(chan struct{})
		var handle Context
		loads, processed := 0, 0
		r := New(4, time.Second, func(c Context) (*Behavior, error) {
			loads++
			if loads > 1 {
				return &Behavior{Receive: func(Context, any) (any, error) { return nil, nil }}, nil
			}
			handle = c
			return &Behavior{Receive: func(Context, any) (any, error) {
				processed++
				if processed == 1 {
					close(started)
					<-release
				}
				return processed, nil
			}, OnDeactivateAfter: func(Context) error { close(cleaning); <-cleaned; return nil }}, nil
		})
		r.SetPlacement(old)
		id := Identity{"role", "1"}
		f, err := r.Submit(id, "first")
		if err != nil {
			t.Fatal(err)
		}
		<-started
		queued, err := r.Submit(id, "queued")
		if err != nil {
			t.Fatal(err)
		}
		r.SetPlacement(next)
		if _, err = r.Submit(id, "stale"); !errors.Is(err, ErrNotOwner) {
			t.Fatal(err)
		}
		if _, err = handle.Post("timer"); !errors.Is(err, ErrDeactivated) {
			t.Fatal(err)
		}
		close(release)
		if _, err = f.Await(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = queued.Await(ctx); err != nil {
			t.Fatal(err)
		}
		<-cleaning
		r.SetPlacement(old)
		if _, err = r.Submit(id, "too early"); !errors.Is(err, ErrDeactivated) {
			t.Fatalf("重获归属不得绕过旧清理: %v", err)
		}
		close(cleaned)
		synctest.Wait()
		// 重建再次调用工厂；上面的清理事件只属于旧实例。
		if _, err = r.Call(ctx, id, "new"); err != nil {
			t.Fatal(err)
		}
		if loads != 2 || processed != 2 {
			t.Fatalf("loads=%d processed=%d", loads, processed)
		}
		if err = r.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		r.SetPlacement(old)
		if _, err = r.Submit(id, "after drain"); !errors.Is(err, ErrDraining) {
			t.Fatal(err)
		}
	})
}

func TestPlacementUsesKindAndStableMemberOrder(t *testing.T) {
	a := NewPlacement("a", map[string][]string{"role": {"a", "b"}, "room": {"c"}})
	b := NewPlacement("b", map[string][]string{"role": {"b", "a"}, "room": {"c"}})
	for _, id := range []Identity{{"role", "1"}, {"role", "2"}, {"room", "1"}} {
		one, err := a.Target(id)
		if err != nil {
			t.Fatal(err)
		}
		two, err := b.Target(id)
		if err != nil || one != two {
			t.Fatalf("路由不一致: %v %v %v", one, two, err)
		}
	}
	if _, err := a.Target(Identity{"missing", "1"}); !errors.Is(err, ErrNotOwner) {
		t.Fatal(err)
	}
}
