package actorrpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

type handoffReader struct {
	assignment atomic.Pointer[discovery.SlotAssignment]
	read       chan struct{}
}

func (r *handoffReader) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	select {
	case r.read <- struct{}{}:
	default:
	}
	return *r.assignment.Load(), nil
}

type handoffTransport struct {
	targets chan actorhost.Target
	err     error
}

func (s *handoffTransport) Call(context.Context, actorhost.Target, *actorpb.Delivery) (*actorpb.Result, error) {
	return nil, errors.New("测试不应走同步 Call")
}
func (s *handoffTransport) Enqueue(_ context.Context, target actorhost.Target, _ *actorpb.Delivery) error {
	s.targets <- target
	return s.err
}

func handoffCaller(t *testing.T, transportErr error) (*RoutedCaller, *handoffReader, *handoffTransport) {
	t.Helper()
	reader := &handoffReader{read: make(chan struct{}, 1)}
	reader.assignment.Store(&discovery.SlotAssignment{SlotID: "slot", RuntimeInstanceID: "old", Generation: 1})
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{ID: "old", Services: []string{"game"}, Endpoint: "old:1", Status: discovery.StatusDraining},
		{ID: "new", Services: []string{"game"}, Endpoint: "new:1", Status: discovery.StatusRunning},
	}); err != nil {
		t.Fatal(err)
	}
	resolver, err := discovery.NewSlotResolver(reader, directory)
	if err != nil {
		t.Fatal(err)
	}
	transport := &handoffTransport{targets: make(chan actorhost.Target, 4), err: transportErr}
	caller := &RoutedCaller{Router: &EntityRouter{Resolver: resolver,
		SlotID:  func(actorhost.Identity) string { return "slot" },
		Service: func(string) (string, error) { return "game", nil },
	}, Transport: transport}
	return caller, reader, transport
}

func TestEnqueueWaitsForHandoffBeforeFirstSend(t *testing.T) {
	caller, reader, transport := handoffCaller(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	id := actorhost.Identity{Type: "role", ID: "i:1"}
	done := make(chan error, 1)
	go func() { done <- caller.Enqueue(ctx, id, &actorpb.Delivery{}) }()
	select {
	case <-reader.read:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("交接完成前结束：%v", err)
	case <-transport.targets:
		t.Fatal("向排水中的旧实例发送")
	case <-time.After(25 * time.Millisecond):
	}
	reader.assignment.Store(&discovery.SlotAssignment{SlotID: "slot", RuntimeInstanceID: "new", Generation: 2})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	target := <-transport.targets
	if target.Instance != "new" || target.Identity != id || len(transport.targets) != 0 {
		t.Fatalf("目标或投递次数错误：%+v", target)
	}
}

func TestEnqueueHandoffWaitIsBoundedAndDoesNotRetryTransport(t *testing.T) {
	t.Run("取消等待不发送", func(t *testing.T) {
		caller, reader, transport := handoffCaller(t, nil)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- caller.Enqueue(ctx, actorhost.Identity{Type: "role", ID: "i:1"}, &actorpb.Delivery{}) }()
		<-reader.read
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("取消未返回：%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("取消未结束等待")
		}
		if len(transport.targets) != 0 {
			t.Fatal("取消后发送了消息")
		}
	})
	t.Run("无期限仍立即返回", func(t *testing.T) {
		caller, _, transport := handoffCaller(t, nil)
		if err := caller.Enqueue(context.Background(), actorhost.Identity{Type: "role", ID: "i:1"}, &actorpb.Delivery{}); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
			t.Fatal(err)
		}
		if len(transport.targets) != 0 {
			t.Fatal("路由不可用却发送")
		}
	})
	t.Run("发送返回同名错误也不重投", func(t *testing.T) {
		caller, reader, transport := handoffCaller(t, discovery.ErrSlotRuntimeUnavailable)
		reader.assignment.Store(&discovery.SlotAssignment{SlotID: "slot", RuntimeInstanceID: "new", Generation: 2})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := caller.Enqueue(ctx, actorhost.Identity{Type: "role", ID: "i:1"}, &actorpb.Delivery{}); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
			t.Fatal(err)
		}
		if len(transport.targets) != 1 {
			t.Fatalf("发送次数=%d", len(transport.targets))
		}
	})
}
