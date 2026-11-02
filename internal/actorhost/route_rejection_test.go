package actorhost

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestDownstreamRouteRejectionIsBusinessFailureOfCurrentActor(t *testing.T) {
	downstream := New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(Context, any) (any, error) { return nil, nil }}, nil
	})
	downstream.BeginDrain()
	r := New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(c Context, _ any) (any, error) {
			_, err := downstream.Call(c.Context, Identity{Type: "downstream", ID: "1"}, "call")
			return nil, fmt.Errorf("downstream: %w", err)
		}}, nil
	})
	t.Cleanup(func() { _ = r.Drain(t.Context()); _ = downstream.Drain(t.Context()) })
	_, err := r.Call(t.Context(), Identity{Type: "upstream", ID: "1"}, "call")
	if !errors.Is(err, ErrDraining) {
		t.Fatalf("必须保留下游错误分类: %v", err)
	}
	if IsRouteRejection(err) || RemoteErrorFrom(err).RouteRejected {
		t.Fatalf("已执行业务的 Actor 被误判为准入拒绝: %v", err)
	}
}

func TestRouteRejectionDistinguishesAdmissionFromBusiness(t *testing.T) {
	r := New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(Context, any) (any, error) { return nil, ErrNotOwner }}, nil
	})
	t.Cleanup(func() { _ = r.Drain(t.Context()) })
	id := Identity{Type: "role", ID: "1"}
	_, err := r.Call(t.Context(), id, "business")
	if !errors.Is(err, ErrNotOwner) || IsRouteRejection(err) {
		t.Fatalf("业务错误误判准入: %v", err)
	}
	r.BeginDrain()
	_, err = r.Submit(id, "rejected")
	if !IsRouteRejection(err) || !errors.Is(err, ErrDraining) {
		t.Fatalf("缺少准入拒绝标识: %v", err)
	}
	if !IsRouteRejection(RemoteErrorFrom(err)) {
		t.Fatal("编码丢失拒绝来源")
	}
}
