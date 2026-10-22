package actor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	txactor "github.com/ngnl47/tx/modules/actor"
)

// 业务只持有公开 Context；回调结束不使实体句柄失效，卸载后旧句柄不能触发重建。
func TestBehaviorContextLocalControl(t *testing.T) {
	var handle txactor.Context
	factory := txactor.BehaviorFactory(func(ctx txactor.Context) (*txactor.Behavior, error) {
		handle = ctx
		return &txactor.Behavior{Receive: func(ctx txactor.Context, message any) (any, error) {
			if message == "stop" {
				ctx.Deactivate()
			}
			return message, nil
		}}, nil
	})
	runtime := actorhost.New(4, time.Second, factory)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer runtime.Drain(ctx)
	if _, err := runtime.Call(ctx, txactor.Identity{Type: "room", ID: "1"}, "start"); err != nil {
		t.Fatal(err)
	}
	if handle.Err() == nil {
		t.Fatal("factory callback context should have ended")
	}
	var future txactor.LocalFuture
	future, err := handle.Post("stop")
	if err != nil {
		t.Fatal(err)
	}
	value, err := future.Await(ctx)
	if err != nil || value != "stop" {
		t.Fatalf("local result: %v %v", value, err)
	}
	select {
	case <-future.Done():
	default:
		t.Fatal("result not completed")
	}
	if _, err := handle.Post("stale"); !errors.Is(err, txactor.ErrDeactivated) {
		t.Fatalf("old handle: %v", err)
	}
}
