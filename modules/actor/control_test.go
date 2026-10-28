package actor_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
)

// 只提供 Probe；等待若误调用激活或重试，会直接暴露错误。
type statusController struct {
	txactor.Controller
	probe func(context.Context, txactor.Identity) (txactor.ActivationStatus, error)
}

func (c statusController) Probe(ctx context.Context, id txactor.Identity) (txactor.ActivationStatus, error) {
	return c.probe(ctx, id)
}

func TestWaitReadyObservesWithoutActivating(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		id := txactor.Identity{Type: "world", ID: "1"}
		calls := 0
		control := statusController{probe: func(_ context.Context, got txactor.Identity) (txactor.ActivationStatus, error) {
			if got != id {
				t.Fatalf("查询身份改变: %v", got)
			}
			calls++
			if calls < 3 {
				return txactor.ActivationStatus{State: txactor.Initializing}, nil
			}
			return txactor.ActivationStatus{State: txactor.Ready}, nil
		}}
		if err := txactor.WaitReady(t.Context(), control, id); err != nil {
			t.Fatal(err)
		}
		if calls != 3 {
			t.Fatalf("未等到就绪: %d", calls)
		}
	})
}

func TestWaitReadyStopsOnTerminalStateOrQueryError(t *testing.T) {
	failure := &txactor.ActivationFailure{Code: 404, Message: "业务加载拒绝"}
	queryError := errors.New("归属查询失败")
	for _, tc := range []struct {
		name             string
		state            txactor.ActivationStatus
		queryError, want error
	}{
		{name: "未加载", state: txactor.ActivationStatus{State: txactor.Unloaded}},
		{name: "关闭中", state: txactor.ActivationStatus{State: txactor.Closing}, want: txactor.ErrDeactivated},
		{name: "加载失败", state: txactor.ActivationStatus{State: txactor.Failed, Failure: failure}, want: failure},
		{name: "缺少失败详情", state: txactor.ActivationStatus{State: txactor.Failed}},
		{name: "查询失败", queryError: queryError, want: queryError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			control := statusController{probe: func(context.Context, txactor.Identity) (txactor.ActivationStatus, error) {
				calls++
				return tc.state, tc.queryError
			}}
			err := txactor.WaitReady(t.Context(), control, txactor.Identity{Type: "world", ID: "1"})
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("终止原因丢失: %v", err)
			}
			if calls != 1 {
				t.Fatalf("不应自动恢复或重试: %d", calls)
			}
		})
	}
}

func TestWaitReadyCancellationOnlyEndsWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := txactor.Initializing
		calls := 0
		control := statusController{probe: func(context.Context, txactor.Identity) (txactor.ActivationStatus, error) {
			calls++
			return txactor.ActivationStatus{State: state}, nil
		}}
		id := txactor.Identity{Type: "world", ID: "1"}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := txactor.WaitReady(ctx, control, id); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		previous := calls
		if err := txactor.WaitReady(ctx, control, id); !errors.Is(err, context.DeadlineExceeded) || calls != previous {
			t.Fatalf("已结束等待仍发起查询: %v", err)
		}
		state = txactor.Ready
		if err := txactor.WaitReady(t.Context(), control, id); err != nil {
			t.Fatalf("可以用新上下文继续观察: %v", err)
		}
	})
}
