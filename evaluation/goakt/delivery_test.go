package goakteval

import (
	"context"
	"errors"
	"testing"
	"time"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

// 接收确认必须早于 Handler 完成，且不能让排水忽略其后的业务。
func TestAcceptedDoesNotMeanProcessed(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r, err := Start(context.Background(), "accepted", 4, 8, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(ctx context.Context, _ *actorpb.Delivery) (*actorpb.Result, error) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &actorpb.Result{MessageType: "done"}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = r.TellAccepted(ctx, request(1)); err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel() // 结束发送方等待，不能取消已经确认接收的业务。
	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
	defer drainCancel()
	short, stop := context.WithTimeout(drainCtx, 20*time.Millisecond)
	err = r.Drain(short)
	stop()
	if err == nil {
		t.Error("accepted reply bypassed business drain")
	}
	close(release)
	if err = r.Drain(drainCtx); err != nil {
		t.Fatal(err)
	}
}

// Tell 只完成本地准入，不等待目标加载；发送后的消息不能受调用方修改影响。
func TestTellReturnsBeforeActivationAndRemainsDrainable(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	received := make(chan string, 1)
	r, err := Start(context.Background(), "tell", 4, 8, time.Hour, func(context.Context, int64) (Handler, error) {
		close(entered)
		<-release
		return func(_ context.Context, d *actorpb.Delivery) (*actorpb.Result, error) {
			received <- string(d.Payload)
			return nil, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	message := request(1)
	if err = r.Tell(message); err != nil {
		t.Fatal(err)
	}
	message.Payload[0] = '9'
	<-entered
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = r.Drain(short)
	stop()
	if err == nil {
		t.Error("pending Tell escaped drain")
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = r.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := <-received; got != "100" {
		t.Fatalf("payload=%q", got)
	}
}

// 单次 Await 超时既不能撤销 Future，也不能提前释放仍在使用的等待容量。
func TestFutureWaitIsIndependentAndCapacityIsBounded(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	r, err := Start(context.Background(), "future", 4, 1, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) {
			entered <- struct{}{}
			<-release
			return &actorpb.Result{MessageType: "done", Payload: []byte("ok")}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f, err := r.CallFuture(ctx, request(1))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = f.Await(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await=%v", err)
	}
	if _, err = r.CallFuture(ctx, request(2)); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("capacity=%v", err)
	}
	close(release)
	result, err := f.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result.Payload[0] = 'x'
	again, err := f.Await(ctx)
	if err != nil || string(again.Payload) != "ok" {
		t.Fatalf("repeat await=%v %v", again, err)
	}
	if _, err = r.Call(ctx, request(2)); err != nil {
		t.Fatal(err)
	}
}

type deniedError struct{}

func (deniedError) Error() string    { return "not allowed" }
func (deniedError) ErrorCode() int32 { return 17 }

// 业务错误必须保留稳定错误码，不依赖 Go 错误对象跨网络传输。
func TestCallPreservesCodedBusinessError(t *testing.T) {
	r, err := Start(context.Background(), "coded-error", 4, 8, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) { return nil, deniedError{} }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = r.Call(ctx, request(1))
	var remote *CallError
	if !errors.As(err, &remote) || remote.Code != 17 || remote.Message != "not allowed" {
		t.Fatalf("business error=%v", err)
	}
}
