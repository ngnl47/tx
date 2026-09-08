package goakteval

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	gerrors "github.com/tochemey/goakt/v4/errors"
)

func request(session int64) *actorpb.Delivery {
	return &actorpb.Delivery{MessageType: "role.reward", CommandId: 1, RouteKeyKind: actorpb.RouteKeyKind_ROUTE_KEY_INT64, RouteKeyNumeric: 1001,
		Identity: &actorpb.SessionIdentity{AccountId: session, RoleId: 1001, SessionId: session, ServerId: 1, ConnectionId: session}, Payload: []byte("100")}
}

// 容量满必须显式失败，不得静默成功或阻塞全部发送方。
func TestBoundedMailboxRejectsOverload(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var executions atomic.Int32
	r, err := Start(context.Background(), "overload", 1, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) {
			if executions.Add(1) == 1 {
				close(entered)
				<-release
			}
			return &actorpb.Result{MessageType: "done"}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, 21)
	go func() { _, e := r.Call(ctx, request(1)); results <- e }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for i := 0; i < 20; i++ {
		go func() { _, e := r.Call(ctx, request(2)); results <- e }()
	}
	for i := 0; i < 19; i++ {
		e := <-results
		if !errors.Is(e, gerrors.ErrMailboxFull) {
			t.Errorf("expected mailbox full, got %v", e)
		}
	}
	close(release)
	for i := 0; i < 2; i++ {
		if e := <-results; e != nil {
			t.Error(e)
		}
	}
	if executions.Load() != 2 {
		t.Fatalf("executions=%d", executions.Load())
	}
}

// 路由若包含 SessionID，同一 RoleID 会分裂为多个状态，最终余额不是 2000。
func TestSessionsShareRole(t *testing.T) {
	var loads atomic.Int32
	r, err := Start(context.Background(), "sessions", 32, time.Hour, func(_ context.Context, id int64) (Handler, error) {
		if id != 1001 {
			return nil, errors.New("wrong role")
		}
		loads.Add(1)
		balance := 0
		return func(ctx context.Context, d *actorpb.Delivery) (*actorpb.Result, error) {
			if d.Identity.SessionId != 1 && d.Identity.SessionId != 2 {
				return nil, errors.New("identity lost")
			}
			amount, e := strconv.Atoi(string(d.Payload))
			if e != nil {
				return nil, e
			}
			balance += amount
			return &actorpb.Result{MessageType: "role.reward", CommandId: 1, Payload: []byte(strconv.Itoa(balance))}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, e := r.Call(ctx, request(int64(i%2+1))); e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	d := request(1)
	d.Payload = []byte("0")
	got, err := r.Call(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != "2000" || loads.Load() != 1 {
		t.Fatalf("balance=%s loads=%d", got.Payload, loads.Load())
	}
}

// 等待超时不能撤销已开始的业务；调用方不能把超时等同于可以关闭持久化资源。
func TestTimeoutDoesNotFinishBusiness(t *testing.T) {
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r, err := Start(context.Background(), "timeout", 4, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) {
			close(entered)
			<-release
			close(finished)
			return &actorpb.Result{MessageType: "done"}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, e := r.Call(ctx, request(1)); result <- e }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("not entered")
	}
	err = <-result
	if err == nil {
		t.Error("expected timeout")
	}
	select {
	case <-finished:
		t.Error("business ended before release")
	default:
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("business never finished")
	}
}

// 内建钝化后必须重新执行加载边界，不沿用已停用的 Handler。
func TestPassivationReloadsRole(t *testing.T) {
	var loads atomic.Int32
	r, err := Start(context.Background(), "passivation", 4, 50*time.Millisecond, func(context.Context, int64) (Handler, error) {
		generation := loads.Add(1)
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) {
			return &actorpb.Result{MessageType: "generation", Payload: []byte(strconv.Itoa(int(generation)))}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err = r.Call(ctx, request(1)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	got, err := r.Call(ctx, request(2))
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != "2" {
		t.Fatalf("generation=%s loads=%d", got.Payload, loads.Load())
	}
}

// 固定本地 Call 的评估基线；不代表与主库 Messenger 的等价性能比较。
func BenchmarkLocalRoleCall(b *testing.B) {
	response := &actorpb.Result{MessageType: "done", Payload: make([]byte, 256)}
	r, err := Start(context.Background(), "bench-role", 1024, time.Hour, func(context.Context, int64) (Handler, error) {
		return func(context.Context, *actorpb.Delivery) (*actorpb.Result, error) { return response, nil }, nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer r.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	d := request(1)
	d.Payload = make([]byte, 256)
	if _, err = r.Call(ctx, d); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err = r.Call(ctx, d); err != nil {
			b.Fatal(err)
		}
	}
}
