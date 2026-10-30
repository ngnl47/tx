package txn

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func submitRuntime(t *testing.T, limit int, timeout time.Duration) *Runtime {
	t.Helper()
	r := NewRuntime()
	if err := r.ConfigureAsync(AsyncConfig{MaxConcurrent: limit, MaxPending: limit, Timeout: timeout}); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSubmitIsIndependentBoundedAndAwaitDoesNotCancel(t *testing.T) {
	r := submitRuntime(t, 1, time.Second)
	parent, cancel := context.WithCancel(t.Context())
	entered, finish := make(chan struct{}), make(chan struct{})
	f, err := r.Submit(parent, continuationProcedure{process: func(tx *Tx) error {
		close(entered)
		<-finish
		return tx.Context().Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel()
	if _, err := r.Submit(t.Context(), continuationProcedure{process: func(*Tx) error { t.Error("满额仍执行"); return nil }}); !errors.Is(err, ErrAsyncFull) {
		t.Fatalf("Submit = %v", err)
	}
	if err := f.Await(parent); !errors.Is(err, context.Canceled) {
		t.Fatalf("Await = %v", err)
	}
	select {
	case <-f.Done():
		t.Fatal("等待取消提前结束任务")
	default:
	}
	close(finish)
	if err := f.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.ConfigureAsync(AsyncConfig{}); err == nil {
		t.Fatal("运行中修改配置未拒绝")
	}
	if err := f.Await(parent); err != nil {
		t.Fatalf("已完成结果不应被等待取消覆盖：%v", err)
	}
}

func TestSubmitTimeoutDoesNotReleaseBeforeActualExit(t *testing.T) {
	r := submitRuntime(t, 1, 20*time.Millisecond)
	expired, finish := make(chan struct{}), make(chan struct{})
	f, err := r.Submit(t.Context(), continuationProcedure{process: func(tx *Tx) error {
		<-tx.Context().Done()
		close(expired)
		<-finish
		return tx.Context().Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-expired
	if _, err := r.Submit(t.Context(), continuationProcedure{}); !errors.Is(err, ErrAsyncFull) {
		t.Fatalf("期限后提前归还额度：%v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.Drain(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain = %v", err)
	}
	select {
	case <-f.Done():
		t.Fatal("期限后提前发布完成")
	default:
	}
	close(finish)
	if err := f.Await(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("result = %v", err)
	}
	if err := r.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit(t.Context(), continuationProcedure{}); err == nil {
		t.Fatal("排水后仍接纳任务")
	}
}

func TestSubmitRejectsTransactionAndCanceledContext(t *testing.T) {
	r := submitRuntime(t, 1, time.Second)
	var old context.Context
	if err := r.Call(t.Context(), continuationProcedure{process: func(tx *Tx) error {
		old = tx.Context()
		if _, err := r.Submit(old, continuationProcedure{}); err == nil {
			t.Error("事务内 Submit 未拒绝")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit(old, continuationProcedure{}); err == nil {
		t.Fatal("过期事务 ctx 未拒绝")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Submit(canceled, continuationProcedure{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit = %v", err)
	}
}

func TestSubmitPanicResultDistinguishesCommitted(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		r := submitRuntime(t, 1, time.Second)
		rolledBack := false
		f, err := r.Submit(t.Context(), continuationProcedure{process: func(tx *Tx) error {
			tx.AfterRollback(func(context.Context) { rolledBack = true })
			if afterCommit {
				tx.AfterCommit(func(context.Context) error { panic("failed") })
				return nil
			}
			panic("failed")
		}})
		if err != nil {
			t.Fatal(err)
		}
		err = f.Await(t.Context())
		var p *PanicError
		var committed *AfterCommitError
		if !errors.As(err, &p) || len(p.Stack) == 0 || errors.As(err, &committed) != afterCommit || rolledBack == afterCommit {
			t.Fatalf("panic result=%v rollback=%v", err, rolledBack)
		}
		f, err = r.Submit(t.Context(), continuationProcedure{process: func(*Tx) error { return nil }})
		if err != nil {
			t.Fatalf("异常未归还额度：%v", err)
		}
		if err := f.Await(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSubmitConcurrentAdmission(t *testing.T) {
	const limit = 4
	r := NewRuntime()
	if err := r.ConfigureAsync(AsyncConfig{MaxConcurrent: limit, MaxPending: 12, Timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	var active atomic.Int32
	start, finish := make(chan struct{}), make(chan struct{})
	results := make(chan *Future, 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f, err := r.Submit(t.Context(), continuationProcedure{process: func(*Tx) error {
				if active.Add(1) > limit {
					t.Error("超出执行上限")
				}
				defer active.Add(-1)
				<-finish
				return nil
			}})
			if err != nil && !errors.Is(err, ErrAsyncFull) {
				t.Error(err)
			}
			if f != nil {
				results <- f
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	if len(results) != 12 {
		t.Errorf("accepted=%d want=%d", len(results), 12)
	}
	close(finish)
	for f := range results {
		if err := f.Await(t.Context()); err != nil {
			t.Error(err)
		}
	}
	if err := r.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitFinishPanicKeepsCommitAndReleasesOtherUses(t *testing.T) {
	r := submitRuntime(t, 1, time.Second)
	released := false
	f, err := r.Submit(t.Context(), continuationProcedure{process: func(tx *Tx) error {
		tx.OnFinish(func() { released = true })
		tx.OnFinish(func() { panic("finish") })
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	err = f.Await(t.Context())
	var committed *AfterCommitError
	if !errors.As(err, &committed) || !released {
		t.Fatalf("err=%v released=%v", err, released)
	}
}

func TestSubmitConfigurationIsPerRuntime(t *testing.T) {
	r := NewRuntime()
	for _, config := range []AsyncConfig{{MaxConcurrent: -1}, {MaxPending: -1}, {MaxConcurrent: 4, MaxPending: 3}, {Timeout: -time.Second}} {
		if err := r.ConfigureAsync(config); err == nil {
			t.Fatal("负配置未拒绝")
		}
	}
	other := submitRuntime(t, 1, time.Second)
	f, err := r.Submit(t.Context(), continuationProcedure{process: func(tx *Tx) error {
		deadline, ok := tx.Context().Deadline()
		if !ok || time.Until(deadline) > defaultAsyncTimeout || time.Until(deadline) < 9*time.Second {
			t.Error("未使用默认独立期限")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if r.asyncConfig.MaxConcurrent != 64 || r.asyncConfig.MaxPending != 8192 || other.asyncConfig.MaxConcurrent != 1 {
		t.Fatal("配置未隔离")
	}
}

func TestSubmitQueuePreservesOrderAndDrainsAcceptedWork(t *testing.T) {
	r := NewRuntime()
	if err := r.ConfigureAsync(AsyncConfig{MaxConcurrent: 1, MaxPending: 3, Timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first, err := r.Submit(t.Context(), continuationProcedure{process: func(tx *Tx) error {
		return tx.CallAfterCommit(continuationProcedure{process: func(*Tx) error { close(entered); <-release; return nil }})
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	order := make(chan int, 2)
	futures := []*Future{first}
	parent, cancel := context.WithCancel(t.Context())
	for i := range 2 {
		f, err := r.Submit(parent, continuationProcedure{process: func(tx *Tx) error { order <- i; return tx.Context().Err() }})
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, f)
	}
	cancel()
	if _, err := r.Submit(t.Context(), continuationProcedure{}); !errors.Is(err, ErrAsyncFull) {
		t.Fatalf("full=%v", err)
	}
	if len(order) != 0 {
		t.Fatal("同步后续过程完成前启动了排队事务")
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if err := r.Drain(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("drain=%v", err)
	}
	close(release)
	for _, f := range futures {
		if err := f.Await(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if <-order != 0 || <-order != 1 {
		t.Fatal("队列出队顺序错误")
	}
	if err := r.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit(t.Context(), continuationProcedure{}); err == nil {
		t.Fatal("排水后仍接纳")
	}
}

func TestSubmitQueuedDeadlineReleasesAdmissionWithoutExecution(t *testing.T) {
	r := NewRuntime()
	if err := r.ConfigureAsync(AsyncConfig{MaxConcurrent: 1, MaxPending: 2, Timeout: 30 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first, err := r.Submit(t.Context(), continuationProcedure{process: func(*Tx) error { close(entered); <-release; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	queued, err := r.Submit(t.Context(), continuationProcedure{process: func(*Tx) error { t.Error("过期队列项仍执行"); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := queued.Await(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-queued.Done():
	default:
		t.Fatal("只是等待超时，任务并未完成")
	}
	next, err := r.Submit(t.Context(), continuationProcedure{process: func(*Tx) error { return nil }})
	if err != nil {
		t.Fatalf("过期项未释放接纳位置: %v", err)
	}
	close(release)
	if err := first.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := next.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
}
