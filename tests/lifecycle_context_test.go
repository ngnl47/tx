package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

func TestWaitGroupContextWaitsForAcceptedWork(t *testing.T) {
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	if err := ctx.Go(func(context.Context) {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("Go returned error: %v", err)
	}
	<-started

	done := make(chan struct{})
	go func() {
		ctx.WaitDoneAndWaitGroupWait()
		close(done)
	}()

	assertNotClosed(t, done, "WaitDoneAndWaitGroupWait returned before cancel")
	if !ctx.Cancel() {
		t.Fatal("Cancel returned false on first call")
	}
	assertNotClosed(t, done, "WaitDoneAndWaitGroupWait returned before task finished")

	close(release)
	assertClosed(t, done, "WaitDoneAndWaitGroupWait did not return after task finished")
}

func TestWaitGroupContextRejectsWorkAfterCancel(t *testing.T) {
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	ctx.Cancel()
	if err := ctx.Go(func(context.Context) {}); !errors.Is(err, lifecycle.ErrStopping) {
		t.Fatalf("Go after Cancel error = %v, want ErrStopping", err)
	}
}

func TestIngressDrainsAcceptedWorkWithoutAcceptingNewWork(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	ingress := lifecycle.NewIngress(root)
	doneWork, err := ingress.Enter()
	if err != nil {
		t.Fatalf("Enter returned error: %v", err)
	}
	ingress.BeginDrain()
	if _, err := ingress.Enter(); !errors.Is(err, lifecycle.ErrDraining) {
		t.Fatalf("Enter while draining error = %v, want ErrDraining", err)
	}

	drained := make(chan error, 1)
	go func() {
		drained <- ingress.Wait()
	}()
	select {
	case err := <-drained:
		t.Fatalf("ingress returned before accepted work finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	doneWork()
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("ingress Wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ingress did not finish after accepted work")
	}
}

func TestIngressWaitContextReportsPendingWorkOnTimeout(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	ingress := lifecycle.NewIngress(root)
	done, err := ingress.Enter()
	if err != nil {
		t.Fatalf("Enter returned error: %v", err)
	}
	ingress.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := ingress.WaitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitContext error = %v, want deadline exceeded", err)
	}
	if pending := ingress.Pending(); pending != 1 {
		t.Fatalf("Pending = %d, want 1", pending)
	}
	done()
	if err := ingress.WaitContext(context.Background()); err != nil {
		t.Fatalf("WaitContext after completion: %v", err)
	}
}

func TestIngressWaitRejectsMissingOrOpenGate(t *testing.T) {
	var missing *lifecycle.Ingress
	if err := missing.Wait(); !errors.Is(err, lifecycle.ErrStopping) {
		t.Fatalf("nil ingress Wait error = %v, want ErrStopping", err)
	}

	ingress := lifecycle.NewIngress(lifecycle.NewWaitGroupContext(context.Background()))
	if err := ingress.Wait(); !errors.Is(err, lifecycle.ErrDrainNotStarted) {
		t.Fatalf("open ingress Wait error = %v, want ErrDrainNotStarted", err)
	}
}

func TestChildWaitGroupContextIsJoinedToParent(t *testing.T) {
	parent := lifecycle.NewWaitGroupContext(context.Background())
	child := lifecycle.NewChildWaitGroupContext(parent)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := child.Go(func(context.Context) {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("child Go returned error: %v", err)
	}
	<-started

	done := make(chan struct{})
	go func() {
		parent.Cancel()
		parent.WaitDoneAndWaitGroupWait()
		close(done)
	}()
	assertNotClosed(t, done, "parent returned before child task finished")

	close(release)
	assertClosed(t, done, "parent did not wait for child task")
}

func assertNotClosed(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()

	select {
	case <-ch:
		t.Fatal(message)
	case <-time.After(20 * time.Millisecond):
	}
}

func assertClosed(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}
