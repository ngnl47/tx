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

	drained := make(chan struct{})
	go func() {
		ingress.Wait()
		close(drained)
	}()
	assertNotClosed(t, drained, "ingress returned before accepted work finished")
	doneWork()
	assertClosed(t, drained, "ingress did not finish after accepted work")
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
