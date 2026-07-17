package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

func TestWaitGroupContextWaitsForCancelAndWorkers(t *testing.T) {
	ctx := lifecycle.NewWaitGroupContext(context.Background())
	ctx.WaitGroupAdd(1)

	done := make(chan struct{})
	go func() {
		ctx.WaitDoneAndWaitGroupWait()
		close(done)
	}()

	assertNotClosed(t, done, "WaitDoneAndWaitGroupWait returned before cancel")

	if !ctx.Cancel() {
		t.Fatalf("Cancel returned false on first call")
	}
	assertNotClosed(t, done, "WaitDoneAndWaitGroupWait returned before worker done")

	ctx.WaitGroupDone()
	assertClosed(t, done, "WaitDoneAndWaitGroupWait did not return after cancel and worker done")
}

func TestChildWaitGroupContextUsesParentWaitGroup(t *testing.T) {
	parent := lifecycle.NewWaitGroupContext(context.Background())
	child := lifecycle.NewChildWaitGroupContext(parent)

	child.ParentWaitGroupAdd(1)

	done := make(chan struct{})
	go func() {
		parent.WaitGroupWait()
		close(done)
	}()

	assertNotClosed(t, done, "parent WaitGroupWait returned before child parent work done")

	child.ParentWaitGroupDone()
	assertClosed(t, done, "parent WaitGroupWait did not return after child parent work done")
}

func TestCancelContextReportsFirstLocalCancel(t *testing.T) {
	ctx := lifecycle.NewCancelContext(context.Background())

	if ctx.Canceled() {
		t.Fatalf("Canceled returned true before cancel")
	}
	if !ctx.ThisCancel() {
		t.Fatalf("ThisCancel returned false on first call")
	}
	if !ctx.Canceled() {
		t.Fatalf("Canceled returned false after cancel")
	}
	if ctx.ThisCancel() {
		t.Fatalf("ThisCancel returned true on second call")
	}
	if ctx.Cancel() {
		t.Fatalf("Cancel returned true after local cancel")
	}
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
