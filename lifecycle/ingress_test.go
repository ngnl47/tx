package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIngressBeginWorkDrainsWithoutCancelingRoot(t *testing.T) {
	root := NewWaitGroupContext(context.Background())
	ingress := NewIngress(root)
	if err := ingress.BeginWork(); err != nil {
		t.Fatalf("BeginWork() error = %v", err)
	}
	ingress.BeginDrain()
	if err := ingress.BeginWork(); !errors.Is(err, ErrDraining) {
		t.Fatalf("BeginWork() after drain error = %v, want %v", err, ErrDraining)
	}
	drained := make(chan error, 1)
	go func() {
		drained <- ingress.WaitContext(context.Background())
	}()
	select {
	case err := <-drained:
		t.Fatalf("WaitContext() returned before work completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := root.Err(); err != nil {
		t.Fatalf("root context was canceled during ingress drain: %v", err)
	}
	ingress.EndWork()
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("WaitContext() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitContext() did not return after work completed")
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()
}
