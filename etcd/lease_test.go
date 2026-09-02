package etcd

import (
	"context"
	"errors"
	"testing"
)

func TestLeaseStopIsIdempotent(t *testing.T) {
	lease := newLeaseStateForTest()
	if err := lease.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Stop(); err != nil {
		t.Fatal(err)
	}
	<-lease.Done()
	if !errors.Is(lease.Err(), ErrLeaseStopped) {
		t.Fatalf("error = %v, want ErrLeaseStopped", lease.Err())
	}
}

func newLeaseStateForTest() *Lease {
	_, cancel := context.WithCancel(context.Background())
	return &Lease{
		cancel: cancel,
		done:   make(chan struct{}),
	}
}
