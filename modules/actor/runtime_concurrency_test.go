package actor

import (
	"sync"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
)

func TestRuntimeVirtualPIDResolvesIndependentIdentitiesConcurrently(t *testing.T) {
	lookup := &blockingIdentityLookup{
		entered: make(chan *cluster.ClusterIdentity, 2),
		release: make(chan struct{}),
	}
	runtime := &Runtime{
		cluster: &cluster.Cluster{IdentityLookup: lookup},
		started: true,
	}

	results := make(chan error, 2)
	for _, identity := range []string{"role-470001", "role-470002"} {
		go func(identity string) {
			_, err := runtime.VirtualPID("role", identity)
			results <- err
		}(identity)
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(lookup.release) })
	}
	defer release()

	waitIdentityLookupEntry(t, lookup.entered)
	waitIdentityLookupEntry(t, lookup.entered)
	release()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("VirtualPID() error = %v", err)
		}
	}
}

func waitIdentityLookupEntry(t *testing.T, entered <-chan *cluster.ClusterIdentity) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("independent VirtualPID lookup was serialized")
	}
}

type blockingIdentityLookup struct {
	entered chan *cluster.ClusterIdentity
	release chan struct{}
}

func (lookup *blockingIdentityLookup) Get(identity *cluster.ClusterIdentity) *protoactor.PID {
	lookup.entered <- identity
	<-lookup.release
	return protoactor.NewPID("127.0.0.1:9201", identity.Identity)
}

func (*blockingIdentityLookup) RemovePid(*cluster.ClusterIdentity, *protoactor.PID) {}

func (*blockingIdentityLookup) Setup(*cluster.Cluster, []string, bool) {}

func (*blockingIdentityLookup) Shutdown() {}
