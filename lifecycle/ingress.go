package lifecycle

import (
	"errors"
	"sync"
)

var ErrDraining = errors.New("lifecycle: ingress is draining")

// Ingress gates new requests for one service while tracking requests accepted
// before draining starts. It deliberately has no knowledge of storage,
// transactions or business handlers.
type Ingress struct {
	work *ChildWaitGroupContext

	mu       sync.Mutex
	draining bool
}

// NewIngress creates a service-local request gate joined to parent shutdown.
func NewIngress(parent *WaitGroupContext) *Ingress {
	return &Ingress{work: NewChildWaitGroupContext(parent)}
}

// Enter accepts one request unless draining has started. The returned function
// must be called after handling the request.
func (g *Ingress) Enter() (func(), error) {
	if g == nil || g.work == nil {
		return nil, ErrStopping
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return nil, ErrDraining
	}
	return g.work.Enter()
}

// BeginDrain stops accepting new requests. It is idempotent.
func (g *Ingress) BeginDrain() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.draining = true
	g.mu.Unlock()
}

// Draining reports whether this ingress accepts new requests.
func (g *Ingress) Draining() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.draining
}

// Wait waits for requests accepted before BeginDrain to complete. Callers must
// begin draining before waiting so no new work can be added concurrently.
func (g *Ingress) Wait() {
	if g == nil || g.work == nil {
		return
	}
	g.work.Wait()
}
