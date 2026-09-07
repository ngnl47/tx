package framework

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/sessionid"
)

var ErrSessionIDUnavailable = errors.New("framework: SessionID allocator is unavailable")

type sessionIDIdentity interface {
	Identity() discovery.RuntimeIdentity
	Done() <-chan struct{}
	Err() error
}

type sessionIDRuntime struct {
	mu           sync.RWMutex
	next         sessionid.Allocator
	identity     sessionIDIdentity
	availability context.Context
	cancel       context.CancelCauseFunc
	cause        error
	enabled      atomic.Bool
	closed       atomic.Bool
}

func newSessionIDRuntime(identity sessionIDIdentity) (*sessionIDRuntime, error) {
	if identity == nil {
		return nil, ErrSessionIDUnavailable
	}
	allocator, err := sessionid.New(identity.Identity().WorkSlot)
	if err != nil {
		return nil, errors.Join(ErrSessionIDUnavailable, err)
	}
	return newSessionIDRuntimeWithAllocator(allocator, identity), nil
}

func newSessionIDRuntimeWithAllocator(next sessionid.Allocator, identity sessionIDIdentity) *sessionIDRuntime {
	availability, cancel := context.WithCancelCause(context.Background())
	runtime := &sessionIDRuntime{
		next:         next,
		identity:     identity,
		availability: availability,
		cancel:       cancel,
	}
	go func() {
		select {
		case <-identity.Done():
			runtime.invalidate(sessionIDUnavailable(identity))
		case <-availability.Done():
		}
	}()
	return runtime
}

// AllocateSessionID 仅在当前 Discovery 运行身份仍有效时分配 SessionID。
func (runtime *sessionIDRuntime) AllocateSessionID(ctx context.Context) (int64, error) {
	if runtime == nil {
		return 0, ErrSessionIDUnavailable
	}
	if runtime.closed.Load() {
		return 0, runtime.unavailableError()
	}
	if !runtime.enabled.Load() {
		return 0, ErrSessionIDUnavailable
	}
	runtime.mu.RLock()
	if runtime.closed.Load() {
		cause := runtime.cause
		runtime.mu.RUnlock()
		if cause != nil {
			return 0, cause
		}
		return 0, ErrSessionIDUnavailable
	}
	if !runtime.enabled.Load() {
		runtime.mu.RUnlock()
		return 0, ErrSessionIDUnavailable
	}
	next := runtime.next
	identity := runtime.identity
	availability := runtime.availability
	runtime.mu.RUnlock()
	if next == nil || identity == nil || availability == nil {
		return 0, ErrSessionIDUnavailable
	}
	select {
	case <-identity.Done():
		cause := sessionIDUnavailable(identity)
		runtime.invalidate(cause)
		return 0, cause
	default:
	}
	allocationCtx := availability
	release := func() {}
	if ctx != nil && ctx.Done() != nil {
		var cancel context.CancelCauseFunc
		allocationCtx, cancel = context.WithCancelCause(availability)
		stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
		release = func() {
			stop()
			cancel(nil)
		}
	}
	value, err := next.AllocateSessionID(allocationCtx)
	release()
	select {
	case <-identity.Done():
		cause := sessionIDUnavailable(identity)
		runtime.invalidate(cause)
		return 0, cause
	default:
	}

	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	if cause := context.Cause(availability); cause != nil {
		return 0, cause
	}
	if err != nil {
		return 0, err
	}
	select {
	case <-identity.Done():
		return 0, sessionIDUnavailable(identity)
	default:
	}
	return value, nil
}

func (runtime *sessionIDRuntime) enable() {
	if runtime == nil || runtime.closed.Load() {
		return
	}
	runtime.mu.Lock()
	if runtime.closed.Load() || runtime.identity == nil {
		runtime.mu.Unlock()
		return
	}
	identity := runtime.identity
	select {
	case <-identity.Done():
		runtime.mu.Unlock()
		runtime.invalidate(sessionIDUnavailable(identity))
		return
	default:
	}
	runtime.enabled.Store(true)
	runtime.mu.Unlock()
}

func (runtime *sessionIDRuntime) close() {
	if runtime == nil {
		return
	}
	runtime.mu.RLock()
	identity := runtime.identity
	runtime.mu.RUnlock()
	cause := error(ErrSessionIDUnavailable)
	if identity != nil {
		select {
		case <-identity.Done():
			cause = sessionIDUnavailable(identity)
		default:
		}
	}
	runtime.invalidate(cause)
}

func (runtime *sessionIDRuntime) invalidate(cause error) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	if runtime.closed.Load() {
		runtime.mu.Unlock()
		return
	}
	if cause == nil {
		cause = ErrSessionIDUnavailable
	}
	runtime.cause = cause
	runtime.enabled.Store(false)
	runtime.closed.Store(true)
	if runtime.cancel != nil {
		runtime.cancel(cause)
	}
	runtime.next = nil
	runtime.identity = nil
	runtime.cancel = nil
	runtime.mu.Unlock()
}

func (runtime *sessionIDRuntime) unavailableError() error {
	if runtime == nil {
		return ErrSessionIDUnavailable
	}
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	if runtime.cause != nil {
		return runtime.cause
	}
	return ErrSessionIDUnavailable
}

func sessionIDUnavailable(identity sessionIDIdentity) error {
	if identity == nil {
		return ErrSessionIDUnavailable
	}
	if err := identity.Err(); err != nil {
		return errors.Join(ErrSessionIDUnavailable, err)
	}
	return ErrSessionIDUnavailable
}
