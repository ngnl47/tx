package framework

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/sessionid"
)

func TestSessionIDCModRequiresDiscovery(t *testing.T) {
	mod := (&Server{}).sessionIDCMod()
	if got := mod.Spec().Requires; !slices.Equal(got, []CModID{CModDiscovery}) {
		t.Fatalf("requires = %v, want [%s]", got, CModDiscovery)
	}
}

func TestSessionIDCModAddsDiscoveryToDependencyClosure(t *testing.T) {
	graph, err := resolveCModGraph((&Server{}).builtInCMods(RunOptions{}), []Service{
		cmodServiceFake{spec: Spec{Name: "login", Requires: []CModID{CModSessionID}}},
	})
	if err != nil {
		t.Fatalf("resolve CMod graph: %v", err)
	}
	if got := graph.IDs(); !slices.Equal(got, []CModID{CModDiscovery, CModSessionID}) {
		t.Fatalf("CMod order = %v, want [%s %s]", got, CModDiscovery, CModSessionID)
	}
}

func TestServerEnablesSessionIDsOnlyAfterSuccessfulStart(t *testing.T) {
	var handle sessionid.Allocator
	var duringStartValue int64
	var duringStartErr error
	server := newSessionIDGateServer(t, func(runtime Context) error {
		handle = runtime.SessionIDs()
		if handle == nil {
			return errors.New("SessionID handle is nil")
		}
		duringStartValue, duringStartErr = handle.AllocateSessionID(context.Background())
		return nil
	})
	if err := server.Start(context.Background(), RunOptions{Services: []string{"login"}}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if duringStartValue != 0 || !errors.Is(duringStartErr, ErrSessionIDUnavailable) {
		t.Fatalf("AllocateSessionID during Start = %d, %v, want unavailable", duringStartValue, duringStartErr)
	}
	value, err := handle.AllocateSessionID(context.Background())
	if err != nil || value <= 0 || value > sessionid.MaxSessionID {
		t.Fatalf("AllocateSessionID after Start = %d, %v, want positive JavaScript-safe value", value, err)
	}
}

func TestServerStartupFailureNeverEnablesSessionIDs(t *testing.T) {
	wantErr := errors.New("login start failed")
	var handle sessionid.Allocator
	var duringStartValue int64
	var duringStartErr error
	server := newSessionIDGateServer(t, func(runtime Context) error {
		handle = runtime.SessionIDs()
		if handle == nil {
			return errors.New("SessionID handle is nil")
		}
		duringStartValue, duringStartErr = handle.AllocateSessionID(context.Background())
		return wantErr
	})
	err := server.Start(context.Background(), RunOptions{Services: []string{"login"}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start error = %v, want %v", err, wantErr)
	}
	if duringStartValue != 0 || !errors.Is(duringStartErr, ErrSessionIDUnavailable) {
		t.Fatalf("AllocateSessionID during failed Start = %d, %v, want unavailable", duringStartValue, duringStartErr)
	}
	if _, err := handle.AllocateSessionID(context.Background()); !errors.Is(err, ErrSessionIDUnavailable) {
		t.Fatalf("AllocateSessionID after failed Start = %v, want unavailable", err)
	}
}

func newSessionIDGateServer(t *testing.T, start func(Context) error) *Server {
	t.Helper()
	identity := newSessionIDIdentityFake()
	next, err := sessionid.NewEtcdAllocator(&sessionIDRevisionFake{}, "test-issuer")
	if err != nil {
		t.Fatal(err)
	}
	allocator := newSessionIDRuntime(next, identity)
	var server *Server
	mod := &builtInCMod{
		spec: CModSpec{ID: "test-session-id-gate"},
		start: func(CModContext) error {
			server.runtime.setSessionIDs(allocator)
			return nil
		},
		drain: func(context.Context) error {
			server.runtime.closeSessionIDs()
			return nil
		},
		stop: func(context.Context) error {
			server.runtime.clearSessionIDs()
			return nil
		},
	}
	server, err = New(
		Config{AppID: "session-id-gate"},
		WithCMod(mod),
		WithService(testService{
			spec:  Spec{Name: "login", Requires: []CModID{mod.Spec().ID}},
			start: start,
		}),
	)
	if err != nil {
		allocator.close()
		t.Fatalf("New: %v", err)
	}
	return server
}

func TestSessionIDRuntimeAllocatesOnlyWhileIdentityIsValid(t *testing.T) {
	identity := newSessionIDIdentityFake()
	allocator, err := sessionid.NewEtcdAllocator(&sessionIDRevisionFake{}, "test-issuer")
	if err != nil {
		t.Fatal(err)
	}
	runtime := newSessionIDRuntime(allocator, identity)
	runtime.enable()

	value, err := runtime.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("AllocateSessionID: %v", err)
	}
	if value <= 0 || value > sessionid.MaxSessionID {
		t.Fatalf("SessionID = %d, want positive JavaScript-safe value", value)
	}

	identity.finish(discovery.ErrRuntimeIdentityLost)
	select {
	case <-runtime.availability.Done():
	case <-time.After(time.Second):
		t.Fatal("SessionID runtime did not observe identity loss")
	}
	_, err = runtime.AllocateSessionID(context.Background())
	if !errors.Is(err, ErrSessionIDUnavailable) || !errors.Is(err, discovery.ErrRuntimeIdentityLost) {
		t.Fatalf("AllocateSessionID after identity loss = %v, want unavailable with identity loss", err)
	}
}

func TestSessionIDRuntimeCannotReenableAfterInvalidation(t *testing.T) {
	t.Run("Drain", func(t *testing.T) {
		identity := newSessionIDIdentityFake()
		next, err := sessionid.NewEtcdAllocator(&sessionIDRevisionFake{}, "test-issuer")

		if err != nil {
			t.Fatal(err)
		}
		runtime := newSessionIDRuntime(next, identity)
		runtime.enable()
		runtime.close()
		runtime.enable()
		if _, err := runtime.AllocateSessionID(context.Background()); !errors.Is(err, ErrSessionIDUnavailable) {
			t.Fatalf("AllocateSessionID after Drain and re-enable = %v, want unavailable", err)
		}
	})

	t.Run("identity loss", func(t *testing.T) {
		identity := newSessionIDIdentityFake()
		next, err := sessionid.NewEtcdAllocator(&sessionIDRevisionFake{}, "test-issuer")

		if err != nil {
			t.Fatal(err)
		}
		runtime := newSessionIDRuntime(next, identity)
		runtime.enable()
		identity.finish(discovery.ErrRuntimeIdentityLost)
		select {
		case <-runtime.availability.Done():
		case <-time.After(time.Second):
			t.Fatal("SessionID runtime did not observe identity loss")
		}
		runtime.enable()
		_, err = runtime.AllocateSessionID(context.Background())
		if !errors.Is(err, ErrSessionIDUnavailable) || !errors.Is(err, discovery.ErrRuntimeIdentityLost) {
			t.Fatalf("AllocateSessionID after identity loss and re-enable = %v, want unavailable with identity loss", err)
		}
	})
}

func TestSessionIDCModDrainStopsFurtherAllocation(t *testing.T) {
	identity := newSessionIDIdentityFake()
	allocator, err := sessionid.NewEtcdAllocator(&sessionIDRevisionFake{}, "test-issuer")
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{runtime: &runtime{sessionIDs: newSessionIDRuntime(allocator, identity)}}
	server.runtime.sessionIDs.enable()
	drainer, ok := server.sessionIDCMod().(CModDrainer)
	if !ok {
		t.Fatal("SessionID CMod does not implement CModDrainer")
	}
	if err := drainer.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	_, err = server.runtime.sessionIDs.AllocateSessionID(context.Background())
	if !errors.Is(err, ErrSessionIDUnavailable) {
		t.Fatalf("AllocateSessionID after Drain = %v, want unavailable", err)
	}
}

func TestSessionIDCModDrainPreservesObservedIdentityLoss(t *testing.T) {
	identity := newSessionIDIdentityBlockingFirstErr()
	runtime := newSessionIDRuntime(newBlockingSessionIDAllocator(), identity)
	runtime.enable()
	t.Cleanup(identity.release)
	close(identity.done)
	select {
	case <-identity.errStarted:
	case <-time.After(time.Second):
		t.Fatal("identity listener did not observe identity loss")
	}

	runtime.close()
	identity.release()
	_, err := runtime.AllocateSessionID(context.Background())
	if !errors.Is(err, ErrSessionIDUnavailable) || !errors.Is(err, discovery.ErrRuntimeIdentityLost) {
		t.Fatalf("AllocateSessionID after identity loss and Drain = %v, want unavailable with identity loss", err)
	}
}

func TestSessionIDRuntimeIdentityLossStopsWaitingAllocation(t *testing.T) {
	identity := newSessionIDIdentityFake()
	next := newBlockingSessionIDAllocator()
	runtime := newSessionIDRuntime(next, identity)
	runtime.enable()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := runtime.AllocateSessionID(ctx)
		result <- err
	}()
	next.waitUntilStarted(t)

	identity.finish(discovery.ErrRuntimeIdentityLost)
	select {
	case err := <-result:
		if !errors.Is(err, ErrSessionIDUnavailable) || !errors.Is(err, discovery.ErrRuntimeIdentityLost) {
			t.Fatalf("waiting AllocateSessionID after identity loss = %v, want unavailable with identity loss", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting AllocateSessionID did not stop after identity loss")
	}
}

func TestSessionIDCModDrainStopsWaitingAllocation(t *testing.T) {
	identity := newSessionIDIdentityFake()
	next := newBlockingSessionIDAllocator()
	allocator := newSessionIDRuntime(next, identity)
	allocator.enable()
	server := &Server{runtime: &runtime{sessionIDs: allocator}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := allocator.AllocateSessionID(ctx)
		result <- err
	}()
	next.waitUntilStarted(t)

	drainer := server.sessionIDCMod().(CModDrainer)
	if err := drainer.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrSessionIDUnavailable) {
			t.Fatalf("waiting AllocateSessionID after Drain = %v, want unavailable", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting AllocateSessionID did not stop after Drain")
	}
}

func TestServerStopCancelsAdmittedSessionIDAllocation(t *testing.T) {
	var serviceContext Context
	server := newSessionIDGateServer(t, func(current Context) error {
		serviceContext = current
		return nil
	})
	if err := server.Start(t.Context(), RunOptions{Services: []string{"login"}}); err != nil {
		t.Fatal(err)
	}
	allocator := server.runtime.sessionIDs
	t.Cleanup(func() {
		allocator.close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Errorf("Stop cleanup: %v", err)
		}
	})
	// 模拟成员租约健康，但一个已接纳 HTTP 请求的 etcd 写入尚未返回。
	next := newBlockingSessionIDAllocator()
	allocator.mu.Lock()
	allocator.next = next
	allocator.mu.Unlock()
	leave, err := serviceContext.Admission().Enter()
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		defer leave()
		_, err := serviceContext.SessionIDs().AllocateSessionID(context.Background())
		result <- err
	}()
	next.waitUntilStarted(t)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop 等待请求退出前没有取消发号: %v", err)
	}
	if err := <-result; !errors.Is(err, ErrSessionIDUnavailable) {
		t.Fatalf("排水后的发号结果 = %v", err)
	}
}

func TestSessionIDRuntimeWaitingAllocationHonorsCallerContext(t *testing.T) {
	identity := newSessionIDIdentityFake()
	next := newBlockingSessionIDAllocator()
	runtime := newSessionIDRuntime(next, identity)
	runtime.enable()
	t.Cleanup(runtime.close)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := runtime.AllocateSessionID(ctx)
		result <- err
	}()
	next.waitUntilStarted(t)

	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting AllocateSessionID after caller cancellation = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting AllocateSessionID did not stop after caller cancellation")
	}
}

func TestSessionIDRuntimeRejectsIDWhenIdentityIsLostDuringAllocation(t *testing.T) {
	identity := newSessionIDIdentityBlockingFirstErr()
	next := &identityLosingSessionIDAllocator{identity: identity}
	runtime := newSessionIDRuntime(next, identity)
	runtime.enable()

	value, err := runtime.AllocateSessionID(context.Background())
	identity.release()
	runtime.close()
	if value != 0 || !errors.Is(err, ErrSessionIDUnavailable) || !errors.Is(err, discovery.ErrRuntimeIdentityLost) {
		t.Fatalf("AllocateSessionID during identity loss = %d, %v, want unavailable with identity loss", value, err)
	}
}

func TestSessionIDRuntimeRejectsCallerCancellationAtAllocationBoundary(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprintf("before=%t", before), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls int
			next := sessionIDAllocatorFunc(func(context.Context) (int64, error) {
				calls++
				cancel()
				return 41, nil
			})
			identity := newSessionIDIdentityFake()
			runtime := newSessionIDRuntime(next, identity)
			runtime.enable()
			t.Cleanup(runtime.close)
			if before {
				cancel()
			}
			id, err := runtime.AllocateSessionID(ctx)
			if id != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("取消后仍返回编号: %d, %v", id, err)
			}
			if before && calls != 0 {
				t.Fatalf("已取消请求调用分配器 %d 次", calls)
			}
		})

	}
}

type sessionIDAllocatorFunc func(context.Context) (int64, error)

func (f sessionIDAllocatorFunc) AllocateSessionID(ctx context.Context) (int64, error) { return f(ctx) }

func TestRuntimeSessionIDsSnapshotPreservesNil(t *testing.T) {
	if got := (&runtime{}).sessionIDsSnapshot(); got != nil {
		t.Fatalf("SessionIDs snapshot = %#v, want nil", got)
	}
}

type sessionIDIdentityFake struct {
	done chan struct{}

	mu  sync.RWMutex
	err error
}

func newSessionIDIdentityFake() *sessionIDIdentityFake {
	return &sessionIDIdentityFake{
		done: make(chan struct{}),
	}
}

func (fake *sessionIDIdentityFake) Done() <-chan struct{} { return fake.done }

func (fake *sessionIDIdentityFake) Err() error {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	return fake.err
}

func (fake *sessionIDIdentityFake) finish(err error) {
	fake.mu.Lock()
	fake.err = err
	fake.mu.Unlock()
	close(fake.done)
}

type blockingSessionIDAllocator struct {
	started chan struct{}
	once    sync.Once
}

type sessionIDIdentityBlockingFirstErr struct {
	done chan struct{}

	errStarted chan struct{}
	releaseErr chan struct{}

	errCalls atomic.Int32

	releaseOne sync.Once
}

func newSessionIDIdentityBlockingFirstErr() *sessionIDIdentityBlockingFirstErr {
	return &sessionIDIdentityBlockingFirstErr{
		done:       make(chan struct{}),
		errStarted: make(chan struct{}),
		releaseErr: make(chan struct{}),
	}
}

func (identity *sessionIDIdentityBlockingFirstErr) Done() <-chan struct{} { return identity.done }

func (identity *sessionIDIdentityBlockingFirstErr) Err() error {
	if identity.errCalls.Add(1) == 1 {
		close(identity.errStarted)
		<-identity.releaseErr
	}
	return discovery.ErrRuntimeIdentityLost
}

func (identity *sessionIDIdentityBlockingFirstErr) release() {
	identity.releaseOne.Do(func() { close(identity.releaseErr) })
}

type identityLosingSessionIDAllocator struct {
	identity *sessionIDIdentityBlockingFirstErr
}

func (allocator *identityLosingSessionIDAllocator) AllocateSessionID(context.Context) (int64, error) {
	close(allocator.identity.done)
	<-allocator.identity.errStarted
	return 47, nil
}

func newBlockingSessionIDAllocator() *blockingSessionIDAllocator {
	return &blockingSessionIDAllocator{started: make(chan struct{})}
}

func (allocator *blockingSessionIDAllocator) AllocateSessionID(ctx context.Context) (int64, error) {
	allocator.once.Do(func() { close(allocator.started) })
	<-ctx.Done()
	return 0, ctx.Err()
}

func (allocator *blockingSessionIDAllocator) waitUntilStarted(t *testing.T) {
	t.Helper()
	select {
	case <-allocator.started:
	case <-time.After(time.Second):
		t.Fatal("SessionID allocation did not start")
	}
}

// sessionIDRevisionFake 仅为准入测试提供独立编号，不模拟 etcd 持久化。
type sessionIDRevisionFake struct{ revision atomic.Int64 }

func (f *sessionIDRevisionFake) PutRevision(context.Context, string, []byte, int64) (int64, error) {
	return f.revision.Add(1), nil
}
