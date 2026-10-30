package component

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

func TestHostValidatesAllBeforeStartingAnyComponent(t *testing.T) {
	wantErr := errors.New("invalid config")
	events := make([]string, 0, 2)
	graph := mustResolve(t, []Component{
		&hostTestComponent{id: "redis", events: &events},
		&hostTestComponent{id: "fusion", events: &events, validateErr: wantErr},
	})
	host := NewHost(lifecycle.NewWaitGroupContext(context.Background()), graph)

	err := host.Start(t.Context())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start error = %v, want %v", err, wantErr)
	}
	if len(events) != 0 {
		t.Fatalf("events = %v, want no component started", events)
	}
}

func TestHostRollsBackFailedStartInReverseOrder(t *testing.T) {
	wantErr := errors.New("procedure start failed")
	events := make([]string, 0, 6)
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
	graph := mustResolve(t, []Component{
		&hostTestComponent{id: "redis", events: &events},
		&hostTestComponent{id: "fusion", events: &events},
		&hostTestComponent{id: "procedure", events: &events, startErr: wantErr},
	})

	err := NewHost(root, graph).Start(t.Context())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start error = %v, want %v", err, wantErr)
	}
	want := []string{
		"start:redis", "start:fusion", "start:procedure",
		"stop:procedure", "stop:fusion", "stop:redis",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestHostStopsOwnedTasksBeforeDependencies(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
	owner := &ownedTaskComponent{exited: make(chan struct{})}
	graph := mustResolve(t, []Component{
		owner,
		&hostTestComponent{id: "failure", events: &[]string{}, startErr: errors.New("failed")},
	})

	if err := NewHost(root, graph).Start(t.Context()); err == nil {
		t.Fatal("Start error = nil, want failure")
	}
	if !owner.stopObservedExit {
		t.Fatal("Stop ran before owned task exited")
	}
}

func TestHostDrainsAndStopsInReverseOrder(t *testing.T) {
	events := make([]string, 0, 9)
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
	graph := mustResolve(t, []Component{
		&hostTestComponent{id: "redis", events: &events},
		&hostTestComponent{id: "fusion", events: &events},
		&hostTestComponent{id: "procedure", events: &events},
	})
	host := NewHost(root, graph)
	if err := host.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := host.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	want := []string{
		"start:redis", "start:fusion", "start:procedure",
		"drain:procedure", "drain:fusion", "drain:redis",
		"stop:procedure", "stop:fusion", "stop:redis",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestHostConcurrentStopSharesResult(t *testing.T) {
	wantErr := errors.New("stop failed")
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
	current := &hostTestComponent{id: "only", events: &[]string{}, stopErr: wantErr}
	host := NewHost(root, mustResolve(t, []Component{current}))
	if err := host.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const callers = 8
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			results <- host.Stop(context.Background())
		}()
	}
	ready.Wait()
	for range callers {
		if err := <-results; !errors.Is(err, wantErr) {
			t.Fatalf("Stop error = %v, want %v", err, wantErr)
		}
	}
	if current.stopCalls != 1 {
		t.Fatalf("Stop calls = %d, want 1", current.stopCalls)
	}
}

func TestHostDrainRetriesFailedComponentBeforeItsDependencies(t *testing.T) {
	events := []string{}
	wantErr := errors.New("flush unavailable")
	fusion := &hostTestComponent{id: "fusion", events: &events, drainErr: wantErr}
	host := NewHost(nil, mustResolve(t, []Component{
		&hostTestComponent{id: "redis", events: &events}, fusion,
		&hostTestComponent{id: "procedure", events: &events},
	}))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := host.Drain(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Drain = %v", err)
	}
	fusion.drainErr = nil
	if err := host.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:redis", "start:fusion", "start:procedure", "drain:procedure", "drain:fusion", "drain:fusion", "drain:redis"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestHostRejectsStartAfterStop(t *testing.T) {
	host := NewHost(nil, mustResolve(t, []Component{
		&hostTestComponent{id: "only", events: &[]string{}},
	}))
	if err := host.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := host.Start(t.Context()); !errors.Is(err, ErrHostStopped) {
		t.Fatalf("Start after Stop error = %v, want %v", err, ErrHostStopped)
	}
}

func mustResolve(t *testing.T, components []Component) Graph {
	t.Helper()
	roots := make([]Root, len(components))
	for index := range components {
		roots[index] = Root{Source: "test", ID: components[index].Spec().ID}
	}
	graph, err := Resolve(components, roots)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return graph
}

type hostTestComponent struct {
	id          ID
	events      *[]string
	validateErr error
	startErr    error
	drainErr    error
	stopErr     error
	stopCalls   int
}

func (component *hostTestComponent) Spec() Spec {
	return Spec{ID: component.id}
}

func (component *hostTestComponent) Validate(Context) error {
	return component.validateErr
}

func (component *hostTestComponent) Start(Context) error {
	*component.events = append(*component.events, "start:"+string(component.id))
	return component.startErr
}

func (component *hostTestComponent) Drain(context.Context) error {
	*component.events = append(*component.events, "drain:"+string(component.id))
	return component.drainErr
}

func (component *hostTestComponent) Stop(context.Context) error {
	component.stopCalls++
	*component.events = append(*component.events, "stop:"+string(component.id))
	return component.stopErr
}

type ownedTaskComponent struct {
	exited           chan struct{}
	stopObservedExit bool
}

func (*ownedTaskComponent) Spec() Spec { return Spec{ID: "owner"} }

func (component *ownedTaskComponent) Start(runtime Context) error {
	return runtime.Tasks().Go(func(ctx context.Context) {
		<-ctx.Done()
		close(component.exited)
	})
}

func (component *ownedTaskComponent) Stop(context.Context) error {
	select {
	case <-component.exited:
		component.stopObservedExit = true
	case <-time.After(time.Second):
	}
	return nil
}

// 第二个关闭调用的期限不能被第一个调用持有的阶段锁吞掉。
func TestHostConcurrentStopHonorsEachWaiterDeadline(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	current := &blockingStopComponent{entered: entered, release: release}
	host := NewHost(nil, mustResolve(t, []Component{current}))
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- host.Stop(t.Context()) }()
	<-entered
	second := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	go func() { second <- host.Stop(ctx) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrStopDeferred) {
			t.Fatalf("第二个等待者 = %v", err)
		}
	case <-time.After(time.Second):
		unblock()
		<-first
		<-second
		t.Fatal("并发关闭等待忽略了调用方期限")
	}
	unblock()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if current.calls != 1 {
		t.Fatalf("重复关闭资源: %d", current.calls)
	}
}

type blockingStopComponent struct {
	entered chan struct{}
	release chan struct{}
	calls   int
}

func (*blockingStopComponent) Spec() Spec          { return Spec{ID: "blocked"} }
func (*blockingStopComponent) Start(Context) error { return nil }
func (c *blockingStopComponent) Stop(context.Context) error {
	c.calls++
	close(c.entered)
	<-c.release
	return nil
}

// 已关闭资源的历史 deadline 是最终错误，不能推断为仍有实际工作未结束。
func TestHostTerminalDeadlineDoesNotBlockDependencyCleanup(t *testing.T) {
	events := []string{}
	closed := &hostTestComponent{id: "closed", events: &events, stopErr: context.DeadlineExceeded}
	host := NewHost(nil, mustResolve(t, []Component{
		&hostTestComponent{id: "dependency", events: &events}, closed,
	}))
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		err := host.Stop(t.Context())
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrStopDeferred) {
			t.Fatalf("最终关闭错误 = %v", err)
		}
	}
	if !slices.Equal(events, []string{"start:dependency", "start:closed", "stop:closed", "stop:dependency"}) {
		t.Fatalf("依赖未关闭或重复关闭: %v", events)
	}
}
