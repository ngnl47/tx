package discovery

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
)

func TestRuntimeStatusAndCloseOwnership(t *testing.T) {
	registry := &runtimeRegistryFake{}
	registration := &runtimeRegistrationFake{}
	slots := newRuntimeSlotStoreFake()
	runtime, err := newRuntime(registry, registration, slots)
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	if runtime.Directory() == nil || runtime.Slots() == nil {
		t.Fatal("Runtime did not expose cached directories")
	}
	if err := runtime.MarkRunning(context.Background()); err != nil {
		t.Fatalf("MarkRunning: %v", err)
	}
	if err := runtime.MarkDraining(context.Background()); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	if err := runtime.MarkStopping(context.Background()); err != nil {
		t.Fatalf("MarkStopping: %v", err)
	}
	want := []Status{StatusRunning, StatusDraining, StatusStopping}
	if !slices.Equal(registration.statuses, want) {
		t.Fatalf("statuses = %v, want %v", registration.statuses, want)
	}

	const callers = 4
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			if err := runtime.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	group.Wait()
	if registry.closeCalls != 1 || registration.closeCalls != 1 {
		t.Fatalf("close calls = registry:%d registration:%d, want 1 each", registry.closeCalls, registration.closeCalls)
	}
}

func TestRuntimeWaitReadyRequiresBothSnapshots(t *testing.T) {
	runtime, err := newRuntime(&runtimeRegistryFake{}, &runtimeRegistrationFake{}, newRuntimeSlotStoreFake())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.WaitReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady error = %v, want context canceled", err)
	}
}

func TestRuntimeExposesImmutableIdentityAndInstance(t *testing.T) {
	registration := newRuntimeRegistrationFake()
	registration.identity = RuntimeIdentity{
		InstanceID:  "txgame-17-291",
		WorkSlot:    17,
		ModRevision: 291,
	}
	registration.instance = Instance{
		ID:        registration.identity.InstanceID,
		Services:  []string{"login", "agent"},
		Endpoint:  "127.0.0.1:9000",
		Endpoints: map[string]string{"public": "game.example.com:443"},
		Status:    StatusStarting,
	}
	runtime, err := newRuntime(&runtimeRegistryFake{}, registration, newRuntimeSlotStoreFake())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	if got := runtime.Identity(); got != registration.identity {
		t.Fatalf("Identity = %#v, want %#v", got, registration.identity)
	}

	first := runtime.Instance()
	first.Services[0] = "changed"
	first.Endpoints["public"] = "changed"
	second := runtime.Instance()
	if second.Services[0] != "login" || second.Endpoints["public"] != "game.example.com:443" {
		t.Fatalf("Instance returned mutable state: %#v", second)
	}
}

func TestRuntimeCloseReportsRuntimeClosed(t *testing.T) {
	registration := newRuntimeRegistrationFake()
	runtime, err := newRuntime(&runtimeRegistryFake{}, registration, newRuntimeSlotStoreFake())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-runtime.Done():
	case <-t.Context().Done():
		t.Fatal("Runtime.Done was not closed")
	}
	if err := runtime.Err(); !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("Runtime.Err = %v, want ErrRuntimeClosed", err)
	}
}

func TestRuntimeMapsRegistrationLossToIdentityLost(t *testing.T) {
	registration := newRuntimeRegistrationFake()
	runtime, err := newRuntime(&runtimeRegistryFake{}, registration, newRuntimeSlotStoreFake())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	registration.finish(ErrEtcdLeaseLost)
	select {
	case <-runtime.Done():
	case <-t.Context().Done():
		t.Fatal("Runtime.Done was not closed")
	}
	if err := runtime.Err(); !errors.Is(err, ErrRuntimeIdentityLost) || !errors.Is(err, ErrEtcdLeaseLost) {
		t.Fatalf("Runtime.Err = %v, want identity lost with lease cause", err)
	}
}

func TestRuntimeCloseDoesNotMaskIdentityAlreadyLost(t *testing.T) {
	registration := newRuntimeRegistrationFake()
	registration.errEntered = make(chan struct{}, 1)
	registration.errGate = make(chan struct{})
	registration.closeEntered = make(chan struct{}, 1)
	runtime, err := newRuntime(&runtimeRegistryFake{}, registration, newRuntimeSlotStoreFake())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	registration.finish(ErrEtcdLeaseLost)
	select {
	case <-registration.errEntered:
	case <-t.Context().Done():
		t.Fatal("registration loss observer did not start")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- runtime.Close(context.Background()) }()
	select {
	case <-registration.closeEntered:
	case <-t.Context().Done():
		t.Fatal("registration close did not start")
	}
	close(registration.errGate)
	if err := <-closeResult; err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-runtime.Done():
	case <-t.Context().Done():
		t.Fatal("Runtime.Done was not closed")
	}
	if err := runtime.Err(); !errors.Is(err, ErrRuntimeIdentityLost) || !errors.Is(err, ErrEtcdLeaseLost) {
		t.Fatalf("Runtime.Err = %v, want identity loss with lease cause", err)
	}
}

type runtimeRegistryFake struct {
	closeCalls int
}

func (*runtimeRegistryFake) Watch(ctx context.Context, _ *ServiceDirectory) error {
	<-ctx.Done()
	return ctx.Err()
}

func (registry *runtimeRegistryFake) Close() error {
	registry.closeCalls++
	return nil
}

type runtimeRegistrationFake struct {
	statuses     []Status
	closeCalls   int
	identity     RuntimeIdentity
	instance     Instance
	done         chan struct{}
	doneOnce     sync.Once
	err          error
	mu           sync.Mutex
	errEntered   chan struct{}
	errGate      chan struct{}
	closeEntered chan struct{}
}

func newRuntimeRegistrationFake() *runtimeRegistrationFake {
	return &runtimeRegistrationFake{done: make(chan struct{})}
}

func (registration *runtimeRegistrationFake) SetStatus(_ context.Context, status Status) error {
	registration.statuses = append(registration.statuses, status)
	return nil
}

func (registration *runtimeRegistrationFake) Close(context.Context) error {
	registration.closeCalls++
	if registration.closeEntered != nil {
		select {
		case registration.closeEntered <- struct{}{}:
		default:
		}
	}
	registration.finish(ErrRegistrationDone)
	return nil
}

func (registration *runtimeRegistrationFake) Done() <-chan struct{} {
	registration.mu.Lock()
	if registration.done == nil {
		registration.done = make(chan struct{})
	}
	done := registration.done
	registration.mu.Unlock()
	return done
}

func (registration *runtimeRegistrationFake) Err() error {
	registration.mu.Lock()
	err := registration.err
	errEntered := registration.errEntered
	errGate := registration.errGate
	registration.mu.Unlock()
	if errEntered != nil {
		select {
		case errEntered <- struct{}{}:
		default:
		}
	}
	if errGate != nil {
		<-errGate
	}
	return err
}

func (registration *runtimeRegistrationFake) Identity() RuntimeIdentity {
	return registration.identity
}

func (registration *runtimeRegistrationFake) Instance() Instance {
	return cloneInstance(registration.instance)
}

func (registration *runtimeRegistrationFake) finish(err error) {
	registration.doneOnce.Do(func() {
		registration.mu.Lock()
		registration.err = err
		if registration.done == nil {
			registration.done = make(chan struct{})
		}
		done := registration.done
		registration.mu.Unlock()
		close(done)
	})
}

type runtimeSlotStoreFake struct {
	assignments map[string]SlotAssignment
}

func newRuntimeSlotStoreFake() *runtimeSlotStoreFake {
	return &runtimeSlotStoreFake{assignments: make(map[string]SlotAssignment)}
}

func (store *runtimeSlotStoreFake) ResolveSlot(_ context.Context, slotID string) (SlotAssignment, error) {
	assignment, ok := store.assignments[slotID]
	if !ok {
		return SlotAssignment{}, ErrSlotNotFound
	}
	return assignment, nil
}

func (store *runtimeSlotStoreFake) CreateSlot(_ context.Context, assignment SlotAssignment) error {
	store.assignments[assignment.SlotID] = assignment
	return nil
}

func (store *runtimeSlotStoreFake) PromoteSlot(_ context.Context, _, next SlotAssignment) error {
	store.assignments[next.SlotID] = next
	return nil
}

func (*runtimeSlotStoreFake) Watch(ctx context.Context, _ *SlotDirectory) error {
	<-ctx.Done()
	return ctx.Err()
}
