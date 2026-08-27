package server

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/wal"
)

type stateCacheComponentFake struct{}

func (stateCacheComponentFake) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "cache", Name: "test-cache"}
}

func (stateCacheComponentFake) Validate(StateRuntimeContext) error { return nil }
func (stateCacheComponentFake) Start(StateRuntimeContext) error    { return nil }
func (stateCacheComponentFake) Drain(context.Context) error        { return nil }
func (stateCacheComponentFake) Stop(context.Context) error         { return nil }

type invalidStateCacheComponentFake struct {
	spec ComponentSpec
}

func (fake invalidStateCacheComponentFake) ComponentSpec() ComponentSpec  { return fake.spec }
func (invalidStateCacheComponentFake) Validate(StateRuntimeContext) error { return nil }
func (invalidStateCacheComponentFake) Start(StateRuntimeContext) error    { return nil }
func (invalidStateCacheComponentFake) Drain(context.Context) error        { return nil }
func (invalidStateCacheComponentFake) Stop(context.Context) error         { return nil }

func TestStateRuntimeBuilderDescribesSelectedComponents(t *testing.T) {
	mod, err := NewStateRuntimeBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(MongoWALForServices("slgworld")).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	spec := mod.Spec()
	if spec.ID != CModStateRuntime {
		t.Fatalf("CMod ID = %q, want %q", spec.ID, CModStateRuntime)
	}
	want := []ComponentSpec{
		{Role: "transaction", Name: "default"},
		{
			Role: "cache",
			Name: "test-cache",
			Components: []ComponentSpec{
				{Role: "wal", Name: "mongo", Optional: true, Services: []string{"slgworld"}},
			},
		},
	}
	if !equalComponentSpecs(spec.Components, want) {
		t.Fatalf("components = %#v, want %#v", spec.Components, want)
	}
}

func TestStateRuntimeBuilderRequiresCache(t *testing.T) {
	mod, err := NewStateRuntimeBuilder().Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want missing cache error", mod, err)
	}
}

func TestStateRuntimeBuilderRejectsTypedNilCache(t *testing.T) {
	var cache *recordingStateCache
	mod, err := NewStateRuntimeBuilder().Cache(cache).Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want typed nil cache error", mod, err)
	}
}

func TestStateRuntimeBuilderRejectsWALWithoutServices(t *testing.T) {
	mod, err := NewStateRuntimeBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(MongoWALForServices()).
		Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want empty WAL services error", mod, err)
	}
}

func TestStateRuntimeBuilderRejectsNilWALComponent(t *testing.T) {
	mod, err := NewStateRuntimeBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(nil).
		Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want nil WAL component error", mod, err)
	}
}

func TestStateRuntimeBuilderRejectsInvalidComponentTopology(t *testing.T) {
	tests := []struct {
		name  string
		cache StateCache
		wal   StateWAL
	}{
		{
			name:  "empty role",
			cache: invalidStateCacheComponentFake{spec: ComponentSpec{Name: "cache"}},
		},
		{
			name: "duplicate child role",
			cache: invalidStateCacheComponentFake{spec: ComponentSpec{
				Role: "cache",
				Name: "custom",
				Components: []ComponentSpec{
					{Role: "wal", Name: "custom"},
				},
			}},
			wal: MongoWALForServices("slgworld"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewStateRuntimeBuilder().Cache(test.cache)
			if test.wal != nil {
				builder.WAL(test.wal)
			}
			mod, err := builder.Build()
			if mod != nil || err == nil {
				t.Fatalf("Build = %#v, %v, want invalid component topology error", mod, err)
			}
		})
	}
}

func TestMongoWALComponentMatchesConfiguredServices(t *testing.T) {
	component := MongoWALForServices("slgworld", "battlefield")
	if !componentEnabled(component.ComponentSpec(), newServiceSelection([]Service{
		testService{spec: Spec{Name: "slgworld"}},
	})) {
		t.Fatal("WAL component was not enabled for slgworld")
	}
	if componentEnabled(component.ComponentSpec(), newServiceSelection([]Service{
		testService{spec: Spec{Name: "slggame"}},
	})) {
		t.Fatal("WAL component was enabled for unrelated service")
	}
}

type recordingStateCache struct {
	events     *[]string
	walEnabled []bool
}

func (*recordingStateCache) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "cache", Name: "recording"}
}

func (cache *recordingStateCache) Validate(runtime StateRuntimeContext) error {
	*cache.events = append(*cache.events, "validate:cache")
	cache.walEnabled = append(cache.walEnabled, runtime.WALEnabled())
	return nil
}

func (cache *recordingStateCache) Start(runtime StateRuntimeContext) error {
	*cache.events = append(*cache.events, "start:cache")
	cache.walEnabled = append(cache.walEnabled, runtime.WALEnabled())
	if runtime.Transaction() == nil {
		return context.Canceled
	}
	return nil
}

func (cache *recordingStateCache) Drain(context.Context) error {
	*cache.events = append(*cache.events, "drain:cache")
	return nil
}

func (cache *recordingStateCache) Stop(context.Context) error {
	*cache.events = append(*cache.events, "stop:cache")
	return nil
}

func TestStateRuntimeOwnsTransactionAndCacheLifecycle(t *testing.T) {
	events := []string{}
	cache := &recordingStateCache{events: &events}
	mod, err := NewStateRuntimeBuilder().Cache(cache).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	server := &Server{runtime: &runtime{
		lifecycle: root,
		instance:  testServiceInstance("slggame"),
		selectedCMods: map[CModID]struct{}{
			CModStateRuntime: {},
		},
	}}
	ctx := newCModContext(server)

	if err := mod.(CModValidator).Validate(ctx); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if server.runtime.transaction == nil {
		t.Fatal("transaction runtime was not installed")
	}
	if err := mod.(CModDrainer).Drain(context.Background()); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if err := mod.(CModStopper).Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	wantEvents := []string{"validate:cache", "start:cache", "drain:cache", "stop:cache"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
	if want := []bool{false, false}; !reflect.DeepEqual(cache.walEnabled, want) {
		t.Fatalf("WAL enabled values = %v, want %v", cache.walEnabled, want)
	}
}

func TestStateRuntimeWALRequiresSelectedMongoCapability(t *testing.T) {
	mod, err := NewStateRuntimeBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(MongoWALForServices("slgworld")).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	server := &Server{runtime: &runtime{
		instance: testServiceInstance("slgworld"),
		selectedCMods: map[CModID]struct{}{
			CModStateRuntime: {},
		},
	}}
	err = mod.(CModValidator).Validate(newCModContext(server))
	if err == nil {
		t.Fatal("Validate returned nil without Mongo capability")
	}
}

type recordingStateWAL struct {
	events   *[]string
	store    wal.Store
	startErr error
}

func (*recordingStateWAL) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "wal", Name: "recording", Optional: true, Services: []string{"custom-state"}}
}

func (component *recordingStateWAL) Validate(StateRuntimeContext) error {
	*component.events = append(*component.events, "validate:wal")
	return nil
}

func (component *recordingStateWAL) Start(StateRuntimeContext) (wal.Store, error) {
	*component.events = append(*component.events, "start:wal")
	return component.store, component.startErr
}

func (component *recordingStateWAL) Stop(context.Context) error {
	*component.events = append(*component.events, "stop:wal")
	return nil
}

type stateWALStoreFake struct{}

func (stateWALStoreFake) Append(context.Context, wal.Record) error { return nil }
func (stateWALStoreFake) Read(context.Context, wal.Stream, uint64, int) ([]wal.Record, error) {
	return nil, nil
}
func (stateWALStoreFake) Replay(context.Context, wal.Stream, uint64, func(context.Context, wal.Record) error) (uint64, error) {
	return 0, nil
}
func (stateWALStoreFake) Checkpoint(context.Context, wal.Stream, uint64, func(context.Context) error) error {
	return nil
}

func TestStateRuntimeOwnsCustomWALLifecycle(t *testing.T) {
	events := []string{}
	store := stateWALStoreFake{}
	cache := &recordingStateCache{events: &events}
	component := &recordingStateWAL{
		events: &events,
		store:  store,
	}
	mod, err := NewStateRuntimeBuilder().Cache(cache).WAL(component).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	server := &Server{runtime: &runtime{
		lifecycle: root,
		storage:   newStorageRuntime(root),
		instance:  testServiceInstance("custom-state"),
		selectedCMods: map[CModID]struct{}{
			CModStateRuntime: {},
		},
	}}
	ctx := newCModContext(server)
	if err := mod.(CModValidator).Validate(ctx); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if server.runtime.wal != store {
		t.Fatalf("installed WAL = %#v, want %#v", server.runtime.wal, store)
	}
	if err := mod.(CModStopper).Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if server.runtime.wal != nil {
		t.Fatalf("WAL after Stop = %#v, want nil", server.runtime.wal)
	}
	wantEvents := []string{"validate:wal", "validate:cache", "start:wal", "start:cache", "stop:cache", "stop:wal"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}

func TestStateRuntimeStopsCustomWALAfterFailedStart(t *testing.T) {
	events := []string{}
	component := &recordingStateWAL{
		events:   &events,
		startErr: errors.New("start failed"),
	}
	mod, err := NewStateRuntimeBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(component).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	server := &Server{runtime: &runtime{
		lifecycle: root,
		instance:  testServiceInstance("custom-state"),
		selectedCMods: map[CModID]struct{}{
			CModStateRuntime: {},
		},
	}}
	ctx := newCModContext(server)
	if err := mod.(CModValidator).Validate(ctx); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if err := mod.Start(ctx); err == nil {
		t.Fatal("Start returned nil")
	}
	if err := mod.(CModStopper).Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if want := []string{"validate:wal", "start:wal", "stop:wal"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestStateRuntimeDoesNotClearExistingWALAfterInstallConflict(t *testing.T) {
	events := []string{}
	existing := stateWALStoreFake{}
	component := &recordingStateWAL{events: &events, store: &stateWALStoreFake{}}
	mod, err := NewStateRuntimeBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(component).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	server := &Server{runtime: &runtime{
		lifecycle: root,
		wal:       existing,
		instance:  testServiceInstance("custom-state"),
		selectedCMods: map[CModID]struct{}{
			CModStateRuntime: {},
		},
	}}
	ctx := newCModContext(server)
	if err := mod.(CModValidator).Validate(ctx); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if err := mod.Start(ctx); err == nil {
		t.Fatal("Start returned nil")
	}
	if err := mod.(CModStopper).Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if server.runtime.wal != existing {
		t.Fatalf("WAL after rollback = %#v, want existing %#v", server.runtime.wal, existing)
	}
	if want := []string{"validate:wal", "start:wal", "stop:wal"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func testServiceInstance(names ...string) discovery.Instance {
	return discovery.Instance{Services: names}
}

func equalComponentSpecs(left, right []ComponentSpec) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Role != right[index].Role ||
			left[index].Name != right[index].Name ||
			left[index].Optional != right[index].Optional ||
			!equalStrings(left[index].Services, right[index].Services) ||
			!equalComponentSpecs(left[index].Components, right[index].Components) {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
