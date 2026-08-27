package framework

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

func (stateCacheComponentFake) Validate(ProcedureContext) error { return nil }
func (stateCacheComponentFake) Start(ProcedureContext) error    { return nil }
func (stateCacheComponentFake) Drain(context.Context) error     { return nil }
func (stateCacheComponentFake) Stop(context.Context) error      { return nil }

type invalidProcedureCacheFake struct {
	spec ComponentSpec
}

func (fake invalidProcedureCacheFake) ComponentSpec() ComponentSpec { return fake.spec }
func (invalidProcedureCacheFake) Validate(ProcedureContext) error   { return nil }
func (invalidProcedureCacheFake) Start(ProcedureContext) error      { return nil }
func (invalidProcedureCacheFake) Drain(context.Context) error       { return nil }
func (invalidProcedureCacheFake) Stop(context.Context) error        { return nil }

func TestProcedureBuilderDescribesSelectedComponents(t *testing.T) {
	mod, err := NewProcedureBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(NewProcedureMongoWAL("slgworld")).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	spec := mod.Spec()
	if spec.ID != CModProcedure {
		t.Fatalf("CMod ID = %q, want %q", spec.ID, CModProcedure)
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

func TestProcedureBuilderRequiresCache(t *testing.T) {
	mod, err := NewProcedureBuilder().Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want missing cache error", mod, err)
	}
}

func TestProcedureBuilderRejectsTypedNilCache(t *testing.T) {
	var cache *recordingProcedureCache
	mod, err := NewProcedureBuilder().Cache(cache).Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want typed nil cache error", mod, err)
	}
}

func TestProcedureBuilderRejectsWALWithoutServices(t *testing.T) {
	mod, err := NewProcedureBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(NewProcedureMongoWAL()).
		Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want empty WAL services error", mod, err)
	}
}

func TestProcedureBuilderRejectsNilWALComponent(t *testing.T) {
	mod, err := NewProcedureBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(nil).
		Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want nil WAL component error", mod, err)
	}
}

func TestProcedureBuilderRejectsInvalidComponentTopology(t *testing.T) {
	tests := []struct {
		name  string
		cache ProcedureCache
		wal   ProcedureWAL
	}{
		{
			name:  "empty role",
			cache: invalidProcedureCacheFake{spec: ComponentSpec{Name: "cache"}},
		},
		{
			name: "duplicate child role",
			cache: invalidProcedureCacheFake{spec: ComponentSpec{
				Role: "cache",
				Name: "custom",
				Components: []ComponentSpec{
					{Role: "wal", Name: "custom"},
				},
			}},
			wal: NewProcedureMongoWAL("slgworld"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewProcedureBuilder().Cache(test.cache)
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
	component := NewProcedureMongoWAL("slgworld", "battlefield")
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

type recordingProcedureCache struct {
	events     *[]string
	walEnabled []bool
}

func (*recordingProcedureCache) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "cache", Name: "recording"}
}

func (cache *recordingProcedureCache) Validate(runtime ProcedureContext) error {
	*cache.events = append(*cache.events, "validate:cache")
	cache.walEnabled = append(cache.walEnabled, runtime.WALEnabled())
	return nil
}

func (cache *recordingProcedureCache) Start(runtime ProcedureContext) error {
	*cache.events = append(*cache.events, "start:cache")
	cache.walEnabled = append(cache.walEnabled, runtime.WALEnabled())
	if runtime.Transaction() == nil {
		return context.Canceled
	}
	return nil
}

func (cache *recordingProcedureCache) Drain(context.Context) error {
	*cache.events = append(*cache.events, "drain:cache")
	return nil
}

func (cache *recordingProcedureCache) Stop(context.Context) error {
	*cache.events = append(*cache.events, "stop:cache")
	return nil
}

func TestProcedureOwnsTransactionAndCacheLifecycle(t *testing.T) {
	events := []string{}
	cache := &recordingProcedureCache{events: &events}
	mod, err := NewProcedureBuilder().Cache(cache).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	server := &Server{runtime: &runtime{
		lifecycle: root,
		instance:  testServiceInstance("slggame"),
		selectedCMods: map[CModID]struct{}{
			CModProcedure: {},
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

func TestProcedureWALRequiresSelectedMongoCapability(t *testing.T) {
	mod, err := NewProcedureBuilder().
		Cache(stateCacheComponentFake{}).
		WAL(NewProcedureMongoWAL("slgworld")).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	server := &Server{runtime: &runtime{
		instance: testServiceInstance("slgworld"),
		selectedCMods: map[CModID]struct{}{
			CModProcedure: {},
		},
	}}
	err = mod.(CModValidator).Validate(newCModContext(server))
	if err == nil {
		t.Fatal("Validate returned nil without Mongo capability")
	}
}

type recordingProcedureWAL struct {
	events   *[]string
	store    wal.Store
	startErr error
}

func (*recordingProcedureWAL) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "wal", Name: "recording", Optional: true, Services: []string{"custom-state"}}
}

func (component *recordingProcedureWAL) Validate(ProcedureContext) error {
	*component.events = append(*component.events, "validate:wal")
	return nil
}

func (component *recordingProcedureWAL) Start(ProcedureContext) (wal.Store, error) {
	*component.events = append(*component.events, "start:wal")
	return component.store, component.startErr
}

func (component *recordingProcedureWAL) Stop(context.Context) error {
	*component.events = append(*component.events, "stop:wal")
	return nil
}

type procedureWALStoreFake struct{}

func (procedureWALStoreFake) Append(context.Context, wal.Record) error { return nil }
func (procedureWALStoreFake) Read(context.Context, wal.Stream, uint64, int) ([]wal.Record, error) {
	return nil, nil
}
func (procedureWALStoreFake) Replay(context.Context, wal.Stream, uint64, func(context.Context, wal.Record) error) (uint64, error) {
	return 0, nil
}
func (procedureWALStoreFake) Checkpoint(context.Context, wal.Stream, uint64, func(context.Context) error) error {
	return nil
}

func TestProcedureOwnsCustomWALLifecycle(t *testing.T) {
	events := []string{}
	store := procedureWALStoreFake{}
	cache := &recordingProcedureCache{events: &events}
	component := &recordingProcedureWAL{
		events: &events,
		store:  store,
	}
	mod, err := NewProcedureBuilder().Cache(cache).WAL(component).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	server := &Server{runtime: &runtime{
		lifecycle: root,
		storage:   newStorageRuntime(root),
		instance:  testServiceInstance("custom-state"),
		selectedCMods: map[CModID]struct{}{
			CModProcedure: {},
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

func TestProcedureStopsCustomWALAfterFailedStart(t *testing.T) {
	events := []string{}
	component := &recordingProcedureWAL{
		events:   &events,
		startErr: errors.New("start failed"),
	}
	mod, err := NewProcedureBuilder().
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
			CModProcedure: {},
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

func TestProcedureDoesNotClearExistingWALAfterInstallConflict(t *testing.T) {
	events := []string{}
	existing := procedureWALStoreFake{}
	component := &recordingProcedureWAL{events: &events, store: &procedureWALStoreFake{}}
	mod, err := NewProcedureBuilder().
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
			CModProcedure: {},
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
