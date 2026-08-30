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

type stateWriteBackComponentFake struct{}

func (stateWriteBackComponentFake) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "writeback", Name: "test-writeback"}
}

func (stateWriteBackComponentFake) Validate(ProcedureContext) error { return nil }
func (stateWriteBackComponentFake) Start(ProcedureContext) error    { return nil }
func (stateWriteBackComponentFake) Drain(context.Context) error     { return nil }
func (stateWriteBackComponentFake) Stop(context.Context) error      { return nil }

type invalidProcedureWriteBackFake struct {
	spec ComponentSpec
}

func (fake invalidProcedureWriteBackFake) ComponentSpec() ComponentSpec { return fake.spec }
func (invalidProcedureWriteBackFake) Validate(ProcedureContext) error   { return nil }
func (invalidProcedureWriteBackFake) Start(ProcedureContext) error      { return nil }
func (invalidProcedureWriteBackFake) Drain(context.Context) error       { return nil }
func (invalidProcedureWriteBackFake) Stop(context.Context) error        { return nil }

func TestProcedureBuilderDescribesSelectedComponents(t *testing.T) {
	mod, err := NewProcedureBuilder().
		WriteBack(stateWriteBackComponentFake{}).
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
			Role: "writeback",
			Name: "test-writeback",
			Components: []ComponentSpec{
				{Role: "wal", Name: "mongo", Optional: true, Services: []string{"slgworld"}},
			},
		},
	}
	if !equalComponentSpecs(spec.Components, want) {
		t.Fatalf("components = %#v, want %#v", spec.Components, want)
	}
}

func TestProcedureBuilderRequiresWriteBack(t *testing.T) {
	mod, err := NewProcedureBuilder().Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want missing writeback error", mod, err)
	}
}

func TestProcedureBuilderRejectsTypedNilWriteBack(t *testing.T) {
	var writeBack *recordingProcedureWriteBack
	mod, err := NewProcedureBuilder().WriteBack(writeBack).Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want typed nil writeback error", mod, err)
	}
}

func TestProcedureBuilderRejectsWALWithoutServices(t *testing.T) {
	mod, err := NewProcedureBuilder().
		WriteBack(stateWriteBackComponentFake{}).
		WAL(NewProcedureMongoWAL()).
		Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want empty WAL services error", mod, err)
	}
}

func TestProcedureBuilderRejectsNilWALComponent(t *testing.T) {
	mod, err := NewProcedureBuilder().
		WriteBack(stateWriteBackComponentFake{}).
		WAL(nil).
		Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want nil WAL component error", mod, err)
	}
}

func TestProcedureBuilderRejectsInvalidComponentTopology(t *testing.T) {
	tests := []struct {
		name      string
		writeBack ProcedureWriteBack
		wal       ProcedureWAL
	}{
		{
			name:      "empty role",
			writeBack: invalidProcedureWriteBackFake{spec: ComponentSpec{Name: "writeback"}},
		},
		{
			name: "duplicate child role",
			writeBack: invalidProcedureWriteBackFake{spec: ComponentSpec{
				Role: "writeback",
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
			builder := NewProcedureBuilder().WriteBack(test.writeBack)
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

type recordingProcedureWriteBack struct {
	events     *[]string
	walEnabled []bool
}

func (*recordingProcedureWriteBack) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "writeback", Name: "recording"}
}

func (writeBack *recordingProcedureWriteBack) Validate(runtime ProcedureContext) error {
	*writeBack.events = append(*writeBack.events, "validate:writeback")
	writeBack.walEnabled = append(writeBack.walEnabled, runtime.WALEnabled())
	return nil
}

func (writeBack *recordingProcedureWriteBack) Start(runtime ProcedureContext) error {
	*writeBack.events = append(*writeBack.events, "start:writeback")
	writeBack.walEnabled = append(writeBack.walEnabled, runtime.WALEnabled())
	if runtime.Transaction() == nil {
		return context.Canceled
	}
	return nil
}

func (writeBack *recordingProcedureWriteBack) Drain(context.Context) error {
	*writeBack.events = append(*writeBack.events, "drain:writeback")
	return nil
}

func (writeBack *recordingProcedureWriteBack) Stop(context.Context) error {
	*writeBack.events = append(*writeBack.events, "stop:writeback")
	return nil
}

func TestProcedureOwnsTransactionAndWriteBackLifecycle(t *testing.T) {
	events := []string{}
	writeBack := &recordingProcedureWriteBack{events: &events}
	mod, err := NewProcedureBuilder().WriteBack(writeBack).Build()
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
	wantEvents := []string{"validate:writeback", "start:writeback", "drain:writeback", "stop:writeback"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
	if want := []bool{false, false}; !reflect.DeepEqual(writeBack.walEnabled, want) {
		t.Fatalf("WAL enabled values = %v, want %v", writeBack.walEnabled, want)
	}
}

func TestProcedureWALRequiresSelectedMongoCapability(t *testing.T) {
	mod, err := NewProcedureBuilder().
		WriteBack(stateWriteBackComponentFake{}).
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
	writeBack := &recordingProcedureWriteBack{events: &events}
	component := &recordingProcedureWAL{
		events: &events,
		store:  store,
	}
	mod, err := NewProcedureBuilder().WriteBack(writeBack).WAL(component).Build()
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
	wantEvents := []string{"validate:wal", "validate:writeback", "start:wal", "start:writeback", "stop:writeback", "stop:wal"}
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
		WriteBack(stateWriteBackComponentFake{}).
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
		WriteBack(stateWriteBackComponentFake{}).
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
