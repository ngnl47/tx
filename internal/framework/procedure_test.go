package framework

import (
	"context"
	"reflect"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
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

func TestProcedureBuilderRejectsInvalidComponentTopology(t *testing.T) {
	tests := []struct {
		name      string
		writeBack ProcedureWriteBack
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
					{Role: "child", Name: "first"},
					{Role: "child", Name: "second"},
				},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewProcedureBuilder().WriteBack(test.writeBack)
			mod, err := builder.Build()
			if mod != nil || err == nil {
				t.Fatalf("Build = %#v, %v, want invalid component topology error", mod, err)
			}
		})
	}
}

type recordingProcedureWriteBack struct {
	events *[]string
}

func (*recordingProcedureWriteBack) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "writeback", Name: "recording"}
}

func (writeBack *recordingProcedureWriteBack) Validate(runtime ProcedureContext) error {
	*writeBack.events = append(*writeBack.events, "validate:writeback")
	return nil
}

func (writeBack *recordingProcedureWriteBack) Start(runtime ProcedureContext) error {
	*writeBack.events = append(*writeBack.events, "start:writeback")
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
