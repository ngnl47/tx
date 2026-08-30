package ant

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ngnl47/tx/wal"
)

func TestCModProcedureBuilderRequiresWriteBack(t *testing.T) {
	mod, err := NewCModProcedureBuilder().Build()
	if err == nil || mod != nil {
		t.Fatalf("Build() = %#v, %v", mod, err)
	}
	if !strings.Contains(err.Error(), "writeback") {
		t.Fatalf("Build error = %q", err)
	}
}

func TestCModProcedureBuilderRejectsTypedNilComponents(t *testing.T) {
	var writeBack *procedureWriteBackFake
	if mod, err := NewCModProcedureBuilder().WriteBack(writeBack).Build(); err == nil || mod != nil {
		t.Fatalf("typed-nil writeBack Build() = %#v, %v", mod, err)
	}

	writeBack = &procedureWriteBackFake{}
	var log *procedureWALFake
	if mod, err := NewCModProcedureBuilder().WriteBack(writeBack).WAL(log).Build(); err == nil || mod != nil {
		t.Fatalf("typed-nil WAL Build() = %#v, %v", mod, err)
	}
}

func TestCModProcedureDeclaresStableTopology(t *testing.T) {
	mod, err := NewCModProcedureBuilder().
		WriteBack(&procedureWriteBackFake{}).
		WAL(&procedureWALFake{}).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	spec := mod.Spec()
	if spec.ID != CModProcedure {
		t.Fatalf("CMod ID = %q", spec.ID)
	}
	if len(spec.Components) != 2 || spec.Components[0].Role != "transaction" || spec.Components[1].Role != "writeback" {
		t.Fatalf("components = %#v", spec.Components)
	}
	if len(spec.Components[1].Components) != 1 || spec.Components[1].Components[0].Role != "wal" {
		t.Fatalf("writeback components = %#v", spec.Components[1].Components)
	}
}

func TestCModProcedureStartsBeforeServiceFactoryAndDrainsWriteBack(t *testing.T) {
	events := &procedureEvents{}
	mod, err := NewCModProcedureBuilder().WriteBack(&procedureWriteBackFake{events: events}).Build()
	if err != nil {
		t.Fatalf("Build CModProcedure: %v", err)
	}
	server, err := New(Config{AppID: "ant-test"}).
		CMods(mod).
		Services(ServiceRegistration{
			Spec: ServiceSpec{Name: "game", Requires: []CModID{CModProcedure}},
			Factory: func(runtime Context) (Service, error) {
				if _, err := runtime.CModProcedure(); err != nil {
					return nil, err
				}
				events.add("factory")
				return serviceFunc(func(context.Context) error {
					events.add("service:start")
					return nil
				}), nil
			},
		}).
		Build()
	if err != nil {
		t.Fatalf("Build Ant server: %v", err)
	}
	if err := server.Start(context.Background(), RunOptions{Services: "game"}); err != nil {
		t.Fatalf("Start Ant server: %v", err)
	}
	if err := server.Drain(context.Background()); err != nil {
		t.Fatalf("Drain Ant server: %v", err)
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("Stop Ant server: %v", err)
	}
	want := []string{"writeback:validate", "writeback:start", "factory", "service:start", "writeback:drain", "writeback:stop"}
	if got := events.values(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

type procedureWriteBackFake struct{ events *procedureEvents }

func (*procedureWriteBackFake) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "writeback", Name: "test"}
}

func (writeBack *procedureWriteBackFake) Validate(CModProcedureContext) error {
	writeBack.events.add("writeback:validate")
	return nil
}

func (writeBack *procedureWriteBackFake) Start(runtime CModProcedureContext) error {
	if runtime.Executor() == nil {
		return context.Canceled
	}
	writeBack.events.add("writeback:start")
	return nil
}

func (writeBack *procedureWriteBackFake) Drain(context.Context) error {
	writeBack.events.add("writeback:drain")
	return nil
}

func (writeBack *procedureWriteBackFake) Stop(context.Context) error {
	writeBack.events.add("writeback:stop")
	return nil
}

type procedureWALFake struct{}

func (*procedureWALFake) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "wal", Name: "test", Optional: true, Services: []string{"world"}}
}

func (*procedureWALFake) Validate(CModProcedureContext) error { return nil }
func (*procedureWALFake) Start(CModProcedureContext) (wal.Store, error) {
	return nil, nil
}
func (*procedureWALFake) Stop(context.Context) error { return nil }

type procedureEvents struct {
	mu     sync.Mutex
	events []string
}

func (events *procedureEvents) add(value string) {
	if events == nil {
		return
	}
	events.mu.Lock()
	events.events = append(events.events, value)
	events.mu.Unlock()
}

func (events *procedureEvents) values() []string {
	if events == nil {
		return nil
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	return append([]string(nil), events.events...)
}
