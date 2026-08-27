package ant

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ngnl47/tx/wal"
)

func TestCModProcedureBuilderRequiresCache(t *testing.T) {
	mod, err := NewCModProcedureBuilder().Build()
	if err == nil || mod != nil {
		t.Fatalf("Build() = %#v, %v", mod, err)
	}
	if !strings.Contains(err.Error(), "cache") {
		t.Fatalf("Build error = %q", err)
	}
}

func TestCModProcedureBuilderRejectsTypedNilComponents(t *testing.T) {
	var cache *procedureCacheFake
	if mod, err := NewCModProcedureBuilder().Cache(cache).Build(); err == nil || mod != nil {
		t.Fatalf("typed-nil cache Build() = %#v, %v", mod, err)
	}

	cache = &procedureCacheFake{}
	var log *procedureWALFake
	if mod, err := NewCModProcedureBuilder().Cache(cache).WAL(log).Build(); err == nil || mod != nil {
		t.Fatalf("typed-nil WAL Build() = %#v, %v", mod, err)
	}
}

func TestCModProcedureDeclaresStableTopology(t *testing.T) {
	mod, err := NewCModProcedureBuilder().
		Cache(&procedureCacheFake{}).
		WAL(&procedureWALFake{}).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	spec := mod.Spec()
	if spec.ID != CModProcedure {
		t.Fatalf("CMod ID = %q", spec.ID)
	}
	if len(spec.Components) != 2 || spec.Components[0].Role != "transaction" || spec.Components[1].Role != "cache" {
		t.Fatalf("components = %#v", spec.Components)
	}
	if len(spec.Components[1].Components) != 1 || spec.Components[1].Components[0].Role != "wal" {
		t.Fatalf("cache components = %#v", spec.Components[1].Components)
	}
}

func TestCModProcedureStartsBeforeServiceFactoryAndDrainsCache(t *testing.T) {
	events := &procedureEvents{}
	mod, err := NewCModProcedureBuilder().Cache(&procedureCacheFake{events: events}).Build()
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
	want := []string{"cache:validate", "cache:start", "factory", "service:start", "cache:drain", "cache:stop"}
	if got := events.values(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

type procedureCacheFake struct{ events *procedureEvents }

func (*procedureCacheFake) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "cache", Name: "test"}
}

func (cache *procedureCacheFake) Validate(CModProcedureContext) error {
	cache.events.add("cache:validate")
	return nil
}

func (cache *procedureCacheFake) Start(runtime CModProcedureContext) error {
	if runtime.Executor() == nil {
		return context.Canceled
	}
	cache.events.add("cache:start")
	return nil
}

func (cache *procedureCacheFake) Drain(context.Context) error {
	cache.events.add("cache:drain")
	return nil
}

func (cache *procedureCacheFake) Stop(context.Context) error {
	cache.events.add("cache:stop")
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
