package bee

import (
	"context"
	"strings"
	"testing"

	framework "github.com/ngnl47/tx/internal/framework"
)

func TestBuilderConstructsAndStartsStatelessServer(t *testing.T) {
	started := false
	registration := ServiceRegistration{
		Spec: ServiceSpec{Name: "game"},
		Factory: func(runtime Context) (Service, error) {
			if runtime.AppID() != "bee-test" {
				t.Fatalf("AppID = %q", runtime.AppID())
			}
			return serviceFunc(func(context.Context) error {
				started = true
				return nil
			}), nil
		},
	}

	server, err := New(Config{AppID: "bee-test"}).Services(registration).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if err := server.Start(context.Background(), RunOptions{Services: "game"}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if !started {
		t.Fatal("service was not started")
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

func TestRunOptionsPublishClonedInstanceEndpoints(t *testing.T) {
	var runtime Context
	server, err := New(Config{AppID: "bee-test"}).Services(ServiceRegistration{
		Spec: ServiceSpec{Name: "agent"},
		Factory: func(current Context) (Service, error) {
			runtime = current
			return serviceFunc(func(context.Context) error { return nil }), nil
		},
	}).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	endpoints := map[string]string{"agent.tcp": "10.0.0.7:9000"}
	if err := server.Start(context.Background(), RunOptions{
		Services:          "agent",
		InstanceID:        "agent-a",
		InstanceEndpoints: endpoints,
	}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	endpoints["agent.tcp"] = "tampered-input"
	instance := runtime.Instance()
	if got := instance.Endpoints["agent.tcp"]; got != "10.0.0.7:9000" {
		t.Fatalf("agent.tcp = %q, want 10.0.0.7:9000", got)
	}
	instance.Endpoints["agent.tcp"] = "tampered-result"
	if got := runtime.Instance().Endpoints["agent.tcp"]; got != "10.0.0.7:9000" {
		t.Fatalf("second agent.tcp = %q, want 10.0.0.7:9000", got)
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

func TestStorageReadersAdapterPreservesNilOnUnbind(t *testing.T) {
	var readers framework.StorageReaders
	if adapted := adaptStorageReaders(readers); adapted != nil {
		t.Fatalf("adapted nil readers = %#v", adapted)
	}
}

func TestSetupContextStorageReturnsErrorWhenKernelIsUnavailable(t *testing.T) {
	runtime := newSetupContext(nil)
	if _, err := runtime.Storage(); err == nil {
		t.Fatal("Storage error = nil, want unavailable context error")
	}
}

func TestServiceContextRejectsUndeclaredCMod(t *testing.T) {
	registration := ServiceRegistration{
		Spec: ServiceSpec{Name: "game"},
		Factory: func(runtime Context) (Service, error) {
			_, err := runtime.CModActor()
			return nil, err
		},
	}

	server, err := New(Config{AppID: "bee-test"}).Services(registration).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	err = server.Start(context.Background(), RunOptions{Services: "game"})
	if err == nil {
		t.Fatal("Start succeeded")
	}
	for _, fragment := range []string{"game", string(CModActor), "declared"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("Start error = %q, want fragment %q", err, fragment)
		}
	}
}

type serviceFunc func(context.Context) error

func (service serviceFunc) Start(ctx context.Context) error {
	return service(ctx)
}
