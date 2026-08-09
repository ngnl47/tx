package tests

import (
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestServiceDirectoryResolveServiceOnlyReturnsRunning(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	directory.Replace([]discovery.Instance{
		{ID: "battle-1", Services: []string{"battle"}, Endpoint: "10.0.0.1:9000", Status: discovery.StatusStopping},
		{ID: "battle-2", Services: []string{"battle"}, Endpoint: "10.0.0.2:9000", Status: discovery.StatusRunning},
	})

	instance, ok := directory.ResolveService("battle")
	if !ok {
		t.Fatal("ResolveService() returned no running battle instance")
	}
	if instance.ID != "battle-2" {
		t.Fatalf("ResolveService() instance ID = %q, want battle-2", instance.ID)
	}
}

func TestServiceDirectoryResolveIDKeepsStoppingInstanceForDrain(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	directory.Replace([]discovery.Instance{{
		ID:       "msgq-1",
		Services: []string{"msgq"},
		Endpoint: "10.0.0.1:9100",
		Status:   discovery.StatusStopping,
	}})

	instance, ok := directory.ResolveID("msgq-1")
	if !ok {
		t.Fatal("ResolveID() returned no stopping instance")
	}
	if instance.Endpoint != "10.0.0.1:9100" {
		t.Fatalf("ResolveID() endpoint = %q, want 10.0.0.1:9100", instance.Endpoint)
	}
}

func TestServiceDirectoryReplaceRemovesMissingInstance(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	directory.Replace([]discovery.Instance{{
		ID:       "battle-1",
		Services: []string{"battle"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}})
	directory.Replace(nil)

	if _, ok := directory.ResolveID("battle-1"); ok {
		t.Fatal("ResolveID() returned instance removed from the latest snapshot")
	}
}

func TestServiceDirectoryRejectsDuplicateServiceNames(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	err := directory.Replace([]discovery.Instance{{
		ID:       "game-1",
		Services: []string{"game", "game"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}})
	if err == nil {
		t.Fatal("Replace() error = nil, want duplicate service rejection")
	}
}
