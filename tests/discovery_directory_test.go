package tests

import (
	"reflect"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestServiceDirectoryNextInstanceOnlyReturnsRunning(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	directory.Replace([]discovery.Instance{
		{ID: "battle-1", Services: []string{"battle"}, Endpoint: "10.0.0.1:9000", Status: discovery.StatusStopping},
		{ID: "battle-2", Services: []string{"battle"}, Endpoint: "10.0.0.2:9000", Status: discovery.StatusRunning},
	})

	instance, ok := directory.NextInstance("battle")
	if !ok {
		t.Fatal("NextInstance() returned no running battle instance")
	}
	if instance.ID != "battle-2" {
		t.Fatalf("NextInstance() instance ID = %q, want battle-2", instance.ID)
	}
}

func TestServiceDirectoryReadyClosesAfterInitialSnapshot(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	select {
	case <-directory.Ready():
		t.Fatal("Ready() closed before initial snapshot")
	default:
	}
	if err := directory.Replace(nil); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	select {
	case <-directory.Ready():
	default:
		t.Fatal("Ready() must close after initial snapshot")
	}
}

func TestServiceDirectoryInvalidationDoesNotCompleteInitialSnapshot(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	directory.Invalidate()
	select {
	case <-directory.Ready():
		t.Fatal("Ready() closed after invalidation without a snapshot")
	default:
	}
}

func TestServiceDirectoryRunningInstancesReturnsDefensiveRunningSnapshot(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{ID: "battle-3", Services: []string{"battle"}, Endpoint: "10.0.0.3:9000", Status: discovery.StatusRunning},
		{ID: "battle-1", Services: []string{"battle"}, Endpoint: "10.0.0.1:9000", Status: discovery.StatusStopping},
		{ID: "battle-2", Services: []string{"battle", "scene"}, Endpoint: "10.0.0.2:9000", Status: discovery.StatusRunning},
	}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}

	instances := directory.RunningInstances("battle")
	ids := make([]string, 0, len(instances))
	for _, instance := range instances {
		ids = append(ids, instance.ID)
	}
	if want := []string{"battle-2", "battle-3"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("running instance IDs = %v, want %v", ids, want)
	}

	instances[0].Services[0] = "changed"
	stored, ok := directory.LookupInstance("battle-2")
	if !ok || stored.Services[0] != "battle" {
		t.Fatalf("directory instance = %#v, want original services", stored)
	}
	stored.Services[0] = "mutated"
	stored, ok = directory.LookupInstance("battle-2")
	if !ok || stored.Services[0] != "battle" {
		t.Fatalf("LookupInstance() returned shared services = %#v", stored)
	}
	resolved, ok := directory.NextInstance("battle")
	if !ok {
		t.Fatal("NextInstance() returned no running battle instance")
	}
	resolved.Services[0] = "mutated"
	resolved, ok = directory.NextInstance("battle")
	if !ok || resolved.Services[0] != "battle" {
		t.Fatalf("NextInstance() returned shared services = %#v", resolved)
	}
}

func TestServiceDirectoryLookupInstanceKeepsStoppingInstanceForDrain(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	directory.Replace([]discovery.Instance{{
		ID:       "msgq-1",
		Services: []string{"msgq"},
		Endpoint: "10.0.0.1:9100",
		Status:   discovery.StatusStopping,
	}})

	instance, ok := directory.LookupInstance("msgq-1")
	if !ok {
		t.Fatal("LookupInstance() returned no stopping instance")
	}
	if instance.Endpoint != "10.0.0.1:9100" {
		t.Fatalf("LookupInstance() endpoint = %q, want 10.0.0.1:9100", instance.Endpoint)
	}
}

func TestServiceDirectoryKeepsDrainingInstanceForExactDrain(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "pod-uid-a",
		Services: []string{"game"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusDraining,
	}}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}

	if _, ok := directory.NextInstance("game"); ok {
		t.Fatal("NextInstance() selected draining instance")
	}
	instance, ok := directory.LookupInstance("pod-uid-a")
	if !ok {
		t.Fatal("LookupInstance() returned no draining instance")
	}
	if instance.Status != discovery.StatusDraining {
		t.Fatalf("LookupInstance() status = %q, want draining", instance.Status)
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

	if _, ok := directory.LookupInstance("battle-1"); ok {
		t.Fatal("LookupInstance() returned instance removed from the latest snapshot")
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
