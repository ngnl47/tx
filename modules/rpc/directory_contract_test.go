package rpc

import (
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestResolveDependsOnDirectoryContract(t *testing.T) {
	directory := fixedDirectory{instance: discovery.Instance{
		ID:       "game-1",
		Services: []string{"game"},
		Endpoint: "127.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}}
	instance, err := Resolve(directory, ServiceRoute("game"))
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if instance.ID != "game-1" {
		t.Fatalf("resolved instance = %q", instance.ID)
	}
}

type fixedDirectory struct {
	instance discovery.Instance
}

func (directory fixedDirectory) NextInstance(string) (discovery.Instance, bool) {
	return directory.instance, true
}

func (directory fixedDirectory) RunningInstances(string) []discovery.Instance {
	return []discovery.Instance{directory.instance}
}

func (directory fixedDirectory) LookupInstance(string) (discovery.Instance, bool) {
	return directory.instance, true
}
