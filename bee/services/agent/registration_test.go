package agent

import (
	"slices"
	"testing"

	"github.com/ngnl47/tx/bee"
)

func TestRegistrationDeclaresAgentDependencies(t *testing.T) {
	registration, err := Registration("project-gateway", func(bee.Context) (Options, error) {
		return Options{}, nil
	})
	if err != nil {
		t.Fatalf("Registration returned error: %v", err)
	}
	want := []bee.CModID{
		bee.CModRPC,
		bee.CModRedisStorage,
		bee.CModDiscovery,
		bee.CModActor,
	}
	if registration.Spec.Name != "project-gateway" || !slices.Equal(registration.Spec.Requires, want) {
		t.Fatalf("registration spec = %#v", registration.Spec)
	}
}
