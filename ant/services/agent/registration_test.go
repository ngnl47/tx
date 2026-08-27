package agent

import (
	"slices"
	"testing"

	"github.com/ngnl47/tx/ant"
)

func TestRegistrationDeclaresAgentDependencies(t *testing.T) {
	registration, err := Registration(func(ant.Context) (Options, error) {
		return Options{}, nil
	}, nil)
	if err != nil {
		t.Fatalf("Registration returned error: %v", err)
	}
	want := []ant.CModID{
		ant.CModRPC,
		ant.CModRedisStorage,
		ant.CModDiscovery,
		ant.CModActor,
	}
	if registration.Spec.Name != ServiceName || !slices.Equal(registration.Spec.Requires, want) {
		t.Fatalf("registration spec = %#v", registration.Spec)
	}
}
