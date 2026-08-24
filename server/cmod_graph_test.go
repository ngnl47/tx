package server

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestResolveCModGraphReturnsStableDependencyOrder(t *testing.T) {
	cmods := []CMod{
		cmodGraphFake{id: "rpc"},
		cmodGraphFake{id: "redis"},
		cmodGraphFake{id: "mongo"},
		cmodGraphFake{id: "fusion", requires: []CModID{"redis", "mongo"}},
		cmodGraphFake{id: "transaction", requires: []CModID{"fusion"}},
	}
	selected := []Service{cmodServiceFake{spec: Spec{
		Name:     "game",
		Requires: []CModID{"rpc", "transaction"},
	}}}

	graph, err := resolveCModGraph(cmods, selected)
	if err != nil {
		t.Fatalf("resolve CMod graph: %v", err)
	}
	want := []CModID{"rpc", "redis", "mongo", "fusion", "transaction"}
	if got := graph.IDs(); !slices.Equal(got, want) {
		t.Fatalf("CMod order = %v, want %v", got, want)
	}
}

func TestResolveCModGraphStartsSharedDependencyOnce(t *testing.T) {
	cmods := []CMod{
		cmodGraphFake{id: "redis"},
		cmodGraphFake{id: "mongo"},
		cmodGraphFake{id: "fusion", requires: []CModID{"redis", "mongo"}},
		cmodGraphFake{id: "transaction", requires: []CModID{"fusion"}},
		cmodGraphFake{id: "scheduler", requires: []CModID{"fusion"}},
	}
	selected := []Service{cmodServiceFake{spec: Spec{
		Name:     "game",
		Requires: []CModID{"transaction", "scheduler"},
	}}}

	graph, err := resolveCModGraph(cmods, selected)
	if err != nil {
		t.Fatalf("resolve CMod graph: %v", err)
	}
	want := []CModID{"redis", "mongo", "fusion", "transaction", "scheduler"}
	if got := graph.IDs(); !slices.Equal(got, want) {
		t.Fatalf("CMod order = %v, want %v", got, want)
	}
}

func TestResolveCModGraphReportsCompleteMissingDependencyPath(t *testing.T) {
	cmods := []CMod{
		cmodGraphFake{id: "transaction", requires: []CModID{"fusion"}},
		cmodGraphFake{id: "fusion", requires: []CModID{"mongo"}},
	}
	selected := []Service{cmodServiceFake{spec: Spec{
		Name:     "game",
		Requires: []CModID{"transaction"},
	}}}

	_, err := resolveCModGraph(cmods, selected)
	if err == nil || !strings.Contains(err.Error(), "game -> transaction -> fusion -> mongo") {
		t.Fatalf("missing dependency error = %v", err)
	}
}

func TestResolveCModGraphReportsCompleteCycle(t *testing.T) {
	cmods := []CMod{
		cmodGraphFake{id: "actor", requires: []CModID{"discovery"}},
		cmodGraphFake{id: "discovery", requires: []CModID{"actor"}},
	}
	selected := []Service{cmodServiceFake{spec: Spec{
		Name:     "game",
		Requires: []CModID{"actor"},
	}}}

	_, err := resolveCModGraph(cmods, selected)
	if err == nil || !strings.Contains(err.Error(), "actor -> discovery -> actor") {
		t.Fatalf("cycle error = %v", err)
	}
}

func TestResolveCModGraphRejectsDuplicateID(t *testing.T) {
	cmods := []CMod{
		cmodGraphFake{id: "rpc"},
		cmodGraphFake{id: "rpc"},
	}
	selected := []Service{cmodServiceFake{spec: Spec{Name: "game", Requires: []CModID{"rpc"}}}}

	_, err := resolveCModGraph(cmods, selected)
	if err == nil || !strings.Contains(err.Error(), "duplicate CMod \"rpc\"") {
		t.Fatalf("duplicate error = %v", err)
	}
}

func TestResolveCModGraphEvaluatesConditionalRequirementsForSelectedServices(t *testing.T) {
	wantErr := errors.New("agent route is invalid")
	agent := &cmodConditionalServiceFake{
		spec: Spec{Name: "agent", Requires: []CModID{"rpc"}},
		err:  wantErr,
	}
	game := cmodServiceFake{spec: Spec{Name: "game", Requires: []CModID{"rpc"}}}
	cmods := []CMod{cmodGraphFake{id: "rpc"}}

	if _, err := resolveCModGraph(cmods, []Service{game}); err != nil {
		t.Fatalf("unselected conditional service affected graph: %v", err)
	}
	if agent.calls != 0 {
		t.Fatalf("unselected conditional calls = %d, want 0", agent.calls)
	}

	_, err := resolveCModGraph(cmods, []Service{game, agent})
	if !errors.Is(err, wantErr) {
		t.Fatalf("selected conditional error = %v, want %v", err, wantErr)
	}
	if agent.calls != 1 {
		t.Fatalf("selected conditional calls = %d, want 1", agent.calls)
	}
}

type cmodGraphFake struct {
	id       CModID
	requires []CModID
}

func (fake cmodGraphFake) Spec() CModSpec {
	return CModSpec{ID: fake.id, Requires: fake.requires}
}

func (cmodGraphFake) Start(CModContext) error { return nil }

type cmodServiceFake struct {
	spec Spec
}

func (fake cmodServiceFake) Spec() Spec     { return fake.spec }
func (cmodServiceFake) Start(Context) error { return nil }

type cmodConditionalServiceFake struct {
	spec  Spec
	err   error
	calls int
}

func (fake *cmodConditionalServiceFake) Spec() Spec     { return fake.spec }
func (*cmodConditionalServiceFake) Start(Context) error { return nil }

func (fake *cmodConditionalServiceFake) RequiredCMods(ServiceSelection) ([]CModID, error) {
	fake.calls++
	return nil, fake.err
}
