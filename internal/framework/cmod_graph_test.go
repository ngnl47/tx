package framework

import (
	"errors"
	"slices"
	"strings"
	"testing"

	protoactor "github.com/asynkron/protoactor-go/actor"
	txactor "github.com/ngnl47/tx/modules/actor"
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

func TestResolveCModGraphOrdersSelectedOptionalPredecessorWithoutSelectingIt(t *testing.T) {
	cmods := []CMod{
		cmodGraphFake{id: "mongo"},
		cmodGraphFake{id: "wal", after: []CModID{"mongo"}},
	}

	withoutMongo, err := resolveCModGraph(cmods, []Service{cmodServiceFake{spec: Spec{
		Name:     "wal-only",
		Requires: []CModID{"wal"},
	}}})
	if err != nil {
		t.Fatalf("resolve WAL-only CMod graph: %v", err)
	}
	if want := []CModID{"wal"}; !slices.Equal(withoutMongo.IDs(), want) {
		t.Fatalf("WAL-only CMod order = %v, want %v", withoutMongo.IDs(), want)
	}

	withMongo, err := resolveCModGraph(cmods, []Service{cmodServiceFake{spec: Spec{
		Name:     "world",
		Requires: []CModID{"wal", "mongo"},
	}}})
	if err != nil {
		t.Fatalf("resolve world CMod graph: %v", err)
	}
	if want := []CModID{"mongo", "wal"}; !slices.Equal(withMongo.IDs(), want) {
		t.Fatalf("world CMod order = %v, want %v", withMongo.IDs(), want)
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

func TestResolveCModGraphRejectsTypedNilCMod(t *testing.T) {
	var mod *cmodLifecycleFake
	_, err := resolveCModGraph([]CMod{mod}, []Service{cmodServiceFake{spec: Spec{Name: "game"}}})
	if err == nil || !strings.Contains(err.Error(), "CMod is nil") {
		t.Fatalf("resolveCModGraph error = %v, want typed nil CMod error", err)
	}
}

func TestResolveCModGraphRejectsInvalidComponentTopology(t *testing.T) {
	cmods := []CMod{
		cmodGraphFake{
			id: "procedure",
			components: []ComponentSpec{
				{Role: "cache", Name: "first"},
				{Role: "cache", Name: "second"},
			},
		},
	}
	selected := []Service{cmodServiceFake{spec: Spec{
		Name:     "slggame",
		Requires: []CModID{"procedure"},
	}}}

	_, err := resolveCModGraph(cmods, selected)
	if err == nil || !strings.Contains(err.Error(), "duplicate component role") {
		t.Fatalf("component topology error = %v", err)
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

func TestResolveCModGraphAddsActorForDeclaredKinds(t *testing.T) {
	roomKind := txactor.MustKind("room", func() protoactor.Actor { return cmodActorFake{} })
	selected := []Service{cmodServiceFake{spec: Spec{Name: "room", ActorKinds: []txactor.Kind{roomKind}}}}
	cmods := []CMod{
		cmodGraphFake{id: CModActor},
	}

	graph, err := resolveCModGraph(cmods, selected)
	if err != nil {
		t.Fatalf("resolve CMod graph: %v", err)
	}
	if want := []CModID{CModActor}; !slices.Equal(graph.IDs(), want) {
		t.Fatalf("CMod order = %v, want %v", graph.IDs(), want)
	}
	kinds, err := collectActorKinds(selected)
	if err != nil {
		t.Fatalf("collect Actor kinds: %v", err)
	}
	if len(kinds) != 1 || kinds["room"].kind.Name() != "room" {
		t.Fatalf("Actor kinds = %#v", kinds)
	}
}

func TestResolveCModGraphAddsActorForFixedKinds(t *testing.T) {
	roleKind := txactor.MustFixedKind("role", func() protoactor.Actor { return cmodActorFake{} })
	selected := []Service{cmodServiceFake{spec: Spec{Name: "slggame", ActorKinds: []txactor.Kind{roleKind}}}}
	cmods := []CMod{
		cmodGraphFake{id: CModActor},
		cmodGraphFake{id: CModDiscovery, after: []CModID{CModActor}},
	}

	graph, err := resolveCModGraph(cmods, selected)
	if err != nil {
		t.Fatalf("resolve CMod graph: %v", err)
	}
	if want := []CModID{CModActor, CModDiscovery}; !slices.Equal(graph.IDs(), want) {
		t.Fatalf("CMod order = %v, want %v", graph.IDs(), want)
	}
	kinds, err := collectActorKinds(selected)
	if err != nil {
		t.Fatalf("collect Actor kinds: %v", err)
	}
	if len(kinds) != 1 || kinds["role"].kind.Name() != "role" {
		t.Fatalf("Actor kinds = %#v", kinds)
	}
}

func TestCollectActorKindsRejectsDuplicateNames(t *testing.T) {
	kind := txactor.MustKind("room", func() protoactor.Actor { return cmodActorFake{} })
	services := []Service{
		cmodServiceFake{spec: Spec{Name: "room-a", ActorKinds: []txactor.Kind{kind}}},
		cmodServiceFake{spec: Spec{Name: "room-b", ActorKinds: []txactor.Kind{kind}}},
	}
	if _, err := collectActorKinds(services); err == nil || !strings.Contains(err.Error(), "duplicate Actor kind") {
		t.Fatalf("collect Actor kinds error = %v", err)
	}
}

type cmodGraphFake struct {
	id         CModID
	requires   []CModID
	after      []CModID
	components []ComponentSpec
}

func (fake cmodGraphFake) Spec() CModSpec {
	return CModSpec{ID: fake.id, Requires: fake.requires, After: fake.after, Components: fake.components}
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

type cmodActorFake struct{}

func (cmodActorFake) Receive(protoactor.Context) {}
