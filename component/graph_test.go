package component

import (
	"slices"
	"strings"
	"testing"
)

func TestResolveReturnsStableDependencyOrder(t *testing.T) {
	components := []Component{
		testComponent{id: "rpc"},
		testComponent{id: "redis"},
		testComponent{id: "mongo"},
		testComponent{id: "fusion", requires: []ID{"redis", "mongo"}},
		testComponent{id: "procedure", requires: []ID{"fusion"}},
	}

	graph, err := Resolve(components, []Root{
		{Source: "game", ID: "rpc"},
		{Source: "game", ID: "procedure"},
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	want := []ID{"rpc", "redis", "mongo", "fusion", "procedure"}
	if got := graph.IDs(); !slices.Equal(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}

func TestResolveReportsSourceAndCompleteMissingDependencyPath(t *testing.T) {
	components := []Component{
		testComponent{id: "procedure", requires: []ID{"fusion"}},
		testComponent{id: "fusion", requires: []ID{"mongo"}},
	}

	_, err := Resolve(components, []Root{{Source: "game", ID: "procedure"}})
	if err == nil || !strings.Contains(err.Error(), "game -> procedure -> fusion -> mongo") {
		t.Fatalf("missing dependency error = %v", err)
	}
}

func TestResolveReportsCompleteCycle(t *testing.T) {
	components := []Component{
		testComponent{id: "actor", requires: []ID{"discovery"}},
		testComponent{id: "discovery", requires: []ID{"actor"}},
	}

	_, err := Resolve(components, []Root{{Source: "game", ID: "actor"}})
	if err == nil || !strings.Contains(err.Error(), "actor -> discovery -> actor") {
		t.Fatalf("cycle error = %v", err)
	}
}

func TestResolveAfterOrdersOnlySelectedComponent(t *testing.T) {
	components := []Component{
		testComponent{id: "mongo"},
		testComponent{id: "wal", after: []ID{"mongo"}},
	}

	withoutMongo, err := Resolve(components, []Root{{Source: "world", ID: "wal"}})
	if err != nil {
		t.Fatalf("Resolve WAL only: %v", err)
	}
	if want := []ID{"wal"}; !slices.Equal(withoutMongo.IDs(), want) {
		t.Fatalf("WAL-only IDs = %v, want %v", withoutMongo.IDs(), want)
	}

	withMongo, err := Resolve(components, []Root{
		{Source: "world", ID: "wal"},
		{Source: "world", ID: "mongo"},
	})
	if err != nil {
		t.Fatalf("Resolve WAL and Mongo: %v", err)
	}
	if want := []ID{"mongo", "wal"}; !slices.Equal(withMongo.IDs(), want) {
		t.Fatalf("WAL and Mongo IDs = %v, want %v", withMongo.IDs(), want)
	}
}

func TestResolveRejectsDuplicateAndTypedNilComponents(t *testing.T) {
	if _, err := Resolve([]Component{
		testComponent{id: "rpc"},
		testComponent{id: "rpc"},
	}, []Root{{Source: "game", ID: "rpc"}}); err == nil || !strings.Contains(err.Error(), "duplicate component") {
		t.Fatalf("duplicate error = %v", err)
	}

	var typedNil *testPointerComponent
	if _, err := Resolve([]Component{typedNil}, nil); err == nil || !strings.Contains(err.Error(), "component is nil") {
		t.Fatalf("typed nil error = %v", err)
	}
}

func TestResolveOwnsGraphMetadata(t *testing.T) {
	requires := []ID{"redis"}
	components := []Component{
		testComponent{id: "redis"},
		testComponent{id: "fusion", requires: requires},
	}
	graph, err := Resolve(components, []Root{{Source: "game", ID: "fusion"}})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}

	requires[0] = "mongo"
	components[0] = testComponent{id: "other"}
	if want := []ID{"redis", "fusion"}; !slices.Equal(graph.IDs(), want) {
		t.Fatalf("IDs after caller mutation = %v, want %v", graph.IDs(), want)
	}
}

type testComponent struct {
	id       ID
	requires []ID
	after    []ID
}

func (component testComponent) Spec() Spec {
	return Spec{ID: component.id, Requires: component.requires, After: component.after}
}

func (testComponent) Start(Context) error { return nil }

type testPointerComponent struct{}

func (*testPointerComponent) Spec() Spec          { return Spec{ID: "pointer"} }
func (*testPointerComponent) Start(Context) error { return nil }
