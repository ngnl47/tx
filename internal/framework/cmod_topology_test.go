package framework

import (
	"reflect"
	"testing"
)

func TestCModTopologyShowsOnlyActiveComponents(t *testing.T) {
	mods := []CMod{
		cmodTopologyFake{spec: CModSpec{
			ID: CModFusionStorage,
			Components: []ComponentSpec{
				{Role: "scheduler", Name: "delay"},
			},
		}},
		cmodTopologyFake{spec: CModSpec{
			ID: CModProcedure,
			Components: []ComponentSpec{
				{Role: "transaction", Name: "default"},
				{
					Role: "cache",
					Name: "txgame",
					Components: []ComponentSpec{
						{Role: "wal", Name: "mongo", Optional: true, Services: []string{"slgworld"}},
					},
				},
			},
		}},
	}

	slggame := ServiceSelection{names: map[string]struct{}{"slggame": {}}}
	if got, want := cmodTopology(mods, slggame), []string{
		"fusion-storage(scheduler=delay)",
		"procedure(transaction=default,cache=txgame)",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slggame topology = %v, want %v", got, want)
	}

	slgworld := ServiceSelection{names: map[string]struct{}{"slgworld": {}}}
	if got, want := cmodTopology(mods, slgworld), []string{
		"fusion-storage(scheduler=delay)",
		"procedure(transaction=default,cache=txgame[wal=mongo])",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slgworld topology = %v, want %v", got, want)
	}
}

type cmodTopologyFake struct {
	spec CModSpec
}

func (fake cmodTopologyFake) Spec() CModSpec     { return fake.spec }
func (cmodTopologyFake) Start(CModContext) error { return nil }
