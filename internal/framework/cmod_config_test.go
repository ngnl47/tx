package framework

import (
	"context"
	"slices"
	"testing"
)

func TestCModConfigProviderReceivesResolvedDependencyClosure(t *testing.T) {
	const (
		root CModID = "test-root"
		leaf CModID = "test-leaf"
	)
	var selected AssemblySelection
	app, err := New(
		Config{AppID: "cmod-config-test"},
		WithCMod(
			&builtInCMod{spec: CModSpec{ID: leaf}},
			&builtInCMod{spec: CModSpec{ID: root, Requires: []CModID{leaf}}},
		),
		WithCModConfigProvider(func(selection AssemblySelection) (Config, error) {
			selected = selection
			return Config{AppID: "cmod-config-test"}, nil
		}),
		WithService(testService{spec: Spec{Name: "test", Requires: []CModID{root}}}),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if selected.CModIDs() != nil || selected.ServiceNames() != nil {
		t.Fatalf("provider ran before Start: %#v", selected)
	}
	if err := app.Start(context.Background(), RunOptions{Services: []string{"test"}}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	if want := []CModID{leaf, root}; !slices.Equal(selected.CModIDs(), want) {
		t.Fatalf("selected CMods = %v, want %v", selected.CModIDs(), want)
	}
	if want := []string{"test"}; !slices.Equal(selected.ServiceNames(), want) {
		t.Fatalf("selected services = %v, want %v", selected.ServiceNames(), want)
	}
}

func TestCModSelectionOwnsItsIDList(t *testing.T) {
	ids := []CModID{"redis-storage", "rpc"}
	selection := newCModSelection(ids...)
	ids[0] = "mutated"
	got := selection.IDs()
	got[1] = "mutated"
	if !selection.Has("redis-storage") || !selection.Has("rpc") {
		t.Fatalf("selection was mutated: %v", selection.IDs())
	}
}
