package change

import "testing"

func viewPath(nodes ...PathNode) Path {
	var path Path
	path.Depth = uint8(len(nodes))
	copy(path.Nodes[:], nodes)
	return path
}

func TestViewStatePreservesSelectedZeroValueMetadata(t *testing.T) {
	name := viewPath(PathNode{Field: 1})
	var state ViewState
	if err := state.Record(name, OpSet); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if !state.HasPath(name) {
		t.Fatal("HasPath(name) = false, want true")
	}
	if !state.HasField(1) {
		t.Fatal("HasField(1) = false, want true")
	}
	if state.HasField(2) {
		t.Fatal("HasField(2) = true, want false")
	}
}

func TestViewStateCanonicalizesUnusedPathStorage(t *testing.T) {
	dirty := viewPath(PathNode{
		Field: 1,
		Key:   PathKey{Kind: KeyNone, Int: 99, Uint: 100, Str: "ignored"},
	})
	dirty.Nodes[3] = PathNode{Field: 4, Key: PathKey{Kind: KeyString, Str: "ignored"}}
	clean := viewPath(PathNode{Field: 1})

	var state ViewState
	if err := state.Record(dirty, OpSet); err != nil {
		t.Fatalf("Record(dirty) error = %v", err)
	}
	if !state.HasPath(clean) {
		t.Fatal("HasPath(clean) = false, want true")
	}
	if err := state.Record(clean, OpReplace); err != nil {
		t.Fatalf("Record(clean) error = %v", err)
	}
	assertViewChanges(t, state, []viewChange{{path: clean, op: OpReplace}})
}

func TestViewStateMergeLaterAncestorRemovesEarlierChildren(t *testing.T) {
	itemsQuantity := viewPath(
		PathNode{Field: 1, Key: PathKey{Kind: KeyInt, Int: 1001}},
		PathNode{Field: 2},
	)
	items := viewPath(PathNode{Field: 1})
	var state ViewState
	if err := state.Record(itemsQuantity, OpSet); err != nil {
		t.Fatalf("Record(child) error = %v", err)
	}
	var next ViewState
	if err := next.Record(items, OpReplace); err != nil {
		t.Fatalf("Record(parent) error = %v", err)
	}
	if err := state.Merge(next); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	assertViewChanges(t, state, []viewChange{{path: items, op: OpReplace}})
}

func TestViewStateMergeChildAfterClearPromotesAncestorReplace(t *testing.T) {
	items := viewPath(PathNode{Field: 1})
	itemsQuantity := viewPath(
		PathNode{Field: 1, Key: PathKey{Kind: KeyInt, Int: 1001}},
		PathNode{Field: 2},
	)
	var state ViewState
	if err := state.Record(items, OpClear); err != nil {
		t.Fatalf("Record(clear) error = %v", err)
	}
	var next ViewState
	if err := next.Record(itemsQuantity, OpSet); err != nil {
		t.Fatalf("Record(child) error = %v", err)
	}
	if err := state.Merge(next); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	assertViewChanges(t, state, []viewChange{{path: items, op: OpReplace}})
}

func TestViewStateMergeDisjointPathsRetainsBoth(t *testing.T) {
	name := viewPath(PathNode{Field: 1})
	power := viewPath(PathNode{Field: 2})
	var state ViewState
	if err := state.Record(name, OpSet); err != nil {
		t.Fatalf("Record(name) error = %v", err)
	}
	var next ViewState
	if err := next.Record(power, OpSet); err != nil {
		t.Fatalf("Record(power) error = %v", err)
	}
	if err := state.Merge(next); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	assertViewChanges(t, state, []viewChange{{path: name, op: OpSet}, {path: power, op: OpSet}})
}

type viewChange struct {
	path Path
	op   Op
}

func assertViewChanges(t *testing.T, state ViewState, want []viewChange) {
	t.Helper()
	got := make([]viewChange, 0, len(want))
	if err := state.VisitChanges(VisitorFunc(func(path Path, op Op) error {
		got = append(got, viewChange{path: path, op: op})
		return nil
	})); err != nil {
		t.Fatalf("VisitChanges() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("VisitChanges() count = %d, want %d; got=%#v", len(got), len(want), got)
	}
	for i := range want {
		if !got[i].path.Equal(want[i].path) || got[i].op != want[i].op {
			t.Fatalf("change[%d] = %#v/%v, want %#v/%v", i, got[i].path, got[i].op, want[i].path, want[i].op)
		}
	}
}

func BenchmarkViewStateRecordDistinct1024(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		var state ViewState
		for i := int64(0); i < 1024; i++ {
			path := viewPath(PathNode{Field: 1, Key: PathKey{Kind: KeyInt, Int: i}})
			if err := state.Record(path, OpReplace); err != nil {
				b.Fatal(err)
			}
		}
	}
}
