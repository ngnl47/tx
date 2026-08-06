package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

const (
	deltaFieldName  change.FieldID = 1
	deltaFieldItems change.FieldID = 2
	deltaFieldQty   change.FieldID = 3
)

func deltaPath(nodes ...change.PathNode) change.DeltaPath {
	var path change.DeltaPath
	path.Depth = uint8(len(nodes))
	copy(path.Nodes[:], nodes)
	return path
}

func fieldNode(field change.FieldID) change.PathNode {
	return change.PathNode{Field: field}
}

func intKeyNode(field change.FieldID, key int64) change.PathNode {
	return change.PathNode{
		Field: field,
		Key:   change.PathKey{Kind: change.KeyInt, Int: key},
	}
}

func TestDeltaSetCompactKeepsLatestSamePath(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaDelete},
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
	}

	compacted := deltas.Compact()

	if len(compacted) != 1 {
		t.Fatalf("compacted len = %d, want 1: %#v", len(compacted), compacted)
	}
	if compacted[0].Kind != change.DeltaKindSet {
		t.Fatalf("remaining kind = %v, want DeltaKindSet", compacted[0].Kind)
	}
}

func TestDeltaSetCompactLaterAncestorRemovesEarlierChild(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind: change.DeltaKindSet,
		},
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001)),
			Kind: change.DeltaDelete,
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 1 {
		t.Fatalf("compacted len = %d, want 1: %#v", len(compacted), compacted)
	}
	if compacted[0].Kind != change.DeltaDelete {
		t.Fatalf("remaining kind = %v, want DeltaDelete", compacted[0].Kind)
	}
	if !compacted[0].Path.Equal(deltaPath(intKeyNode(deltaFieldItems, 1001))) {
		t.Fatalf("remaining path = %#v, want Items[1001]", compacted[0].Path)
	}
}

func TestDeltaSetCompactKeepsLaterChildAfterEarlierAncestor(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(fieldNode(deltaFieldItems)),
			Kind: change.DeltaKindSet,
		},
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind: change.DeltaKindSet,
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 2 {
		t.Fatalf("compacted len = %d, want 2: %#v", len(compacted), compacted)
	}
	if !compacted[0].Path.Equal(deltaPath(fieldNode(deltaFieldItems))) {
		t.Fatalf("first path = %#v, want Items ancestor", compacted[0].Path)
	}
	if !compacted[1].Path.Equal(deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty))) {
		t.Fatalf("second path = %#v, want Items[1001].Quantity child", compacted[1].Path)
	}
}

func TestDeltaSetCompactKeepsSameDepthWildcardAndKeyedPaths(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind: change.DeltaKindSet,
		},
		{
			Path: deltaPath(fieldNode(deltaFieldItems), fieldNode(deltaFieldQty)),
			Kind: change.DeltaKindSet,
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 2 {
		t.Fatalf("compacted len = %d, want 2 because same-depth wildcard path is not an ancestor: %#v", len(compacted), compacted)
	}
}

func TestDeltaSetCompactLaterWildcardAncestorRemovesKeyedChild(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind: change.DeltaKindSet,
		},
		{
			Path: deltaPath(fieldNode(deltaFieldItems)),
			Kind: change.DeltaKindReplace,
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 1 || compacted[0].Kind != change.DeltaKindReplace {
		t.Fatalf("compacted = %#v, want later Items replacement only", compacted)
	}
}

func TestDeltaSetCompactKeepsSetAfterDeleteOnSamePath(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001)),
			Kind: change.DeltaDelete,
		},
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001)),
			Kind: change.DeltaKindSet,
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 1 {
		t.Fatalf("compacted len = %d, want 1: %#v", len(compacted), compacted)
	}
	if compacted[0].Kind != change.DeltaKindSet {
		t.Fatalf("remaining kind = %v, want DeltaKindSet", compacted[0].Kind)
	}
}

func TestDeltaSetCompactClearRemovesEarlierChildren(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind: change.DeltaKindSet,
		},
		{
			Path: deltaPath(fieldNode(deltaFieldItems)),
			Kind: change.DeltaClear,
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 1 {
		t.Fatalf("compacted len = %d, want 1: %#v", len(compacted), compacted)
	}
	if compacted[0].Kind != change.DeltaClear {
		t.Fatalf("remaining kind = %v, want DeltaClear", compacted[0].Kind)
	}
	if !compacted[0].Path.Equal(deltaPath(fieldNode(deltaFieldItems))) {
		t.Fatalf("remaining path = %#v, want Items", compacted[0].Path)
	}
}

func TestDeltaSetCompactDoesNotMergeChildAfterClear(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(fieldNode(deltaFieldItems)),
			Kind: change.DeltaClear,
		},
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001)),
			Kind: change.DeltaKindSet,
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 2 {
		t.Fatalf("compacted len = %d, want 2 because generated Store owns clear-plus-set upgrade: %#v", len(compacted), compacted)
	}
}

func TestDeltaSetRemoveFieldRemovesFieldAndChildren(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
		{Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)), Kind: change.DeltaKindSet},
		{Path: deltaPath(fieldNode(deltaFieldQty)), Kind: change.DeltaKindSet},
	}

	deltas = deltas.RemoveField(deltaFieldItems)

	if len(deltas) != 2 {
		t.Fatalf("deltas len = %d, want 2: %#v", len(deltas), deltas)
	}
	for _, delta := range deltas {
		if delta.Path.Depth > 0 && delta.Path.Nodes[0].Field == deltaFieldItems {
			t.Fatalf("items delta was not removed: %#v", deltas)
		}
	}
}

func TestDeltaSetCloneDoesNotShareSliceHeader(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
	}

	cloned := deltas.Clone()
	cloned[0].Kind = change.DeltaDelete

	if deltas[0].Kind != change.DeltaKindSet {
		t.Fatalf("source kind = %v, want DeltaKindSet", deltas[0].Kind)
	}
}

func TestDeltaPathJoinAppendsChildPath(t *testing.T) {
	parent := deltaPath(intKeyNode(deltaFieldItems, 1001))
	child := deltaPath(fieldNode(deltaFieldQty))

	joined, ok := change.JoinDeltaPath(parent, child)

	if !ok {
		t.Fatal("JoinDeltaPath returned ok=false, want true")
	}
	if !joined.Equal(deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty))) {
		t.Fatalf("joined path = %#v, want Items[1001].Quantity", joined)
	}
}

func TestDeltaPathJoinReportsOverflow(t *testing.T) {
	parent := deltaPath(fieldNode(1), fieldNode(2), fieldNode(3))
	child := deltaPath(fieldNode(4), fieldNode(5))

	if _, ok := change.JoinDeltaPath(parent, child); ok {
		t.Fatal("JoinDeltaPath returned ok=true, want overflow")
	}
}

func TestDeltaPathJoinLimitedKeepsNearestAncestor(t *testing.T) {
	parent := deltaPath(fieldNode(1), fieldNode(2))
	child := deltaPath(fieldNode(3), fieldNode(4), fieldNode(5))

	joined, complete := change.JoinDeltaPathLimited(parent, child)

	if complete {
		t.Fatal("JoinDeltaPathLimited complete = true, want overflow")
	}
	want := deltaPath(fieldNode(1), fieldNode(2), fieldNode(3), fieldNode(4))
	if !joined.Equal(want) {
		t.Fatalf("limited path = %#v, want %#v", joined, want)
	}
}
