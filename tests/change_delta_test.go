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
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet, Value: "alice"},
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet, Value: "bob"},
	}

	compacted := deltas.Compact()

	if len(compacted) != 1 {
		t.Fatalf("compacted len = %d, want 1: %#v", len(compacted), compacted)
	}
	if compacted[0].Value != "bob" {
		t.Fatalf("remaining value = %#v, want bob", compacted[0].Value)
	}
}

func TestDeltaSetCompactLaterAncestorRemovesEarlierChild(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path:  deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind:  change.DeltaKindSet,
			Value: int64(7),
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
			Path:  deltaPath(fieldNode(deltaFieldItems)),
			Kind:  change.DeltaKindSet,
			Value: map[int64]any{1001: map[string]any{"quantity": int64(7)}},
		},
		{
			Path:  deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind:  change.DeltaKindSet,
			Value: int64(8),
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

func TestDeltaSetCompactKeepsSetAfterDeleteOnSamePath(t *testing.T) {
	deltas := change.DeltaSet{
		{
			Path: deltaPath(intKeyNode(deltaFieldItems, 1001)),
			Kind: change.DeltaDelete,
		},
		{
			Path:  deltaPath(intKeyNode(deltaFieldItems, 1001)),
			Kind:  change.DeltaKindSet,
			Value: map[string]any{"quantity": int64(7)},
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
			Path:  deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)),
			Kind:  change.DeltaKindSet,
			Value: int64(7),
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
			Path:  deltaPath(intKeyNode(deltaFieldItems, 1001)),
			Kind:  change.DeltaKindSet,
			Value: map[string]any{"quantity": int64(7)},
		},
	}

	compacted := deltas.Compact()

	if len(compacted) != 2 {
		t.Fatalf("compacted len = %d, want 2 because generated Store owns clear-plus-set upgrade: %#v", len(compacted), compacted)
	}
}
