package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

func TestPathJoinAppendsChildPath(t *testing.T) {
	parent := changePath(intKeyNode(changeFieldItems, 1001))
	child := changePath(fieldNode(changeFieldQty))

	joined, ok := change.JoinPath(parent, child)
	if !ok {
		t.Fatal("JoinPath returned ok=false, want true")
	}
	want := changePath(intKeyNode(changeFieldItems, 1001), fieldNode(changeFieldQty))
	if !joined.Equal(want) {
		t.Fatalf("joined path = %#v, want %#v", joined, want)
	}
}

func TestPathJoinReportsOverflow(t *testing.T) {
	parent := changePath(fieldNode(1), fieldNode(2), fieldNode(3))
	child := changePath(fieldNode(4), fieldNode(5))

	if _, ok := change.JoinPath(parent, child); ok {
		t.Fatal("JoinPath returned ok=true, want overflow")
	}
}

func TestPathJoinLimitedKeepsNearestAncestor(t *testing.T) {
	parent := changePath(fieldNode(1), fieldNode(2))
	child := changePath(fieldNode(3), fieldNode(4), fieldNode(5))

	joined, complete := change.JoinPathLimited(parent, child)
	if complete {
		t.Fatal("JoinPathLimited complete = true, want overflow")
	}
	want := changePath(fieldNode(1), fieldNode(2), fieldNode(3), fieldNode(4))
	if !joined.Equal(want) {
		t.Fatalf("limited path = %#v, want %#v", joined, want)
	}
}

func TestPathRejectsDepthBeyondFixedCapacity(t *testing.T) {
	path := change.Path{Depth: change.MaxPathDepth + 1}
	resolver := testFieldNameResolver{change.FieldNameGo: {1: "Name"}}

	if path.Valid() {
		t.Fatal("Path.Valid() = true, want false")
	}
	if got := path.FullPath(resolver, change.FieldNameGo); got != nil {
		t.Fatalf("FullPath() = %q, want nil", got)
	}
	if path.Equal(path) {
		t.Fatal("invalid paths compare equal")
	}
}
