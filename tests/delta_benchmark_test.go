package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

var benchmarkCompactedDeltaSet change.DeltaSet

func BenchmarkDeltaSetCompact(b *testing.B) {
	cases := []struct {
		name     string
		template change.DeltaSet
	}{
		{
			name:     "RepeatedField64",
			template: benchmarkRepeatedFieldDeltas(64),
		},
		{
			name:     "MapItemPaths256",
			template: benchmarkMapItemDeltas(256),
		},
		{
			name:     "AncestorReplace256",
			template: benchmarkAncestorReplaceDeltas(256),
		},
	}

	for _, benchmarkCase := range cases {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			work := make(change.DeltaSet, len(benchmarkCase.template))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				copy(work, benchmarkCase.template)
				benchmarkCompactedDeltaSet = work.Compact()
			}
		})
	}
}

func benchmarkRepeatedFieldDeltas(count int) change.DeltaSet {
	deltas := make(change.DeltaSet, count)
	path := benchmarkDeltaPath(benchmarkFieldNode(1))
	for i := range deltas {
		deltas[i] = change.DeltaEntry{Path: path, Kind: change.DeltaKindSet}
	}
	return deltas
}

func benchmarkMapItemDeltas(count int) change.DeltaSet {
	deltas := make(change.DeltaSet, count)
	for i := range deltas {
		deltas[i] = change.DeltaEntry{
			Path: benchmarkDeltaPath(
				benchmarkIntKeyNode(2, int64(i)),
				benchmarkFieldNode(3),
			),
			Kind: change.DeltaKindSet,
		}
	}
	return deltas
}

func benchmarkAncestorReplaceDeltas(count int) change.DeltaSet {
	deltas := benchmarkMapItemDeltas(count)
	return append(deltas, change.DeltaEntry{
		Path: benchmarkDeltaPath(benchmarkFieldNode(2)),
		Kind: change.DeltaKindReplace,
	})
}

func benchmarkDeltaPath(nodes ...change.PathNode) change.DeltaPath {
	var path change.DeltaPath
	path.Depth = uint8(len(nodes))
	copy(path.Nodes[:], nodes)
	return path
}

func benchmarkFieldNode(field change.FieldID) change.PathNode {
	return change.PathNode{Field: field}
}

func benchmarkIntKeyNode(field change.FieldID, key int64) change.PathNode {
	return change.PathNode{
		Field: field,
		Key:   change.PathKey{Kind: change.KeyInt, Int: key},
	}
}
