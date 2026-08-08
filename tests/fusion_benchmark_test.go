package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	txredis "github.com/ngnl47/tx/storage/redis"
)

type benchmarkFusionRedisApplier struct{}

func (benchmarkFusionRedisApplier) ApplyDeltasWithOptions(_ context.Context, spec meta.TypeSpec, _ storage.RecordKey, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver, _ storage.ApplyOptions) error {
	_, err := txredis.BuildDocumentPatchesWithDeltas(spec, deltas, resolver, values)
	return err
}

type benchmarkFusionMongoLoader struct{}

func (benchmarkFusionMongoLoader) LoadDocument(context.Context, meta.TypeSpec, storage.RecordKey, ...string) (map[string]any, bool, error) {
	return nil, false, nil
}

type benchmarkFusionQueue struct{}

func (benchmarkFusionQueue) Enqueue(context.Context, fusion.DelayedOperation) error {
	return nil
}

var benchmarkFusionErr error

func BenchmarkFusionStrategyApplyDeltas(b *testing.B) {
	ctx := context.Background()
	strategy := fusion.Strategy{
		Redis:       benchmarkFusionRedisApplier{},
		MongoLoader: benchmarkFusionMongoLoader{},
		MongoQueue:  benchmarkFusionQueue{},
	}
	resolver := change.FieldNameResolverFunc(func(field change.FieldID, backend change.NameBackend) string {
		if backend == change.NameBackendGo {
			return "Power"
		}
		return "power"
	})
	values := change.DeltaValueResolverFunc(func(change.DeltaPath) (any, bool) {
		return int64(7), true
	})
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(470001)}
	baseSpec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelaySave: meta.Duration{Duration: 10 * time.Minute},
		},
	}
	delta := change.DeltaEntry{Path: benchmarkFusionDeltaPath(change.PathNode{Field: 1}), Kind: change.DeltaKindSet}
	deltas := change.DeltaSet{delta}

	for _, benchmarkCase := range []struct {
		name string
		spec meta.TypeSpec
	}{
		{name: "SaveOnly", spec: baseSpec},
		{
			name: "SaveAndDelayedDelete",
			spec: meta.TypeSpec{
				Type: baseSpec.Type,
				Storage: meta.StorageSpec{
					DelaySave:   baseSpec.Storage.DelaySave,
					DelayDelete: meta.Duration{Duration: 24 * time.Hour},
				},
			},
		},
	} {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkFusionErr = strategy.ApplyDeltasWithOptions(ctx, benchmarkCase.spec, key, deltas, resolver, values, storage.ApplyOptions{})
			}
			if benchmarkFusionErr != nil {
				b.Fatal(benchmarkFusionErr)
			}
		})
	}
}

func benchmarkFusionDeltaPath(nodes ...change.PathNode) change.DeltaPath {
	var path change.DeltaPath
	path.Depth = uint8(len(nodes))
	copy(path.Nodes[:], nodes)
	return path
}
