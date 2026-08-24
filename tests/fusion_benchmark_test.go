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

func (benchmarkFusionRedisApplier) ApplyChangesWithOptions(_ context.Context, spec meta.TypeSpec, _ storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, _ storage.ApplyOptions) error {
	_, err := txredis.BuildDocumentPatches(spec, source, resolver, values)
	return err
}

type benchmarkFusionMongoLoader struct{}

func (benchmarkFusionMongoLoader) LoadDocument(context.Context, meta.TypeSpec, storage.RecordKey, ...string) (map[string]any, bool, error) {
	return nil, false, nil
}

type benchmarkDelaySubmitter struct{}

func (benchmarkDelaySubmitter) Submit(meta.TypeSpec, storage.RecordKey) {}

var benchmarkFusionErr error

func BenchmarkFusionStrategyApplyChanges(b *testing.B) {
	ctx := context.Background()
	strategy := fusion.Strategy{
		RedisApplier:   benchmarkFusionRedisApplier{},
		MongoLoader:    benchmarkFusionMongoLoader{},
		DelaySubmitter: benchmarkDelaySubmitter{},
	}
	resolver := change.FieldNameResolverFunc(func(field change.FieldID, kind change.FieldNameKind) string {
		if kind == change.FieldNameGo {
			return "Power"
		}
		return "power"
	})
	values := change.ValueResolverFunc(func(change.Path) (any, bool) {
		return int64(7), true
	})
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(470001)}
	baseSpec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelaySave: meta.Duration{Duration: 10 * time.Minute},
		},
	}
	delta := recordedChange{Path: benchmarkFusionPath(change.PathNode{Field: 1}), Op: change.OpSet}
	changes := recordedChanges{delta}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkFusionErr = strategy.ApplyChangesWithOptions(ctx, baseSpec, key, changes, resolver, values, storage.ApplyOptions{})
	}
	if benchmarkFusionErr != nil {
		b.Fatal(benchmarkFusionErr)
	}
}

func benchmarkFusionPath(nodes ...change.PathNode) change.Path {
	var path change.Path
	path.Depth = uint8(len(nodes))
	copy(path.Nodes[:], nodes)
	return path
}
