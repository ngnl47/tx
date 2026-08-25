package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	redisclient "github.com/redis/go-redis/v9"
)

func BenchmarkRedisDocumentBackfill(b *testing.B) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		b.Skip("set TX_REDIS_ENDPOINT to run Redis document backfill benchmark")
	}
	ctx := context.Background()
	raw := redisclient.NewClient(&redisclient.Options{Addr: endpoint})
	b.Cleanup(func() { _ = raw.Close() })
	if err := raw.Ping(ctx).Err(); err != nil {
		b.Fatalf("ping redis %q: %v", endpoint, err)
	}
	adapter, err := txredis.NewAdapter(txgoredis.NewClient(raw))
	if err != nil {
		b.Fatalf("NewAdapter returned error: %v", err)
	}
	doc := map[string]any{
		"roleId": int64(470001),
		"name":   "alice",
		"power":  int64(9001),
		"items": map[string]any{
			"1001": map[string]any{"itemId": int64(1001), "quantity": int64(9)},
			"1002": map[string]any{"itemId": int64(1002), "quantity": int64(3)},
		},
	}
	options := storage.ApplyOptions{TTL: 10 * time.Second}

	b.Run("replace-missing", func(b *testing.B) {
		spec := benchmarkRedisBackfillSpec(fmt.Sprintf("tx:bench:replace:%d", time.Now().UnixNano()))
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			if err := adapter.ReplaceDocumentWithOptions(ctx, spec, storage.RecordKey{ID: int64(i + 1)}, doc, options); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("conditional-missing", func(b *testing.B) {
		spec := benchmarkRedisBackfillSpec(fmt.Sprintf("tx:bench:backfill:%d", time.Now().UnixNano()))
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			inserted, err := adapter.BackfillDocumentWithOptions(ctx, spec, storage.RecordKey{ID: int64(i + 1)}, doc, options)
			if err != nil {
				b.Fatal(err)
			}
			if !inserted {
				b.Fatal("BackfillDocumentWithOptions inserted = false, want true for unique key")
			}
		}
	})
}

func benchmarkRedisBackfillSpec(key string) meta.TypeSpec {
	return meta.TypeSpec{
		Type:  meta.Type{Fields: []meta.Field{{Name: "Items", JSONName: "items"}}},
		Redis: &meta.RedisSpec{Key: key},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
}
