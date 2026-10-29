package entitycache

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCacheHit(b *testing.B) {
	b.Run("IdleDisabled", func(b *testing.B) {
		cache := benchmarkCache(b, 0)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if exists, err := cache.Use(context.Background(), 47, func(*cachedValue) error { return nil }); err != nil || !exists {
				b.Fatalf("Load returned exists=%v error=%v", exists, err)
			}
		}
	})
	b.Run("IdleEnabled", func(b *testing.B) {
		cache := benchmarkCache(b, time.Minute)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if exists, err := cache.Use(context.Background(), 47, func(*cachedValue) error { return nil }); err != nil || !exists {
				b.Fatalf("Load returned exists=%v error=%v", exists, err)
			}
		}
	})
}

func BenchmarkCacheParallelHit(b *testing.B) {
	cache := benchmarkCache(b, 0)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if exists, err := cache.Use(ctx, 47, func(*cachedValue) error { return nil }); err != nil || !exists {
				b.Fatalf("Load returned exists=%v error=%v", exists, err)
			}
		}
	})
}

func benchmarkCache(b *testing.B, idleTimeout time.Duration) *Cache[int64, *cachedValue] {
	b.Helper()
	cache, err := NewCache(
		func(value int64) string { return strconv.FormatInt(value, 10) },
		func(context.Context, int64) (*cachedValue, bool, error) {
			return &cachedValue{id: 47}, true, nil
		},
		Options[*cachedValue]{
			MaximumSize: 16,
			IdleTimeout: idleTimeout,
			TryRelease:  func(context.Context, *cachedValue) (bool, error) { return true, nil },
		},
	)
	if err != nil {
		b.Fatalf("NewCache returned error: %v", err)
	}
	if exists, err := cache.Use(context.Background(), 47, func(*cachedValue) error { return nil }); err != nil || !exists {
		b.Fatalf("prewarm Load returned exists=%v error=%v", exists, err)
	}
	b.Cleanup(func() {
		cache.policy.StopAllGoroutines()
	})
	return cache
}

// Find 包含使用期登记，和原 Use 的短回调基线分别统计。
func BenchmarkScopedFindHit(b *testing.B) {
	cache := benchmarkCache(b, 0)
	b.ReportAllocs()
	for b.Loop() {
		ctx, end := Begin(context.Background())
		if _, found, err := cache.Find(ctx, 47); err != nil || !found {
			b.Fatal(err)
		}
		end()
	}
}
