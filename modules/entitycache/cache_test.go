package entitycache

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cachedValue struct {
	id int64
}

func TestNewCacheRejectsInvalidOptions(t *testing.T) {
	keyString := func(value int64) string { return strconv.FormatInt(value, 10) }
	loader := func(context.Context, int64) (*cachedValue, bool, error) {
		return &cachedValue{id: 47}, true, nil
	}
	validOptions := Options[*cachedValue]{
		MaximumSize: 16,
		TryRelease:  func(context.Context, *cachedValue) (bool, error) { return true, nil },
	}

	tests := []struct {
		name      string
		keyString KeyString[int64]
		loader    Loader[int64, *cachedValue]
		options   Options[*cachedValue]
	}{
		{name: "nil key formatter", loader: loader, options: validOptions},
		{name: "nil loader", keyString: keyString, options: validOptions},
		{
			name:      "zero maximum size",
			keyString: keyString,
			loader:    loader,
			options: Options[*cachedValue]{
				TryRelease: validOptions.TryRelease,
			},
		},
		{
			name:      "negative maximum size",
			keyString: keyString,
			loader:    loader,
			options: Options[*cachedValue]{
				MaximumSize: -1,
				TryRelease:  validOptions.TryRelease,
			},
		},
		{
			name:      "negative idle timeout",
			keyString: keyString,
			loader:    loader,
			options: Options[*cachedValue]{
				MaximumSize: 16,
				IdleTimeout: -time.Second,
				TryRelease:  validOptions.TryRelease,
			},
		},
		{
			name:      "nil try release",
			keyString: keyString,
			loader:    loader,
			options: Options[*cachedValue]{
				MaximumSize: 16,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache, err := NewCache(test.keyString, test.loader, test.options)
			if err == nil {
				cache.policy.StopAllGoroutines()
				t.Fatal("NewCache returned nil error")
			}
		})
	}
}

func TestNewCacheAcceptsDisabledIdleTimeout(t *testing.T) {
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		return &cachedValue{id: 47}, true, nil
	})
	if cache == nil {
		t.Fatal("NewCache returned nil cache")
	}
}

func TestCachePolicyTokenKeepsEntryIdentity(t *testing.T) {
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		return &cachedValue{id: 47}, true, nil
	})

	value, exists, err := cache.loadForTest(context.Background(), 47)
	if err != nil || !exists {
		t.Fatalf("Load returned exists=%v error=%v", exists, err)
	}
	cache.mu.RLock()
	entry := cache.values[47]
	cache.mu.RUnlock()
	policyEntry, exists := cache.policy.GetIfPresent(47)
	if !exists {
		t.Fatal("policy token is missing")
	}
	if policyEntry != entry {
		t.Fatalf("policy entry = %p, values entry = %p", policyEntry, entry)
	}
	if policyEntry.value != value {
		t.Fatalf("policy value = %p, loaded value = %p", policyEntry.value, value)
	}
}

func TestCacheMergesConcurrentColdLoads(t *testing.T) {
	var loads atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		if loads.Add(1) == 1 {
			close(started)
		}
		<-release
		return &cachedValue{id: 47}, true, nil
	})

	values := make([]*cachedValue, 2)
	var wait sync.WaitGroup
	wait.Add(len(values))
	for index := range values {
		go func(index int) {
			defer wait.Done()
			value, exists, loadErr := cache.loadForTest(context.Background(), 47)
			if loadErr != nil || !exists {
				t.Errorf("Load returned exists=%v error=%v", exists, loadErr)
				return
			}
			values[index] = value
		}(index)
	}
	<-started
	close(release)
	wait.Wait()

	if loads.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", loads.Load())
	}
	if values[0] == nil || values[0] != values[1] {
		t.Fatalf("cached values = (%p,%p), want the same pointer", values[0], values[1])
	}
}

func TestCacheDoesNotRetainMissingValues(t *testing.T) {
	var loads atomic.Int32
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		loads.Add(1)
		return nil, false, nil
	})

	for range 2 {
		value, exists, loadErr := cache.loadForTest(context.Background(), 47)
		if loadErr != nil || exists || value != nil {
			t.Fatalf("Load returned value=%v exists=%v error=%v", value, exists, loadErr)
		}
	}
	if loads.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2", loads.Load())
	}
	assertKeyNotRetained(t, cache, 47)
}

func TestCacheReturnsLoaderErrorWithoutRetainingValue(t *testing.T) {
	wantErr := errors.New("load failed")
	var loads atomic.Int32
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		loads.Add(1)
		return nil, false, wantErr
	})

	for range 2 {
		value, exists, err := cache.loadForTest(context.Background(), 47)
		if !errors.Is(err, wantErr) || exists || value != nil {
			t.Fatalf("Load returned value=%v exists=%v error=%v", value, exists, err)
		}
	}
	if loads.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2", loads.Load())
	}
	assertKeyNotRetained(t, cache, 47)
}

func TestCacheAdvancesGenerationOnHitAfterPolicyActivation(t *testing.T) {
	cache := newTestCache(t, 1, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		return &cachedValue{id: 47}, true, nil
	})
	first, _, _ := cache.loadForTest(context.Background(), 47)
	cache.mu.RLock()
	entry := cache.values[47]
	firstGeneration := entry.generation.Load()
	cache.mu.RUnlock()

	second, exists, err := cache.loadForTest(context.Background(), 47)
	if err != nil || !exists || second != first {
		t.Fatalf("second Load returned value=%p exists=%v error=%v", second, exists, err)
	}
	if generation := entry.generation.Load(); generation <= firstGeneration {
		t.Fatalf("generation = %d, want greater than %d", generation, firstGeneration)
	}
}

func TestCacheReinsertsMissingPolicyToken(t *testing.T) {
	cache := newTestCache(t, 1, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		return &cachedValue{id: 47}, true, nil
	})
	first, _, _ := cache.loadForTest(context.Background(), 47)
	cache.mu.RLock()
	entry := cache.values[47]
	cache.mu.RUnlock()
	cache.policy.Invalidate(47)
	if _, exists := cache.policy.GetIfPresent(47); exists {
		t.Fatal("policy token still exists after invalidation")
	}

	second, exists, err := cache.loadForTest(context.Background(), 47)
	if err != nil || !exists || second != first {
		t.Fatalf("second Load returned value=%p exists=%v error=%v", second, exists, err)
	}
	policyEntry, exists := cache.policy.GetIfPresent(47)
	if !exists || policyEntry != entry {
		t.Fatalf("policy entry = %p exists=%v, want %p", policyEntry, exists, entry)
	}
}

func TestCacheRetainsHotPolicyTokenUnderCapacityPressure(t *testing.T) {
	const maximumSize = 16
	cache := newTestCache(t, maximumSize, 0, func(_ context.Context, key int64) (*cachedValue, bool, error) {
		return &cachedValue{id: key}, true, nil
	})
	for key := int64(0); key < maximumSize; key++ {
		if _, exists, err := cache.loadForTest(context.Background(), key); err != nil || !exists {
			t.Fatalf("warm Load(%d) returned exists=%v error=%v", key, exists, err)
		}
	}
	cache.policy.CleanUp()
	for range 256 {
		if _, exists, err := cache.loadForTest(context.Background(), 0); err != nil || !exists {
			t.Fatalf("hot Load returned exists=%v error=%v", exists, err)
		}
	}
	cache.policy.CleanUp()
	for key := int64(maximumSize); key < maximumSize*4; key++ {
		if _, exists, err := cache.loadForTest(context.Background(), key); err != nil || !exists {
			t.Fatalf("cold Load(%d) returned exists=%v error=%v", key, exists, err)
		}
	}
	cache.policy.CleanUp()

	if _, exists := cache.policy.GetEntryQuietly(0); !exists {
		t.Fatal("hot policy token was evicted")
	}
	coldEvicted := false
	for key := int64(1); key < maximumSize*4; key++ {
		if _, exists := cache.policy.GetEntryQuietly(key); !exists {
			coldEvicted = true
			break
		}
	}
	if !coldEvicted {
		t.Fatal("no cold policy token was evicted")
	}
}

func newTestCache(
	t *testing.T,
	maximumSize int,
	idleTimeout time.Duration,
	loader Loader[int64, *cachedValue],
) *Cache[int64, *cachedValue] {
	t.Helper()
	cache, err := NewCache(
		func(value int64) string { return strconv.FormatInt(value, 10) },
		loader,
		Options[*cachedValue]{
			MaximumSize: maximumSize,
			IdleTimeout: idleTimeout,
			TryRelease:  func(context.Context, *cachedValue) (bool, error) { return true, nil },
		},
	)
	if err != nil {
		t.Fatalf("NewCache returned error: %v", err)
	}
	t.Cleanup(func() {
		cache.policy.StopAllGoroutines()
	})
	return cache
}

func assertKeyNotRetained(t *testing.T, cache *Cache[int64, *cachedValue], key int64) {
	t.Helper()
	cache.mu.RLock()
	_, cached := cache.values[key]
	cache.mu.RUnlock()
	if cached {
		t.Fatalf("values[%d] was retained", key)
	}
	if _, exists := cache.policy.GetEntryQuietly(key); exists {
		t.Fatalf("policy token %d was retained", key)
	}
}

// loadForTest 仅用于测试观察一次 Use 的结果；生产调用必须在 Use 中完成对象操作。
func (cache *Cache[K, V]) loadForTest(ctx context.Context, key K) (value V, exists bool, err error) {
	exists, err = cache.Use(ctx, key, func(v V) error { value = v; return nil })
	return
}
