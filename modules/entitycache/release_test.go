package entitycache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/ngnl47/tx/lifecycle"
)

func TestAtomicDeletionIgnoresNonEvictionEvents(t *testing.T) {
	var releases atomic.Int32
	cache := newReleaseTestCache(t, 16, func(context.Context, *cachedValue) (bool, error) {
		releases.Add(1)
		return true, nil
	})
	entry := loadReleaseTestEntry(t, cache, 47)

	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 47, Value: entry, Cause: otter.CauseInvalidation,
	})
	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 47, Value: entry, Cause: otter.CauseReplacement,
	})

	cache.deferredMu.Lock()
	deferredCount := len(cache.deferred)
	cache.deferredMu.Unlock()
	if deferredCount != 0 {
		t.Fatalf("deferred candidates = %d, want 0", deferredCount)
	}
	if releases.Load() != 0 {
		t.Fatalf("TryRelease calls = %d, want 0", releases.Load())
	}
}

func TestAtomicDeletionDeduplicatesEvictionCandidates(t *testing.T) {
	cache := newReleaseTestCache(t, 16, func(context.Context, *cachedValue) (bool, error) { return true, nil })
	entry := loadReleaseTestEntry(t, cache, 47)
	generation := entry.generation.Load()

	for _, cause := range []otter.DeletionCause{otter.CauseOverflow, otter.CauseExpiration, otter.CauseOverflow} {
		cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
			Key: 47, Value: entry, Cause: cause,
		})
	}

	cache.deferredMu.Lock()
	candidate, exists := cache.deferred[47]
	deferredCount := len(cache.deferred)
	cache.deferredMu.Unlock()
	if !exists || deferredCount != 1 {
		t.Fatalf("deferred candidate exists=%v count=%d, want true and 1", exists, deferredCount)
	}
	if candidate.entry != entry || candidate.generation != generation {
		t.Fatalf("candidate entry=%p generation=%d, want %p generation=%d", candidate.entry, candidate.generation, entry, generation)
	}
}

func TestAtomicDeletionDoesNotReplaceNewEntryCandidateWithOldEntry(t *testing.T) {
	cache := newReleaseTestCache(t, 16, func(context.Context, *cachedValue) (bool, error) { return true, nil })
	oldEntry := &cacheEntry[*cachedValue]{value: &cachedValue{id: 47}}
	oldEntry.generation.Store(9)
	newEntry := &cacheEntry[*cachedValue]{value: &cachedValue{id: 48}}
	newEntry.generation.Store(1)

	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 47, Value: newEntry, Cause: otter.CauseOverflow,
	})
	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 47, Value: oldEntry, Cause: otter.CauseOverflow,
	})

	cache.deferredMu.Lock()
	candidate := cache.deferred[47]
	cache.deferredMu.Unlock()
	if candidate.entry != newEntry {
		t.Fatalf("candidate entry = %p, want new entry %p", candidate.entry, newEntry)
	}
}

func TestCacheCancelsDeferredGenerationOnHit(t *testing.T) {
	cache := newReleaseTestCache(t, 16, func(context.Context, *cachedValue) (bool, error) { return true, nil })
	entry := loadReleaseTestEntry(t, cache, 47)
	cache.policy.Invalidate(47)
	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 47, Value: entry, Cause: otter.CauseOverflow,
	})

	value, exists, err := cache.loadForTest(context.Background(), 47)
	if err != nil || !exists || value != entry.value {
		t.Fatalf("Load returned value=%p exists=%v error=%v", value, exists, err)
	}
	cache.deferredMu.Lock()
	_, deferred := cache.deferred[47]
	cache.deferredMu.Unlock()
	if deferred {
		t.Fatal("touched entry retained an old deferred candidate")
	}
	policyEntry, exists := cache.policy.GetIfPresent(47)
	if !exists || policyEntry != entry {
		t.Fatalf("policy entry=%p exists=%v, want %p", policyEntry, exists, entry)
	}
}

func TestDeferredReleaseDeletesUnchangedEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var releases atomic.Int32
		cache := newBubbleReleaseTestCache(t, func(context.Context, *cachedValue) (bool, error) {
			releases.Add(1)
			return true, nil
		})
		entry := loadReleaseTestEntry(t, cache, 47)
		cache.policy.Invalidate(47)
		tasks := startReleaseTestCache(t, cache)
		cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
			Key: 47, Value: entry, Cause: otter.CauseOverflow,
		})
		synctest.Wait()

		cache.mu.RLock()
		_, exists := cache.values[47]
		cache.mu.RUnlock()
		if exists {
			t.Fatal("released entry remains in values")
		}
		if releases.Load() != 1 {
			t.Fatalf("TryRelease calls = %d, want 1", releases.Load())
		}
		stopReleaseTestCache(tasks, cache)
	})
}

func TestDeferredReleaseUsesCappedExponentialBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var attempts []time.Time
		cache := newBubbleReleaseTestCache(t, func(context.Context, *cachedValue) (bool, error) {
			mu.Lock()
			attempts = append(attempts, time.Now())
			attempt := len(attempts)
			mu.Unlock()
			return attempt == 9, nil
		})
		entry := loadReleaseTestEntry(t, cache, 47)
		cache.policy.Invalidate(47)
		tasks := startReleaseTestCache(t, cache)
		cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
			Key: 47, Value: entry, Cause: otter.CauseOverflow,
		})
		time.Sleep(16_300 * time.Millisecond)
		synctest.Wait()

		mu.Lock()
		got := append([]time.Time(nil), attempts...)
		mu.Unlock()
		wantIntervals := []time.Duration{
			100 * time.Millisecond,
			200 * time.Millisecond,
			400 * time.Millisecond,
			800 * time.Millisecond,
			1600 * time.Millisecond,
			3200 * time.Millisecond,
			5 * time.Second,
			5 * time.Second,
		}
		if len(got) != len(wantIntervals)+1 {
			t.Fatalf("TryRelease calls = %d, want %d", len(got), len(wantIntervals)+1)
		}
		for index, want := range wantIntervals {
			if interval := got[index+1].Sub(got[index]); interval != want {
				t.Fatalf("retry interval %d = %v, want %v", index, interval, want)
			}
		}
		stopReleaseTestCache(tasks, cache)
	})
}

func TestDeferredReleaseRetriesAfterError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wantErr := errors.New("release failed")
		var attempts []time.Time
		cache := newBubbleReleaseTestCache(t, func(context.Context, *cachedValue) (bool, error) {
			attempts = append(attempts, time.Now())
			if len(attempts) == 1 {
				return false, wantErr
			}
			return true, nil
		})
		entry := loadReleaseTestEntry(t, cache, 47)
		cache.policy.Invalidate(47)
		tasks := startReleaseTestCache(t, cache)
		cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
			Key: 47, Value: entry, Cause: otter.CauseOverflow,
		})
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		if len(attempts) != 2 {
			t.Fatalf("TryRelease calls = %d, want 2", len(attempts))
		}
		if interval := attempts[1].Sub(attempts[0]); interval != 100*time.Millisecond {
			t.Fatalf("retry interval = %v, want 100ms", interval)
		}
		stopReleaseTestCache(tasks, cache)
	})
}

func TestUseWaitsForReleaseAndHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, resume, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
		cache := newBubbleReleaseTestCache(t, func(context.Context, *cachedValue) (bool, error) {
			close(started)
			<-resume
			return true, nil
		})
		defer cache.policy.StopAllGoroutines()
		entry := loadReleaseTestEntry(t, cache, 47)
		candidate := deferredEntry[int64, *cachedValue]{key: 47, entry: entry, generation: entry.generation.Load()}
		go func() { defer close(released); cache.tryRelease(t.Context(), candidate) }()
		<-started

		ctx, cancel := context.WithCancel(t.Context())
		cancelled := make(chan error, 1)
		go func() {
			_, err := cache.Use(ctx, 47, func(*cachedValue) error { t.Error("取消等待后执行了回调"); return nil })
			cancelled <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-cancelled; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := cache.Use(t.Context(), 48, func(*cachedValue) error { return nil }); err != nil {
			t.Fatal(err)
		}

		loaded := make(chan *cachedValue, 1)
		go func() { _, _ = cache.Use(t.Context(), 47, func(v *cachedValue) error { loaded <- v; return nil }) }()
		synctest.Wait()
		select {
		case <-loaded:
			t.Fatal("回收判定结束前获得了对象")
		default:
		}
		close(resume)
		<-released
		if value := <-loaded; value == entry.value {
			t.Fatal("成功回收后仍返回旧对象")
		}
		synctest.Wait()
	})
}

func TestDeferredReleaseDoesNotDeleteReplacementEntry(t *testing.T) {
	started := make(chan struct{})
	resume := make(chan struct{})
	cache := newReleaseTestCache(t, 16, func(context.Context, *cachedValue) (bool, error) {
		close(started)
		<-resume
		return true, nil
	})
	oldEntry := loadReleaseTestEntry(t, cache, 47)
	cache.policy.Invalidate(47)
	tasks := startReleaseTestCache(t, cache)
	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 47, Value: oldEntry, Cause: otter.CauseOverflow,
	})
	<-started

	replacement := &cacheEntry[*cachedValue]{value: &cachedValue{id: 48}}
	replacement.generation.Store(1)
	cache.mu.Lock()
	cache.values[47] = replacement
	cache.mu.Unlock()
	cache.policy.Set(47, replacement)
	close(resume)
	stopReleaseTestCache(tasks, cache)

	cache.mu.RLock()
	current := cache.values[47]
	cache.mu.RUnlock()
	if current != replacement {
		t.Fatalf("current entry = %p, want replacement %p", current, replacement)
	}
}

func TestDeferredReleaseSerializesMultipleKeys(t *testing.T) {
	started := make(chan int64, 2)
	resume := make(chan struct{})
	cache := newReleaseTestCache(t, 16, func(_ context.Context, value *cachedValue) (bool, error) {
		started <- value.id
		<-resume
		return true, nil
	})
	first := loadReleaseTestEntry(t, cache, 47)
	second := loadReleaseTestEntry(t, cache, 48)
	cache.policy.Invalidate(47)
	cache.policy.Invalidate(48)
	tasks := startReleaseTestCache(t, cache)
	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 47, Value: first, Cause: otter.CauseOverflow,
	})
	cache.onAtomicDeletion(otter.DeletionEvent[int64, *cacheEntry[*cachedValue]]{
		Key: 48, Value: second, Cause: otter.CauseOverflow,
	})
	<-started
	select {
	case key := <-started:
		t.Fatalf("second key %d started while first TryRelease was blocked", key)
	default:
	}
	resume <- struct{}{}
	<-started
	resume <- struct{}{}
	stopReleaseTestCache(tasks, cache)
}

func TestCacheKeepsBusinessValuesBeyondMaximumSizeWhenReleaseDeferred(t *testing.T) {
	const valueCount = 32
	cache := newReleaseTestCache(t, 1, func(context.Context, *cachedValue) (bool, error) { return false, nil })
	want := make(map[int64]*cachedValue, valueCount)
	for key := int64(0); key < valueCount; key++ {
		value, exists, err := cache.loadForTest(context.Background(), key)
		if err != nil || !exists {
			t.Fatalf("Load(%d) returned exists=%v error=%v", key, exists, err)
		}
		want[key] = value
	}
	cache.policy.CleanUp()

	cache.mu.RLock()
	residentCount := len(cache.values)
	cache.mu.RUnlock()
	if residentCount != valueCount {
		t.Fatalf("resident values = %d, want %d", residentCount, valueCount)
	}
	cache.deferredMu.Lock()
	deferredCount := len(cache.deferred)
	cache.deferredMu.Unlock()
	if deferredCount == 0 {
		t.Fatal("capacity pressure produced no deferred candidates")
	}
	for key, expected := range want {
		actual, exists, err := cache.loadForTest(context.Background(), key)
		if err != nil || !exists || actual != expected {
			t.Fatalf("Load(%d) returned value=%p exists=%v error=%v, want %p", key, actual, exists, err, expected)
		}
	}
}

func TestCacheExpiryDisabledKeepsPolicyToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := newBubbleEntityCache(t, 16, 0, func(context.Context, *cachedValue) (bool, error) { return true, nil })
		entry := loadReleaseTestEntry(t, cache, 47)
		time.Sleep(24 * time.Hour)
		cache.policy.CleanUp()

		policyEntry, exists := cache.policy.GetEntryQuietly(47)
		if !exists || policyEntry.Value != entry {
			t.Fatalf("policy entry=%p exists=%v, want %p", policyEntry.Value, exists, entry)
		}
		cache.policy.StopAllGoroutines()
	})
}

func TestCacheExpiryRefreshesAfterAccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idleTimeout = time.Second
		cache := newBubbleEntityCache(t, 16, idleTimeout, func(context.Context, *cachedValue) (bool, error) { return true, nil })
		entry := loadReleaseTestEntry(t, cache, 47)
		time.Sleep(750 * time.Millisecond)
		if _, exists, err := cache.loadForTest(context.Background(), 47); err != nil || !exists {
			t.Fatalf("refreshing Load returned exists=%v error=%v", exists, err)
		}
		time.Sleep(750 * time.Millisecond)
		cache.policy.CleanUp()
		if policyEntry, exists := cache.policy.GetEntryQuietly(47); !exists || policyEntry.Value != entry {
			t.Fatalf("policy entry=%p exists=%v before refreshed timeout, want %p", policyEntry.Value, exists, entry)
		}

		time.Sleep(2 * idleTimeout)
		cache.policy.CleanUp()
		if _, exists := cache.policy.GetEntryQuietly(47); exists {
			t.Fatal("policy token remains after refreshed idle timeout")
		}
		cache.deferredMu.Lock()
		candidate, deferred := cache.deferred[47]
		cache.deferredMu.Unlock()
		if !deferred || candidate.entry != entry {
			t.Fatalf("expiration candidate=%p exists=%v, want %p", candidate.entry, deferred, entry)
		}
		cache.policy.StopAllGoroutines()
	})
}

func TestCacheExpiryKeepsBusinessValueWhenReleaseIsDeferred(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var releases atomic.Int32
		cache := newBubbleEntityCache(t, 16, time.Second, func(context.Context, *cachedValue) (bool, error) {
			releases.Add(1)
			return false, nil
		})
		entry := loadReleaseTestEntry(t, cache, 47)
		time.Sleep(2 * time.Second)
		cache.policy.CleanUp()
		tasks := startReleaseTestCache(t, cache)
		synctest.Wait()

		cache.mu.RLock()
		current := cache.values[47]
		cache.mu.RUnlock()
		if current != entry {
			t.Fatalf("current entry=%p, want deferred entry %p", current, entry)
		}
		if releases.Load() != 1 {
			t.Fatalf("TryRelease calls=%d, want 1", releases.Load())
		}
		stopReleaseTestCache(tasks, cache)
	})
}

func TestCacheStartRejectsNilTasks(t *testing.T) {
	cache := newReleaseTestCache(t, 16, func(context.Context, *cachedValue) (bool, error) { return true, nil })
	if err := cache.Start(nil); err == nil {
		t.Fatal("Start returned nil error")
	}
}

func TestCacheShutdownStopsPolicyGoroutines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := newBubbleEntityCache(t, 16, time.Second, func(context.Context, *cachedValue) (bool, error) { return true, nil })
		tasks := startReleaseTestCache(t, cache)
		tasks.Cancel()
		tasks.Wait()

		if cache.policy.StopAllGoroutines() {
			t.Fatal("policy goroutines remained active after EntityCache shutdown")
		}
		cache.deferredMu.Lock()
		deferredCount := len(cache.deferred)
		cache.deferredMu.Unlock()
		if deferredCount != 0 {
			t.Fatalf("shutdown registered %d non-eviction candidates", deferredCount)
		}
	})
}

func newReleaseTestCache(
	t *testing.T,
	maximumSize int,
	tryRelease func(context.Context, *cachedValue) (bool, error),
) *Cache[int64, *cachedValue] {
	t.Helper()
	cache, err := NewCache(
		func(key int64) string { return string(rune(key)) },
		func(_ context.Context, key int64) (*cachedValue, bool, error) {
			return &cachedValue{id: key}, true, nil
		},
		Options[*cachedValue]{
			MaximumSize: maximumSize,
			TryRelease:  tryRelease,
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

func newBubbleReleaseTestCache(
	t *testing.T,
	tryRelease func(context.Context, *cachedValue) (bool, error),
) *Cache[int64, *cachedValue] {
	return newBubbleEntityCache(t, 16, 0, tryRelease)
}

func newBubbleEntityCache(
	t *testing.T,
	maximumSize int,
	idleTimeout time.Duration,
	tryRelease func(context.Context, *cachedValue) (bool, error),
) *Cache[int64, *cachedValue] {
	t.Helper()
	cache, err := NewCache(
		func(key int64) string { return string(rune(key)) },
		func(_ context.Context, key int64) (*cachedValue, bool, error) {
			return &cachedValue{id: key}, true, nil
		},
		Options[*cachedValue]{
			MaximumSize: maximumSize,
			IdleTimeout: idleTimeout,
			TryRelease:  tryRelease,
		},
	)
	if err != nil {
		t.Fatalf("NewCache returned error: %v", err)
	}
	return cache
}

func loadReleaseTestEntry(
	t *testing.T,
	cache *Cache[int64, *cachedValue],
	key int64,
) *cacheEntry[*cachedValue] {
	t.Helper()
	if _, exists, err := cache.loadForTest(context.Background(), key); err != nil || !exists {
		t.Fatalf("Load(%d) returned exists=%v error=%v", key, exists, err)
	}
	cache.mu.RLock()
	entry := cache.values[key]
	cache.mu.RUnlock()
	return entry
}

func startReleaseTestCache(t *testing.T, cache *Cache[int64, *cachedValue]) *lifecycle.WaitGroupContext {
	t.Helper()
	tasks := lifecycle.NewWaitGroupContext(context.Background())
	if err := cache.Start(tasks); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	return tasks
}

func stopReleaseTestCache(tasks *lifecycle.WaitGroupContext, cache *Cache[int64, *cachedValue]) {
	tasks.Cancel()
	tasks.Wait()
	cache.policy.StopAllGoroutines()
}
