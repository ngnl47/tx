package entitycache

import (
	"context"
	"time"

	"github.com/maypok86/otter/v2"
	txlog "github.com/ngnl47/tx/log"
)

const (
	releaseRetryInitialDelay = 100 * time.Millisecond
	releaseRetryMaxDelay     = 5 * time.Second
)

type deferredEntry[K comparable, V any] struct {
	key         K
	entry       *cacheEntry[V]
	generation  uint64
	delay       time.Duration
	nextAttempt time.Time
	running     bool
}

func (cache *Cache[K, V]) onAtomicDeletion(event otter.DeletionEvent[K, *cacheEntry[V]]) {
	if !event.WasEvicted() {
		return
	}
	cache.policyReads.Store(true)
	candidate := deferredEntry[K, V]{
		key:         event.Key,
		entry:       event.Value,
		generation:  event.Value.generation.Load(),
		delay:       releaseRetryInitialDelay,
		nextAttempt: time.Now(),
	}

	cache.deferredMu.Lock()
	if event.Value.retired.Load() {
		cache.deferredMu.Unlock()
		return
	}
	current, exists := cache.deferred[event.Key]
	changed := !exists
	if !exists {
		cache.deferred[event.Key] = candidate
		cache.deferredN.Add(1)
	} else if current.entry == event.Value && current.generation < candidate.generation {
		cache.deferred[event.Key] = candidate
		changed = true
	}
	cache.deferredMu.Unlock()
	if changed {
		cache.wakeWorker()
	}
}

func (cache *Cache[K, V]) cancelDeferred(key K, entry *cacheEntry[V]) {
	if cache.deferredN.Load() == 0 {
		return
	}
	cache.deferredMu.Lock()
	if candidate, exists := cache.deferred[key]; exists &&
		(candidate.entry != entry || candidate.generation < entry.generation.Load()) {
		delete(cache.deferred, key)
		cache.deferredN.Add(-1)
	}
	cache.deferredMu.Unlock()
}

func (cache *Cache[K, V]) wakeWorker() {
	select {
	case cache.wake <- struct{}{}:
	default:
	}
}

func (cache *Cache[K, V]) runReleaseWorker(ctx context.Context) {
	defer cache.policy.StopAllGoroutines()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		candidate, wait, ready := cache.nextDeferred(time.Now())
		if ready {
			cache.tryRelease(ctx, candidate)
			continue
		}
		if wait < 0 {
			select {
			case <-ctx.Done():
				return
			case <-cache.wake:
			}
			continue
		}

		timer.Reset(wait)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-cache.wake:
			stopTimer(timer)
		case <-timer.C:
		}
	}
}

func (cache *Cache[K, V]) nextDeferred(now time.Time) (deferredEntry[K, V], time.Duration, bool) {
	cache.deferredMu.Lock()
	defer cache.deferredMu.Unlock()

	var selected deferredEntry[K, V]
	found := false
	for _, candidate := range cache.deferred {
		if candidate.running || found && !candidate.nextAttempt.Before(selected.nextAttempt) {
			continue
		}
		selected = candidate
		found = true
	}
	if !found {
		return deferredEntry[K, V]{}, -1, false
	}
	if wait := selected.nextAttempt.Sub(now); wait > 0 {
		return deferredEntry[K, V]{}, wait, false
	}
	selected.running = true
	cache.deferred[selected.key] = selected
	return selected, 0, true
}

func (cache *Cache[K, V]) tryRelease(ctx context.Context, candidate deferredEntry[K, V]) {
	cache.mu.Lock()
	result, err := cache.releaseEntryLocked(ctx, candidate.key, candidate.entry, candidate.generation, true)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		txlog.Error("entity cache try release failed", "key", cache.keyString(candidate.key), "err", err)
	}
	switch result {
	case Released, NotCached, Stale:
		cache.removeDeferred(candidate)
	default:
		cache.rescheduleDeferred(candidate)
	}
}

// releaseEntryLocked 接收已持有的缓存锁并负责释放；手动与自动回收共用同一屏障。
// 不在缓存锁内等待记录锁；只有安全判定成功才退休 policy 与对象身份。
func (cache *Cache[K, V]) releaseEntryLocked(ctx context.Context, key K, expected *cacheEntry[V], generation uint64, checkGeneration bool) (result ReleaseResult, err error) {
	entry := cache.values[key]
	if entry == nil {
		cache.mu.Unlock()
		return NotCached, nil
	}
	if entry != expected || checkGeneration && entry.generation.Load() != generation {
		cache.mu.Unlock()
		return Stale, nil
	}
	if entry.users > 0 || entry.creating || entry.loading || entry.releasing != nil {
		cache.mu.Unlock()
		return Busy, nil
	}
	entry.releasing = make(chan struct{})
	cache.mu.Unlock()
	released := false
	defer func() {
		cache.mu.Lock()
		if released && cache.values[key] == entry {
			entry.retired.Store(true)
			cache.policy.Invalidate(key)
			cache.deferredMu.Lock()
			if candidate, ok := cache.deferred[key]; ok && candidate.entry == entry {
				delete(cache.deferred, key)
				cache.deferredN.Add(-1)
			}
			cache.deferredMu.Unlock()
			delete(cache.values, key)
		}
		close(entry.releasing)
		entry.releasing = nil
		cache.mu.Unlock()
	}()
	released, err = cache.options.TryRelease(ctx, entry.value)
	if err != nil {
		released = false
	}
	if released {
		return Released, nil
	}
	return Retained, err
}

func (cache *Cache[K, V]) isCurrent(candidate deferredEntry[K, V]) bool {
	cache.mu.RLock()
	current := cache.values[candidate.key]
	currentGeneration := uint64(0)
	if current != nil {
		currentGeneration = current.generation.Load()
	}
	cache.mu.RUnlock()
	return current == candidate.entry && currentGeneration == candidate.generation
}

func (cache *Cache[K, V]) removeDeferred(candidate deferredEntry[K, V]) {
	cache.deferredMu.Lock()
	current, exists := cache.deferred[candidate.key]
	if exists && current.entry == candidate.entry && current.generation == candidate.generation {
		delete(cache.deferred, candidate.key)
		cache.deferredN.Add(-1)
	}
	cache.deferredMu.Unlock()
}

func (cache *Cache[K, V]) rescheduleDeferred(candidate deferredEntry[K, V]) {
	if !cache.isCurrent(candidate) {
		cache.removeDeferred(candidate)
		return
	}

	cache.deferredMu.Lock()
	current, exists := cache.deferred[candidate.key]
	if exists && current.entry == candidate.entry && current.generation == candidate.generation {
		current.running = false
		current.nextAttempt = time.Now().Add(current.delay)
		current.delay = min(current.delay*2, releaseRetryMaxDelay)
		cache.deferred[candidate.key] = current
	}
	cache.deferredMu.Unlock()
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
