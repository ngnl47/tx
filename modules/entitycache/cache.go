// Package entitycache 提供进程内的实体对象缓存。
package entitycache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/txn"
	"golang.org/x/sync/singleflight"
)

// ErrReleasing 表示事务内访问遇到正在回收的对象，必须结束当前调用，不能持锁等待回收。
var ErrReleasing = errors.New("entity cache: cannot wait for release inside a transaction")

// KeyString 将强类型业务键转换为 singleflight 使用的稳定键。
type KeyString[K comparable] func(K) string

// Loader 从项目选择的存储边界加载一个业务对象。
type Loader[K comparable, V any] func(context.Context, K) (V, bool, error)

// Options 配置实体缓存的容量、可选访问过期和安全释放边界。
type Options[V any] struct {
	MaximumSize int
	IdleTimeout time.Duration
	TryRelease  func(context.Context, V) (bool, error)
}

// Cache 合并同一业务键的并发冷加载，并在进程内复用同一个对象实例。
// values 拥有业务对象身份，policy 只保存同一 cacheEntry 指针作为淘汰策略 token。
type Cache[K comparable, V any] struct {
	keyString KeyString[K]
	loader    Loader[K, V]
	options   Options[V]
	loads     singleflight.Group

	mu              sync.RWMutex
	values          map[K]*cacheEntry[V]
	nextIncarnation uint64
	policy          *otter.Cache[K, *cacheEntry[V]]
	// policyReads 在 Otter 的频率 sketch 开始工作，或启用 access expiry 后开放读路径。
	// 固定 v2.3.0 会在容量达到一半时惰性初始化 sketch；此前的读取不会进入淘汰策略。
	policyReads         atomic.Bool
	policyReadThreshold int

	deferredMu sync.Mutex
	deferred   map[K]deferredEntry[K, V]
	deferredN  atomic.Int64
	wake       chan struct{}
}

type cacheEntry[V any] struct {
	incarnation uint64
	value       V
	owner       *txn.Tx
	creating    bool
	loading     bool
	users       int
	releasing   chan struct{}
	generation  atomic.Uint64
	retired     atomic.Bool
}

// NewCache 创建实体对象缓存。
func NewCache[K comparable, V any](
	keyString KeyString[K],
	loader Loader[K, V],
	options Options[V],
) (*Cache[K, V], error) {
	if keyString == nil {
		return nil, errors.New("entity cache: key formatter is nil")
	}
	if loader == nil {
		return nil, errors.New("entity cache: loader is nil")
	}
	if options.MaximumSize <= 0 {
		return nil, errors.New("entity cache: maximum size must be greater than 0")
	}
	if options.IdleTimeout < 0 {
		return nil, errors.New("entity cache: idle timeout must not be negative")
	}
	if options.TryRelease == nil {
		return nil, errors.New("entity cache: try-release function is nil")
	}

	cache := &Cache[K, V]{
		keyString:           keyString,
		loader:              loader,
		options:             options,
		values:              make(map[K]*cacheEntry[V]),
		policyReadThreshold: max(1, options.MaximumSize/2),
		deferred:            make(map[K]deferredEntry[K, V]),
		wake:                make(chan struct{}, 1),
	}
	cache.policyReads.Store(options.IdleTimeout > 0)
	policyOptions := &otter.Options[K, *cacheEntry[V]]{
		MaximumSize:      options.MaximumSize,
		OnAtomicDeletion: cache.onAtomicDeletion,
	}
	if options.IdleTimeout > 0 {
		policyOptions.ExpiryCalculator = otter.ExpiryAccessing[K, *cacheEntry[V]](options.IdleTimeout)
	}
	policy, err := otter.New(policyOptions)
	if err != nil {
		return nil, err
	}
	cache.policy = policy
	return cache, nil
}

// Start 将缓存维护任务登记到服务生命周期。
func (cache *Cache[K, V]) Start(tasks lifecycle.Tasks) error {
	if cache == nil || cache.policy == nil {
		return nil
	}
	if tasks == nil {
		return errors.New("entity cache: lifecycle tasks are nil")
	}
	return tasks.Go(cache.runReleaseWorker)
}

// Use 在回调期间保留唯一对象，覆盖加载后等待事务锁、事务执行和提交回调。
// 回调结束（含错误或 panic）释放使用权；对象与子对象不得逃逸到回调外继续使用。
// 它只阻止缓存淘汰，不替代 Procedure 的数据锁；不存在时不执行回调。
func (cache *Cache[K, V]) Use(ctx context.Context, key K, use func(V) error) (bool, error) {
	entry, exists, err := cache.acquireLoaded(ctx, key)
	if err != nil || !exists {
		return exists, err
	}
	defer func() {
		cache.mu.Lock()
		entry.users--
		cache.mu.Unlock()
	}()
	return true, use(entry.value)
}

// acquireLoaded 合并冷加载并重新取得使用权；Use 的单次 defer 不放入重试循环。
func (cache *Cache[K, V]) acquireLoaded(ctx context.Context, key K) (*cacheEntry[V], bool, error) {
	for {
		entry, err := cache.acquire(ctx, key)
		if err != nil {
			return nil, false, err
		}
		if entry != nil {
			return entry, true, nil
		}
		loaded, err, _ := cache.loads.Do(cache.keyString(key), func() (any, error) {
			return cache.loadMissing(ctx, key)
		})
		if err != nil {
			return nil, false, err
		}
		if !loaded.(bool) {
			return nil, false, nil
		}
	}
}

// acquire 与回收共用缓存锁；一旦回收开始，新使用者等待它完成后重新定位对象。
func (cache *Cache[K, V]) acquire(ctx context.Context, key K) (*cacheEntry[V], error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cache.mu.Lock()
		entry := cache.values[key]
		if entry == nil || entry.loading {
			cache.mu.Unlock()
			return nil, nil
		}
		if done := entry.releasing; done != nil {
			cache.mu.Unlock()
			// 回收可能正在等待本事务持有的实体锁，不能反向等待它结束。
			if _, active := txn.FromContext(ctx); active {
				return nil, ErrReleasing
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}
		if entry.creating {
			owner, active := txn.FromContext(ctx)
			if !active || entry.owner != owner {
				cache.mu.Unlock()
				return nil, ErrCreating
			}
		}
		entry.users++
		trackPolicy := entry.owner == nil && cache.policyReads.Load()
		if trackPolicy {
			entry.generation.Add(1)
		}
		cache.mu.Unlock()
		if trackPolicy {
			cache.cancelDeferred(key, entry)
			cache.touchPolicy(key, entry)
		}
		return entry, nil
	}
}

// loadMissing 只填充缓存；返回后由 acquire 原子取得使用权，淘汰抢先完成时重新加载。
func (cache *Cache[K, V]) loadMissing(ctx context.Context, key K) (bool, error) {
	entry := &cacheEntry[V]{loading: true}
	entry.generation.Store(1)
	cache.mu.Lock()
	if cache.values[key] != nil {
		cache.mu.Unlock()
		return true, nil
	}
	cache.nextIncarnation++
	entry.incarnation = cache.nextIncarnation
	cache.values[key] = entry
	cache.mu.Unlock()
	completed := false
	defer func() {
		if !completed {
			cache.mu.Lock()
			delete(cache.values, key)
			cache.mu.Unlock()
		}
	}()
	value, exists, err := cache.loader(ctx, key)
	if err != nil || !exists {
		return exists, err
	}
	cache.mu.Lock()
	entry.value = value
	// policy 发布与可回收状态共用屏障，禁止已退休的 entry 被冷加载重新发布。
	cache.policy.Set(key, entry)
	entry.loading = false
	count := len(cache.values)
	cache.mu.Unlock()
	completed = true
	cache.activatePolicyReads(count)
	return true, nil
}

func (cache *Cache[K, V]) touchPolicy(key K, entry *cacheEntry[V]) {
	if !cache.policyReads.Load() {
		return
	}
	policyEntry, exists := cache.policy.GetIfPresent(key)
	if !exists || policyEntry != entry {
		cache.policy.Set(key, entry)
	}
}

func (cache *Cache[K, V]) activatePolicyReads(residentCount int) {
	if cache.policyReads.Load() || residentCount < cache.policyReadThreshold {
		return
	}
	cache.policy.CleanUp()
	cache.policyReads.Store(true)
}
