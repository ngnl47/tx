package entitycache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

var (
	ErrInvalidReference      = errors.New("entity cache: reference belongs to another runtime or is empty")
	ErrStaleReference        = errors.New("entity cache: reference belongs to an earlier residency")
	ErrNotCached             = errors.New("entity cache: entry is not cached")
	ErrEntryBusy             = errors.New("entity cache: entry is not available for inspection")
	ErrManagementInProcedure = errors.New("entity cache: management snapshot and release require a context without Procedure")
	ErrManagementDraining    = errors.New("entity cache: management is draining")
	ErrManagementClosed      = errors.New("entity cache: management is closed")
)

// EntryRef 仅在创建它的运行时内使用；不持有实体，不延长使用期，不可序列化保存。
// 驻留代次与淘汰访问 generation 独立，普通查询不会使引用失效。
type EntryRef struct {
	owner       *managementGate
	id          identity
	incarnation uint64
}

// Stats 描述缓存规模；数量分别观察，不承诺跨指标的原子一致性。
type Stats struct {
	Entries          int
	DeferredReleases int64
	MaximumSize      int
	IdleTimeout      time.Duration
}

// EntryState 是框架元数据阶段，不代表业务对象状态或持久化完成。
type EntryState string

const (
	EntryLoading   EntryState = "loading"
	EntryCreating  EntryState = "creating"
	EntryReady     EntryState = "ready"
	EntryDeleting  EntryState = "deleting"
	EntryReleasing EntryState = "releasing"
)

// EntryMetadata 不包含 GD、Store 或业务集合的引用；Record 是完整存储身份。
type EntryMetadata struct {
	Ref      EntryRef
	TypeName string
	Record   storage.RecordKey
	State    EntryState
	UseCount int
}

// ListOptions 是低频诊断过滤条件；Limit 缺省100，最多1000，不提供排序或稳定分页。
type ListOptions struct {
	TypeName string
	Limit    int
}
type EntryList struct {
	Items     []EntryMetadata
	Truncated bool
}

// ReleaseResult 区分正常保留与已释放，不推断数据库记录或 Actor 的状态。
type ReleaseResult string

const (
	Released  ReleaseResult = "released"
	NotCached ReleaseResult = "not_cached"
	Stale     ReleaseResult = "stale"
	Busy      ReleaseResult = "busy"
	Retained  ReleaseResult = "retained"
)

// managementGate 只统计管理操作；普通 Find/Snapshot 不经过此锁或计数。
// 元数据观察短时持有 mu，排水期间允许；最终关闭后拒绝所有管理调用。
type managementGate struct {
	mu       sync.Mutex
	draining bool
	closed   bool
	active   int
	idle     chan struct{}
}

func (g *managementGate) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.closed {
		return ErrManagementClosed
	}
	return nil
}
func (g *managementGate) begin(ctx context.Context) (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.check(ctx); err != nil {
		return nil, err
	}
	if g.draining {
		return nil, ErrManagementDraining
	}
	if tx, _ := txn.FromContext(ctx); tx != nil {
		return nil, ErrManagementInProcedure
	}
	if g.active == 0 {
		g.idle = make(chan struct{})
	}
	g.active++
	return func() {
		g.mu.Lock()
		g.active--
		if g.active == 0 {
			close(g.idle)
		}
		g.mu.Unlock()
	}, nil
}
func (g *managementGate) stop(ctx context.Context, closeAll bool) error {
	g.mu.Lock()
	g.draining = true
	g.closed = g.closed || closeAll
	idle := g.idle
	g.mu.Unlock()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DrainManagement 关闭新快照/回收准入，等待已接纳操作实际退出；不阻断排水中的业务查询。
func (r *Runtime) DrainManagement(ctx context.Context) error { return r.management.stop(ctx, false) }

// CloseManagement 最终关闭管理入口。超时不会提前释放仍在执行的操作，可以再次等待。
func (r *Runtime) CloseManagement(ctx context.Context) error { return r.management.stop(ctx, true) }

// Stats 只观察规模和配置，不扫描 GD、不触发加载、不刷新淘汰热度。
func (r *Runtime) Stats(ctx context.Context) (Stats, error) {
	r.management.mu.Lock()
	defer r.management.mu.Unlock()
	if err := r.management.check(ctx); err != nil {
		return Stats{}, err
	}
	c := r.cache
	c.mu.RLock()
	count := len(c.values)
	c.mu.RUnlock()
	return Stats{Entries: count, DeferredReleases: c.deferredN.Load(), MaximumSize: c.options.MaximumSize, IdleTimeout: c.options.IdleTimeout}, nil
}

// List 返回有界元数据视图；筛选最坏扫描全部条目，不能用于高频全量采集。
// 不读取业务字段；后续快照/回收必须重新验证 Ref，列表不是操作预留。
func (r *Runtime) List(ctx context.Context, options ListOptions) (EntryList, error) {
	r.management.mu.Lock()
	defer r.management.mu.Unlock()
	if err := r.management.check(ctx); err != nil {
		return EntryList{}, err
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 0 || limit > 1000 {
		return EntryList{}, errors.New("entity cache: list limit must be between 1 and 1000")
	}
	c := r.cache
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := EntryList{Items: make([]EntryMetadata, 0, min(limit, len(c.values)))}
	for id, e := range c.values {
		if err := ctx.Err(); err != nil {
			return EntryList{}, err
		}
		if options.TypeName != "" && id.descriptor.TypeName != options.TypeName {
			continue
		}
		if len(result.Items) == limit {
			result.Truncated = true
			break
		}
		state := EntryReady
		switch {
		case e.loading:
			state = EntryLoading
		case e.creating:
			state = EntryCreating
		case e.releasing != nil:
			state = EntryReleasing
		case e.value.deleting:
			state = EntryDeleting
		}
		result.Items = append(result.Items, EntryMetadata{Ref: EntryRef{r.management, id, e.incarnation}, TypeName: id.descriptor.TypeName, Record: id.record, State: state, UseCount: e.users})
	}
	return result, nil
}

func (r *Runtime) validateRef(ref EntryRef) error {
	if ref.owner != r.management || ref.incarnation == 0 {
		return ErrInvalidReference
	}
	return nil
}

// SnapshotCached 只复制指定驻留对象；未缓存、过渡状态或旧引用均不加载存储。
// 复制在记录锁内完成，返回的业务数据可在锁外序列化；复制开始后取消不代表已退出。
func (r *Runtime) SnapshotCached(ctx context.Context, ref EntryRef) (any, error) {
	end, err := r.management.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	if err = r.validateRef(ref); err != nil {
		return nil, err
	}
	c := r.cache
	c.mu.Lock()
	e := c.values[ref.id]
	if e == nil {
		c.mu.Unlock()
		return nil, ErrNotCached
	}
	if e.incarnation != ref.incarnation {
		c.mu.Unlock()
		return nil, ErrStaleReference
	}
	if e.loading || e.creating || e.releasing != nil || e.value.deleting {
		c.mu.Unlock()
		return nil, ErrEntryBusy
	}
	e.users++
	c.mu.Unlock()
	defer func() { c.mu.Lock(); e.users--; c.mu.Unlock() }()
	var copy any
	err = txn.WithLocks(ctx, []txn.LockTarget{e.value.value.(txn.Lockable).LockTarget()}, func(ctx context.Context) error {
		value, found, err := r.visible(ctx, e.value)
		if err != nil {
			return err
		}
		if !found {
			return ErrNotCached
		}
		copy, err = ref.id.descriptor.Clone(value)
		return err
	})
	return copy, err
}

// TryRelease 仅尝试回收当前驻留对象，不加载、不刷盘、不删除持久记录，不启动后台重试。
func (r *Runtime) TryRelease(ctx context.Context, ref EntryRef) (ReleaseResult, error) {
	end, err := r.management.begin(ctx)
	if err != nil {
		return Retained, err
	}
	defer end()
	if err = r.validateRef(ref); err != nil {
		return Retained, err
	}
	c := r.cache
	c.mu.Lock()
	e := c.values[ref.id]
	if e == nil {
		c.mu.Unlock()
		return NotCached, nil
	}
	if e.incarnation != ref.incarnation {
		c.mu.Unlock()
		return Stale, nil
	}
	if !e.loading && e.value.deleting {
		c.mu.Unlock()
		return Busy, nil
	}
	return c.releaseEntryLocked(ctx, ref.id, e, 0, false)
}
