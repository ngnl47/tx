// Package cache 提供有状态服务的根对象身份与延迟刷盘管理。
package cache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	goruntime "runtime"
	"sort"
	"sync"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

var (
	ErrIdentityConflict          = errors.New("cache manager: lock key is bound to a different Store")
	ErrInvalidTarget             = errors.New("cache manager: invalid lock target")
	ErrPendingRoot               = errors.New("cache manager: root still has pending changes")
	ErrPendingRootCapacity       = errors.New("cache manager: pending root capacity reached")
	ErrChangeCollapseUnsupported = errors.New("cache manager: persistent Store does not support change collapse")
)

// WriteAdmission 在 Procedure 修改内存对象前检查外部存储是否允许产生新的待刷状态。
type WriteAdmission interface {
	RequireWritable() error
}

// Dependencies 是缓存运行时启动时注入的基础能力，不属于静态配置。
type Dependencies struct {
	Writer         storage.ChangeApplier
	WriteAdmission WriteAdmission
}

// Runtime 是有状态缓存 CMod 依赖的稳定 SPI。
// 项目可以包装默认 Manager 扩展对象加载、淘汰和业务索引，也可以为高级场景提供完整实现。
type Runtime interface {
	txn.CommitSink
	Start(lifecycle.Tasks) error
	Shutdown(context.Context) error
	Release(txn.LockTarget) error
}

// Config 描述缓存根对象的刷盘调度与资源边界。
type Config struct {
	FlushInterval        time.Duration
	ShutdownFlushTimeout time.Duration
	MaxPendingRoots      int
	MaxRootChanges       int
	HeapSoftLimitBytes   uint64
	HeapSampleInterval   time.Duration
	ReadHeapBytes        func() uint64
}

// Validate 校验缓存管理器完整生命周期所需的配置。
func (config Config) Validate() error {
	if config.FlushInterval <= 0 {
		return errors.New("cache manager: flush interval must be greater than 0")
	}
	if config.ShutdownFlushTimeout <= 0 {
		return errors.New("cache manager: shutdown flush timeout must be greater than 0")
	}
	if config.MaxPendingRoots < 0 {
		return errors.New("cache manager: pending root limit must be greater than or equal to 0")
	}
	if config.MaxRootChanges < 0 {
		return errors.New("cache manager: root change limit must be greater than or equal to 0")
	}
	if config.HeapSampleInterval < 0 {
		return errors.New("cache manager: heap sample interval must be greater than or equal to 0")
	}
	return nil
}

// Manager 维护当前进程内已经接纳的根 Store，以及等待持久化的根对象集合。
// 它不复制业务数据；Store 会继续引用唯一的 GD 根对象。
type Manager struct {
	writer         storage.ChangeApplier
	writeAdmission WriteAdmission
	config         Config

	mu         sync.Mutex
	generation uint64
	entries    map[txn.LockKey]cacheEntry
	pending    map[txn.LockKey]pendingEntry
	reserved   map[txn.LockKey]int

	heapMu              sync.Mutex
	lastHeapSampleAt    time.Time
	lastHeapBytes       uint64
	heapSoftLimitWarned bool
}

var _ Runtime = (*Manager)(nil)

type cacheEntry struct {
	target   txn.LockTarget
	retained bool
}

type pendingEntry struct {
	target     txn.LockTarget
	generation uint64
}

// NewManager 创建根对象缓存与延迟刷盘管理器。
func NewManager(dependencies Dependencies, config Config) *Manager {
	if config.ReadHeapBytes == nil {
		config.ReadHeapBytes = currentHeapBytes
	}
	if isNil(dependencies.WriteAdmission) {
		dependencies.WriteAdmission = nil
	}
	return &Manager{
		writer:         dependencies.Writer,
		writeAdmission: dependencies.WriteAdmission,
		config:         config,
		entries:        make(map[txn.LockKey]cacheEntry),
		pending:        make(map[txn.LockKey]pendingEntry),
		reserved:       make(map[txn.LockKey]int),
	}
}

// Prepare 在 Procedure 修改前校验根对象身份并预留待刷容量。
func (manager *Manager) Prepare(_ context.Context, targets []txn.LockTarget) (txn.CommitReservation, error) {
	if manager == nil {
		return nil, errors.New("cache manager: manager is nil")
	}
	persistent := persistentTargets(targets)
	if len(persistent) == 0 {
		return &reservation{manager: manager}, nil
	}
	if manager.writeAdmission != nil {
		if err := manager.writeAdmission.RequireWritable(); err != nil {
			return nil, fmt.Errorf("cache manager: write admission rejected procedure: %w", err)
		}
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if err := manager.validateTargetsLocked(persistent); err != nil {
		return nil, err
	}
	if !manager.hasCapacityLocked(persistent) {
		return nil, ErrPendingRootCapacity
	}
	if manager.hasNewPendingRootLocked(persistent) {
		manager.warnHeapSoftLimitIfExceeded()
	}
	var result reservation
	result.manager = manager
	if len(persistent) == 1 {
		result.key = persistent[0].Key
	} else {
		result.keys = make([]txn.LockKey, 0, len(persistent))
	}
	for _, target := range persistent {
		entry, exists := manager.entries[target.Key]
		if !exists {
			manager.entries[target.Key] = cacheEntry{target: target}
		} else if !entry.retained {
			entry.target = target
			manager.entries[target.Key] = entry
		}
		manager.reserved[target.Key]++
		if len(persistent) > 1 {
			result.keys = append(result.keys, target.Key)
		}
	}
	return &result, nil
}

type reservation struct {
	once    sync.Once
	manager *Manager
	key     txn.LockKey
	keys    []txn.LockKey
}

func (reservation *reservation) Commit(targets []txn.LockTarget) {
	if reservation == nil || reservation.manager == nil {
		return
	}
	reservation.once.Do(func() {
		reservation.manager.commit(reservation.key, reservation.keys, targets)
	})
}

func (reservation *reservation) Abort() {
	if reservation == nil || reservation.manager == nil {
		return
	}
	reservation.once.Do(func() {
		reservation.manager.abort(reservation.key, reservation.keys)
	})
}

func (manager *Manager) commit(key txn.LockKey, keys []txn.LockKey, targets []txn.LockTarget) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if key.Scope != "" {
		manager.releaseReservationLocked(key)
		for _, target := range targets {
			if target.Key == key && isPersistentTarget(target) {
				manager.markPendingLocked(target)
				break
			}
		}
		manager.dropProvisionalEntryLocked(key)
		return
	}
	allowed := make(map[txn.LockKey]struct{}, len(keys))
	for _, key := range keys {
		allowed[key] = struct{}{}
		manager.releaseReservationLocked(key)
	}
	for _, target := range persistentTargets(targets) {
		if _, ok := allowed[target.Key]; !ok {
			continue
		}
		manager.markPendingLocked(target)
	}
	manager.dropProvisionalEntriesLocked(keys)
}

func (manager *Manager) abort(key txn.LockKey, keys []txn.LockKey) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if key.Scope != "" {
		manager.releaseReservationLocked(key)
		manager.dropProvisionalEntryLocked(key)
		return
	}
	for _, key := range keys {
		manager.releaseReservationLocked(key)
	}
	manager.dropProvisionalEntriesLocked(keys)
}

func (manager *Manager) markPendingLocked(target txn.LockTarget) {
	entry := manager.entries[target.Key]
	entry.target = target
	entry.retained = true
	manager.entries[target.Key] = entry
	manager.generation++
	manager.pending[target.Key] = pendingEntry{target: target, generation: manager.generation}
}

func (manager *Manager) releaseReservationLocked(key txn.LockKey) {
	if manager.reserved[key] <= 1 {
		delete(manager.reserved, key)
		return
	}
	manager.reserved[key]--
}

func (manager *Manager) dropProvisionalEntriesLocked(keys []txn.LockKey) {
	for _, key := range keys {
		manager.dropProvisionalEntryLocked(key)
	}
}

func (manager *Manager) dropProvisionalEntryLocked(key txn.LockKey) {
	entry, exists := manager.entries[key]
	if !exists || entry.retained || manager.reserved[key] > 0 {
		return
	}
	if _, pending := manager.pending[key]; !pending {
		delete(manager.entries, key)
	}
}

// Pending 返回当前等待 Flush 的持久化根对象数量。
func (manager *Manager) Pending() int {
	if manager == nil {
		return 0
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return len(manager.pending)
}

// Release 释放一个已经干净且没有事务预留的缓存根对象。
// 它会取得与 Procedure 相同的实体锁，因此必须在 Procedure 外调用。
func (manager *Manager) Release(target txn.LockTarget) error {
	if manager == nil {
		return errors.New("cache manager: manager is nil")
	}
	if target.Key.Scope == "" || target.Key.Value == "" || isNil(target.Store) {
		return ErrInvalidTarget
	}
	return txn.WithLocks(context.Background(), []txn.LockTarget{target}, func(context.Context) error {
		return manager.releaseLocked(target)
	})
}

func (manager *Manager) releaseLocked(target txn.LockTarget) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.entries[target.Key]
	if !exists {
		return nil
	}
	if !sameStore(entry.target.Store, target.Store) {
		return ErrIdentityConflict
	}
	if manager.reserved[target.Key] > 0 {
		return ErrPendingRoot
	}
	if _, pending := manager.pending[target.Key]; pending {
		return ErrPendingRoot
	}
	if store, ok := target.Store.(storage.ChangeStore); ok && store.HasChanges() {
		return ErrPendingRoot
	}
	delete(manager.entries, target.Key)
	return nil
}

// Flush 依次保存当前待刷根对象。失败对象保留，供后续重试。
func (manager *Manager) Flush(ctx context.Context) error {
	if manager == nil {
		return errors.New("cache manager: manager is nil")
	}
	if manager.writer == nil {
		return storage.ErrNilChangeApplier
	}
	entries := manager.snapshotPending()
	var errs []error
	for _, entry := range entries {
		if err := manager.flushTarget(ctx, entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (manager *Manager) flushTarget(ctx context.Context, entry pendingEntry) error {
	return txn.WithLocks(ctx, []txn.LockTarget{entry.target}, func(ctx context.Context) error {
		store, ok := entry.target.Store.(storage.ChangeStore)
		if !ok || !store.HasChanges() {
			manager.clearPending(entry)
			return nil
		}
		if err := manager.saveStore(ctx, entry.target, store); err != nil {
			return err
		}
		manager.clearPending(entry)
		return nil
	})
}

func (manager *Manager) snapshotPending() []pendingEntry {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entries := make([]pendingEntry, 0, len(manager.pending))
	for _, entry := range manager.pending {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].target.Order != entries[j].target.Order {
			return entries[i].target.Order < entries[j].target.Order
		}
		return entries[i].target.Key.Value < entries[j].target.Key.Value
	})
	return entries
}

func (manager *Manager) clearPending(entry pendingEntry) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	current, exists := manager.pending[entry.target.Key]
	if exists && current.generation == entry.generation {
		delete(manager.pending, entry.target.Key)
	}
}

// Run 按固定间隔持续 Flush，直到 ctx 取消。
func (manager *Manager) Run(ctx context.Context, interval time.Duration) error {
	if manager == nil {
		return errors.New("cache manager: manager is nil")
	}
	if interval <= 0 {
		return errors.New("cache manager: flush interval must be greater than 0")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := manager.Flush(ctx); err != nil {
				txlog.ErrorContext(ctx, "cache manager flush failed", "err", err)
			}
		}
	}
}

// Start 将后台刷盘循环登记到服务生命周期。
func (manager *Manager) Start(ctx lifecycle.Tasks) error {
	if manager == nil {
		return errors.New("cache manager: manager is nil")
	}
	if ctx == nil {
		return errors.New("cache manager: lifecycle context is nil")
	}
	if err := manager.config.Validate(); err != nil {
		return err
	}
	return ctx.Go(func(ctx context.Context) {
		if err := manager.Run(ctx, manager.config.FlushInterval); err != nil {
			txlog.ErrorContext(ctx, "cache manager stopped", "err", err)
		}
	})
}

// Shutdown 在超时范围内反复 Flush，直到待刷集合为空。
func (manager *Manager) Shutdown(ctx context.Context) error {
	if manager == nil {
		return errors.New("cache manager: manager is nil")
	}
	if err := manager.config.Validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, manager.config.ShutdownFlushTimeout)
	defer cancel()
	for {
		err := manager.Flush(ctx)
		if manager.Pending() == 0 {
			return nil
		}
		if err != nil {
			txlog.ErrorContext(ctx, "cache manager final flush failed, retry scheduled", "pending", manager.Pending(), "err", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cache manager: final flush stopped with %d pending roots: %w", manager.Pending(), ctx.Err())
		case <-time.After(manager.config.FlushInterval):
		}
	}
}

func (manager *Manager) validateTargetsLocked(targets []txn.LockTarget) error {
	for _, target := range targets {
		if entry, exists := manager.entries[target.Key]; exists && !sameStore(entry.target.Store, target.Store) {
			return fmt.Errorf("%w: %s", ErrIdentityConflict, target.Key.String())
		}
		if manager.config.MaxRootChanges > 0 {
			if _, ok := target.Store.(storage.CollapsibleChangeStore); !ok {
				return fmt.Errorf("%w: %s", ErrChangeCollapseUnsupported, target.Key.String())
			}
		}
	}
	return nil
}

func (manager *Manager) hasCapacityLocked(targets []txn.LockTarget) bool {
	if manager.config.MaxPendingRoots <= 0 {
		return true
	}
	occupied := len(manager.pending)
	for key := range manager.reserved {
		if _, pending := manager.pending[key]; !pending {
			occupied++
		}
	}
	additional := 0
	for _, target := range targets {
		if _, pending := manager.pending[target.Key]; pending || manager.reserved[target.Key] > 0 {
			continue
		}
		additional++
	}
	return occupied+additional <= manager.config.MaxPendingRoots
}

func (manager *Manager) hasNewPendingRootLocked(targets []txn.LockTarget) bool {
	for _, target := range targets {
		if _, pending := manager.pending[target.Key]; pending || manager.reserved[target.Key] > 0 {
			continue
		}
		return true
	}
	return false
}

func (manager *Manager) warnHeapSoftLimitIfExceeded() {
	if manager.config.HeapSoftLimitBytes == 0 {
		return
	}
	now := time.Now()
	manager.heapMu.Lock()
	if manager.lastHeapSampleAt.IsZero() || manager.config.HeapSampleInterval <= 0 || now.Sub(manager.lastHeapSampleAt) >= manager.config.HeapSampleInterval {
		manager.lastHeapBytes = manager.config.ReadHeapBytes()
		manager.lastHeapSampleAt = now
	}
	exceeded := manager.lastHeapBytes >= manager.config.HeapSoftLimitBytes
	shouldWarn := exceeded && !manager.heapSoftLimitWarned
	manager.heapSoftLimitWarned = exceeded
	heapBytes := manager.lastHeapBytes
	limit := manager.config.HeapSoftLimitBytes
	manager.heapMu.Unlock()
	if shouldWarn {
		txlog.Warn("cache manager heap soft limit exceeded", "heapBytes", heapBytes, "limitBytes", limit)
	}
}

func currentHeapBytes() uint64 {
	var stats goruntime.MemStats
	goruntime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

var errChangeBudgetExceeded = errors.New("cache manager: change budget exceeded")

type changeBudgetStore struct {
	storage.ChangeStore
	limit int
}

func (store changeBudgetStore) VisitChanges(visitor change.Visitor) error {
	if visitor == nil {
		return change.ErrNilVisitor
	}
	return store.ChangeStore.VisitChanges(&changeBudgetVisitor{visitor: visitor, remaining: store.limit})
}

type changeBudgetVisitor struct {
	visitor   change.Visitor
	remaining int
}

func (visitor *changeBudgetVisitor) VisitChange(path change.Path, op change.Op) error {
	if visitor.remaining == 0 {
		return errChangeBudgetExceeded
	}
	visitor.remaining--
	return visitor.visitor.VisitChange(path, op)
}

func (manager *Manager) saveStore(ctx context.Context, target txn.LockTarget, store storage.ChangeStore) error {
	if manager.config.MaxRootChanges <= 0 {
		return storage.Save(ctx, manager.writer, store)
	}
	err := storage.Save(ctx, manager.writer, changeBudgetStore{ChangeStore: store, limit: manager.config.MaxRootChanges})
	if !errors.Is(err, errChangeBudgetExceeded) {
		return err
	}
	collapser, ok := store.(storage.CollapsibleChangeStore)
	if !ok {
		return fmt.Errorf("%w: %s", ErrChangeCollapseUnsupported, target.Key.String())
	}
	collapser.CollapseChanges()
	txlog.Warn("cache manager root changes collapsed", "scope", target.Key.Scope, "key", target.Key.Value, "budget", manager.config.MaxRootChanges)
	return storage.Save(ctx, manager.writer, store)
}

func persistentTargets(targets []txn.LockTarget) []txn.LockTarget {
	if len(targets) == 0 {
		return nil
	}
	if len(targets) == 1 {
		if isPersistentTarget(targets[0]) {
			return targets
		}
		return nil
	}
	result := make([]txn.LockTarget, 0, len(targets))
	seen := make(map[txn.LockKey]struct{}, len(targets))
	for _, target := range targets {
		if !isPersistentTarget(target) {
			continue
		}
		if _, exists := seen[target.Key]; exists {
			continue
		}
		seen[target.Key] = struct{}{}
		result = append(result, target)
	}
	return result
}

func isPersistentTarget(target txn.LockTarget) bool {
	store, ok := target.Store.(storage.ChangeStore)
	return ok && store.TypeSpec().Storage.Mode != "" && target.Key.Scope != "" && target.Key.Value != ""
}

func sameStore(left, right change.UndoBindable) bool {
	if isNil(left) || isNil(right) {
		return left == nil && right == nil
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if leftValue.Type() != rightValue.Type() || !leftValue.Type().Comparable() {
		return false
	}
	return leftValue.Interface() == rightValue.Interface()
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
