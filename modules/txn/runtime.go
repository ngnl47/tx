package txn

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngnl47/tx/change"
	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/storage"
)

var (
	ErrPendingRootCapacity       = errors.New("txn runtime: pending root capacity reached")
	ErrChangeCollapseUnsupported = errors.New("txn runtime: persistent store does not support change collapse")
)

// WriteAdmission 在开始新的 Procedure 前检查项目是否接受新的可持久化修改。
// 它通常由项目侧的共享存储健康闸门实现；已有 pending 根不会经过该检查。
type WriteAdmission interface {
	RequireWritable() error
}

// RuntimeConfig 描述 Procedure 运行时的调度与资源边界。
type RuntimeConfig struct {
	// FlushInterval 是后台合并刷盘的间隔。通过 Start 启动时必须大于零。
	FlushInterval time.Duration
	// ShutdownFlushTimeout 限制服务关闭时的最终刷盘时间。通过 Shutdown 执行时必须大于零。
	ShutdownFlushTimeout time.Duration
	// WriteAdmission 在新的 Procedure 开始前执行。nil 表示不额外限制。
	WriteAdmission WriteAdmission
	// MaxPendingRoots 限制等待 Flush 的持久化根对象数。零表示不限制。
	MaxPendingRoots int
	// MaxRootChanges 限制单个持久化根对象的细粒度变更数量。超过后，
	// Runtime 会将该根 Store 收敛为顶层字段 Replace，零表示不限制。
	MaxRootChanges int
	// HeapSoftLimitBytes 是进程 Heap 告警阈值（字节）。达到上限时，
	// Runtime 仅记录告警，不拒绝业务变更。零表示不观测。
	HeapSoftLimitBytes uint64
	// HeapSampleInterval 是 Heap 采样最小间隔。零表示每次新增根预检时采样。
	HeapSampleInterval time.Duration
	// ReadHeapBytes 用于读取当前进程 Heap 占用。默认读取 runtime.MemStats.HeapAlloc。
	// 项目测试或自定义运行时观测可替换该函数。
	ReadHeapBytes func() uint64
}

// Runtime 聚合当前进程内需要落盘的实体 Store。
//
// 它不保存业务数据副本，也不把 Store 作为异步消息传递。Flush 会取得与
// Procedure 相同的本机锁，在锁内从 Store 读取当前值并调用 storage.Save。
type Runtime struct {
	writer storage.ChangeApplier
	config RuntimeConfig

	mu                  sync.Mutex
	generation          uint64
	targets             map[LockKey]targetEntry
	reserved            map[LockKey]int
	maxPendingRoots     int
	maxRootChanges      int
	heapSoftLimitBytes  uint64
	heapSampleInterval  time.Duration
	readHeapBytes       func() uint64
	heapMu              sync.Mutex
	lastHeapSampleAt    time.Time
	lastHeapBytes       uint64
	heapSoftLimitWarned bool
	running             atomic.Bool
}

// Executor 是面向应用的事务契约，调度、刷盘和关闭仍由进程 runtime 负责。
type Executor interface {
	Call(context.Context, Procedure) error
	MarkDirty(...LockTarget) error
	Pending() int
}

var _ Executor = (*Runtime)(nil)

type targetEntry struct {
	target     LockTarget
	generation uint64
}

// NewRuntime 创建 Procedure 的标准运行时。writer 通常是已装配的 Fusion 写入器。
// 未提供配置时使用零值配置，适合不启动后台循环的单元测试或手动 Flush 场景。
func NewRuntime(writer storage.ChangeApplier, configs ...RuntimeConfig) *Runtime {
	var options RuntimeConfig
	if len(configs) > 0 {
		options = configs[0]
	}
	readHeapBytes := options.ReadHeapBytes
	if readHeapBytes == nil {
		readHeapBytes = currentHeapBytes
	}
	return &Runtime{
		writer:             writer,
		config:             options,
		targets:            make(map[LockKey]targetEntry),
		reserved:           make(map[LockKey]int),
		maxPendingRoots:    options.MaxPendingRoots,
		maxRootChanges:     options.MaxRootChanges,
		heapSoftLimitBytes: options.HeapSoftLimitBytes,
		heapSampleInterval: options.HeapSampleInterval,
		readHeapBytes:      readHeapBytes,
	}
}

// Call 执行一个单机 Procedure。过程成功后，它只登记本次修改的持久化 Store，
// 不在事务提交路径执行 Redis 或 Mongo I/O。
func (runtime *Runtime) Call(ctx context.Context, procedure Procedure) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if procedure == nil {
		return errors.New("txn runtime: procedure is nil")
	}
	if runtime.config.WriteAdmission != nil {
		if err := runtime.config.WriteAdmission.RequireWritable(); err != nil {
			return fmt.Errorf("txn runtime: write admission rejected procedure: %w", err)
		}
	}
	managed := &managedProcedure{runtime: runtime, procedure: procedure}
	err := CallWithOptions(ctx, managed, CallOptions{
		AllowPendingChanges: true,
	})
	if err != nil {
		managed.releaseReservation()
	}
	return err
}

// MarkDirty 登记可持久化的根 Store。相同锁键在同一 Runtime 内只保留一个目标。
// 纯内存目标不会进入落盘调度。
func (runtime *Runtime) MarkDirty(targets ...LockTarget) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if err := runtime.acceptTargetsLocked(targets); err != nil {
		return err
	}
	runtime.markDirtyLocked(targets)
	return nil
}

func (runtime *Runtime) markDirtyLocked(targets []LockTarget) {
	for _, target := range targets {
		if !isPersistentTarget(target) {
			continue
		}
		runtime.generation++
		runtime.targets[target.Key] = targetEntry{target: target, generation: runtime.generation}
	}
}

// Pending 返回当前等待 Flush 的持久化实体数量。
func (runtime *Runtime) Pending() int {
	if runtime == nil {
		return 0
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return len(runtime.targets)
}

// Flush 依次落盘当前待处理实体。每个实体在自身本机锁内执行，成功后清理
// Store 变更并解除登记；失败的实体保留，以便下一次 Flush 重试。
func (runtime *Runtime) Flush(ctx context.Context) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if runtime.writer == nil {
		return storage.ErrNilChangeApplier
	}
	entries := runtime.snapshotTargets()
	var errs []error
	for _, entry := range entries {
		if err := runtime.flushTarget(ctx, entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Run 按 interval 持续 Flush，直到 ctx 取消。单个 Runtime 同一时刻只能运行一个
// 调度循环；单次 Flush 失败只记录日志并保留待处理 Store，下一轮继续重试。
func (runtime *Runtime) Run(ctx context.Context, interval time.Duration) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if interval <= 0 {
		return fmt.Errorf("txn runtime: flush interval must be greater than 0")
	}
	if !runtime.running.CompareAndSwap(false, true) {
		return errors.New("txn runtime: runtime is already running")
	}
	defer runtime.running.Store(false)
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
			if err := runtime.Flush(ctx); err != nil {
				txlog.ErrorContext(ctx, "txn runtime flush failed", "err", err)
			}
		}
	}
}

// Shutdown 在关闭上下文有效期内反复 Flush，直到所有待落盘 Store 已清空。
// 调用方应传入独立于服务运行上下文取消信号的关闭上下文，以确保服务停止时
// 仍可完成最后一次 Redis 落盘。
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	retryInterval := runtime.config.FlushInterval
	if retryInterval <= 0 {
		return fmt.Errorf("txn runtime: shutdown retry interval must be greater than 0")
	}
	if runtime.config.ShutdownFlushTimeout <= 0 {
		return fmt.Errorf("txn runtime: shutdown flush timeout must be greater than 0")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, runtime.config.ShutdownFlushTimeout)
	defer cancel()
	for {
		err := runtime.Flush(ctx)
		if runtime.Pending() == 0 {
			return nil
		}
		if err != nil {
			txlog.ErrorContext(ctx, "txn runtime final flush failed, retry scheduled", "pending", runtime.Pending(), "err", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("txn runtime: final flush stopped with %d pending stores: %w", runtime.Pending(), ctx.Err())
		case <-time.After(retryInterval):
		}
	}
}

type waitGroupContext interface {
	context.Context
	Go(func(context.Context)) error
}

// Start 将后台刷盘循环绑定到服务的 WaitGroupContext。
func (runtime *Runtime) Start(ctx waitGroupContext) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if ctx == nil {
		return errors.New("txn runtime: lifecycle context is nil")
	}
	if runtime.config.FlushInterval <= 0 {
		return errors.New("txn runtime: flush interval must be greater than 0")
	}
	return ctx.Go(func(context.Context) {
		if err := runtime.Run(ctx, runtime.config.FlushInterval); err != nil {
			txlog.ErrorContext(ctx, "txn runtime stopped", "err", err)
		}
	})
}

type managedProcedure struct {
	runtime     *Runtime
	procedure   Procedure
	reservation *pendingReservation
}

func (procedure *managedProcedure) Locks(ctx context.Context) ([]LockTarget, error) {
	targets, err := procedure.procedure.Locks(ctx)
	if err != nil {
		return nil, err
	}
	reservation, err := procedure.runtime.reserve(targets)
	if err != nil {
		return nil, err
	}
	procedure.reservation = reservation
	return targets, nil
}

func (procedure *managedProcedure) Process(tx *Tx) error {
	tx.AfterRollback(func(context.Context) {
		procedure.releaseReservation()
	})
	tx.AfterCommit(func(context.Context) error {
		procedure.runtime.commitReservation(procedure.reservation, tx.DirtyTargets())
		procedure.reservation = nil
		return nil
	})
	return procedure.procedure.Process(tx)
}

func (procedure *managedProcedure) releaseReservation() {
	if procedure == nil || procedure.reservation == nil {
		return
	}
	procedure.runtime.releaseReservation(procedure.reservation)
	procedure.reservation = nil
}

type pendingReservation struct {
	keys []LockKey
}

func (runtime *Runtime) reserve(targets []LockTarget) (*pendingReservation, error) {
	if runtime == nil {
		return nil, errors.New("txn runtime: runtime is nil")
	}
	keys := persistentTargetKeys(targets)
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if err := runtime.acceptTargetsLocked(targets); err != nil {
		return nil, err
	}
	for _, key := range keys {
		runtime.reserved[key]++
	}
	return &pendingReservation{keys: keys}, nil
}

func (runtime *Runtime) releaseReservation(reservation *pendingReservation) {
	if runtime == nil || reservation == nil {
		return
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.releaseReservationLocked(reservation)
}

func (runtime *Runtime) commitReservation(reservation *pendingReservation, targets []LockTarget) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.releaseReservationLocked(reservation)
	runtime.markDirtyLocked(targets)
}

func (runtime *Runtime) releaseReservationLocked(reservation *pendingReservation) {
	if reservation == nil {
		return
	}
	for _, key := range reservation.keys {
		if runtime.reserved[key] <= 1 {
			delete(runtime.reserved, key)
			continue
		}
		runtime.reserved[key]--
	}
}

func (runtime *Runtime) hasCapacityLocked(targets []LockTarget) bool {
	if runtime.maxPendingRoots <= 0 {
		return true
	}
	occupied := len(runtime.targets)
	for key := range runtime.reserved {
		if _, exists := runtime.targets[key]; !exists {
			occupied++
		}
	}
	additional := 0
	for _, key := range persistentTargetKeys(targets) {
		if _, exists := runtime.targets[key]; exists || runtime.reserved[key] > 0 {
			continue
		}
		additional++
	}
	return occupied+additional <= runtime.maxPendingRoots
}

func (runtime *Runtime) acceptTargetsLocked(targets []LockTarget) error {
	if runtime.maxRootChanges > 0 {
		for _, target := range targets {
			if !isPersistentTarget(target) {
				continue
			}
			if _, ok := target.Store.(storage.CollapsibleChangeStore); !ok {
				return fmt.Errorf("%w: scope=%s key=%s", ErrChangeCollapseUnsupported, target.Key.Scope, target.Key.Value)
			}
		}
	}
	if !runtime.hasCapacityLocked(targets) {
		return ErrPendingRootCapacity
	}
	if runtime.hasNewPersistentRootLocked(targets) {
		runtime.warnHeapSoftLimitIfExceeded()
	}
	return nil
}

func (runtime *Runtime) hasNewPersistentRootLocked(targets []LockTarget) bool {
	for _, key := range persistentTargetKeys(targets) {
		if _, exists := runtime.targets[key]; exists || runtime.reserved[key] > 0 {
			continue
		}
		return true
	}
	return false
}

func (runtime *Runtime) warnHeapSoftLimitIfExceeded() {
	if runtime.heapSoftLimitBytes == 0 {
		return
	}
	now := time.Now()
	runtime.heapMu.Lock()
	if runtime.lastHeapSampleAt.IsZero() || runtime.heapSampleInterval <= 0 || now.Sub(runtime.lastHeapSampleAt) >= runtime.heapSampleInterval {
		runtime.lastHeapBytes = runtime.readHeapBytes()
		runtime.lastHeapSampleAt = now
	}
	exceeded := runtime.lastHeapBytes >= runtime.heapSoftLimitBytes
	shouldWarn := exceeded && !runtime.heapSoftLimitWarned
	runtime.heapSoftLimitWarned = exceeded
	heapBytes := runtime.lastHeapBytes
	limit := runtime.heapSoftLimitBytes
	runtime.heapMu.Unlock()
	if shouldWarn {
		txlog.Warn("txn runtime heap soft limit exceeded", "heapBytes", heapBytes, "limitBytes", limit)
	}
}

func currentHeapBytes() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

var errChangeBudgetExceeded = errors.New("txn runtime: change budget exceeded")

type changeBudgetStore struct {
	storage.ChangeStore
	limit int
}

func (store changeBudgetStore) VisitChanges(visitor change.Visitor) error {
	if visitor == nil {
		return change.ErrNilVisitor
	}
	return store.ChangeStore.VisitChanges(&changeBudgetVisitor{
		visitor:   visitor,
		remaining: store.limit,
	})
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

func (runtime *Runtime) saveStore(ctx context.Context, target LockTarget, store storage.ChangeStore) error {
	if runtime == nil || runtime.maxRootChanges <= 0 {
		return storage.Save(ctx, runtime.writer, store)
	}
	err := storage.Save(ctx, runtime.writer, changeBudgetStore{
		ChangeStore: store,
		limit:       runtime.maxRootChanges,
	})
	if !errors.Is(err, errChangeBudgetExceeded) {
		return err
	}
	collapser, ok := store.(storage.CollapsibleChangeStore)
	if !ok {
		return fmt.Errorf("%w: scope=%s key=%s", ErrChangeCollapseUnsupported, target.Key.Scope, target.Key.Value)
	}
	collapser.CollapseChanges()
	txlog.Warn("txn runtime root changes collapsed", "scope", target.Key.Scope, "key", target.Key.Value, "budget", runtime.maxRootChanges)
	return storage.Save(ctx, runtime.writer, store)
}

func persistentTargetKeys(targets []LockTarget) []LockKey {
	keys := make([]LockKey, 0, len(targets))
	seen := make(map[LockKey]struct{}, len(targets))
	for _, target := range targets {
		if !isPersistentTarget(target) {
			continue
		}
		if _, exists := seen[target.Key]; exists {
			continue
		}
		seen[target.Key] = struct{}{}
		keys = append(keys, target.Key)
	}
	return keys
}

func isPersistentTarget(target LockTarget) bool {
	_, ok := target.Store.(storage.ChangeStore)
	return ok && target.Key.Scope != "" && target.Key.Value != ""
}

func (runtime *Runtime) flushTarget(ctx context.Context, entry targetEntry) error {
	target := entry.target
	return WithLocks(ctx, []LockTarget{target}, func(ctx context.Context) error {
		store, ok := target.Store.(storage.ChangeStore)
		if !ok {
			runtime.forget(entry)
			return nil
		}
		if !store.HasChanges() {
			runtime.forget(entry)
			return nil
		}
		if err := runtime.saveStore(ctx, target, store); err != nil {
			return err
		}
		runtime.forget(entry)
		return nil
	})
}

func (runtime *Runtime) snapshotTargets() []targetEntry {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	targets := make([]targetEntry, 0, len(runtime.targets))
	for _, entry := range runtime.targets {
		targets = append(targets, entry)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].target.Order != targets[j].target.Order {
			return targets[i].target.Order < targets[j].target.Order
		}
		if targets[i].target.Key.Scope != targets[j].target.Key.Scope {
			return targets[i].target.Key.Scope < targets[j].target.Key.Scope
		}
		return targets[i].target.Key.Value < targets[j].target.Key.Value
	})
	return targets
}

func (runtime *Runtime) forget(entry targetEntry) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if current, ok := runtime.targets[entry.target.Key]; ok && current.generation == entry.generation {
		delete(runtime.targets, entry.target.Key)
	}
}
