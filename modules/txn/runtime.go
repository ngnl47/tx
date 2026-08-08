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

var ErrPendingRootCapacity = errors.New("txn runtime: pending root capacity reached")

// Admission 在开始新的 Procedure 前检查项目是否接受新的可持久化修改。
// 它通常由项目侧的共享存储健康闸门实现；已有 pending 根不会经过该检查。
type Admission interface {
	RequireWritable() error
}

// RuntimeConfig 描述 Procedure 运行时的调度与资源边界。
type RuntimeConfig struct {
	// FlushInterval 是后台合并刷盘的间隔。通过 Start 启动时必须大于零。
	FlushInterval time.Duration
	// ShutdownFlushTimeout 限制服务关闭时的最终刷盘时间。通过 Shutdown 执行时必须大于零。
	ShutdownFlushTimeout time.Duration
	// Admission 在新的 Procedure 开始前执行。nil 表示不额外限制。
	Admission Admission
	// MaxPendingRoots 限制等待 Flush 的持久化根对象数。零表示不限制。
	MaxPendingRoots int
	// MaxRootDeltaEntries 限制单个持久化根对象的细粒度 Delta 数量。超过后，
	// Runtime 会将该根 Store 收敛为顶层字段 Replace，零表示不限制。
	MaxRootDeltaEntries int
	// PendingMemorySoftLimit 是进程 Heap 告警阈值（字节）。达到上限时，
	// Runtime 仅记录告警，不拒绝业务变更。零表示不观测。
	PendingMemorySoftLimit uint64
	// MemorySampleInterval 是 Heap 采样最小间隔。零表示每次新增根预检时采样。
	MemorySampleInterval time.Duration
	// HeapBytes 用于读取当前进程 Heap 占用。默认读取 runtime.MemStats.HeapAlloc。
	// 项目测试或自定义运行时观测可替换该函数。
	HeapBytes func() uint64
}

// Runtime 聚合当前进程内需要落盘的实体 Store。
//
// 它不保存业务数据副本，也不把 DeltaSet 作为异步消息传递。Flush 会取得与
// Procedure 相同的本机锁，在锁内从 Store 读取当前值并调用 storage.Save。
type Runtime struct {
	writer storage.DeltaApplier
	config RuntimeConfig

	mu                    sync.Mutex
	generation            uint64
	targets               map[LockKey]targetEntry
	reserved              map[LockKey]int
	maxPendingRoots       int
	maxRootDeltas         int
	memorySoftLimit       uint64
	memorySampleInterval  time.Duration
	heapBytes             func() uint64
	memoryMu              sync.Mutex
	lastMemoryAt          time.Time
	lastHeapBytes         uint64
	memorySoftLimitWarned bool
	running               atomic.Bool
}

type targetEntry struct {
	target     LockTarget
	generation uint64
}

// NewRuntime 创建 Procedure 的标准运行时。writer 通常是已装配的 Fusion 写入器。
// 未提供配置时使用零值配置，适合不启动后台循环的单元测试或手动 Flush 场景。
func NewRuntime(writer storage.DeltaApplier, configs ...RuntimeConfig) *Runtime {
	var options RuntimeConfig
	if len(configs) > 0 {
		options = configs[0]
	}
	heapBytes := options.HeapBytes
	if heapBytes == nil {
		heapBytes = currentHeapBytes
	}
	return &Runtime{
		writer:               writer,
		config:               options,
		targets:              make(map[LockKey]targetEntry),
		reserved:             make(map[LockKey]int),
		maxPendingRoots:      options.MaxPendingRoots,
		maxRootDeltas:        options.MaxRootDeltaEntries,
		memorySoftLimit:      options.PendingMemorySoftLimit,
		memorySampleInterval: options.MemorySampleInterval,
		heapBytes:            heapBytes,
	}
}

// Call 执行一个单机 Procedure。过程成功后，它只登记本次修改的持久化 Store，
// 不在事务提交路径执行 Redis 或 Mongo I/O。
func (manager *Runtime) Call(ctx context.Context, procedure Procedure) error {
	if manager == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if procedure == nil {
		return errors.New("txn runtime: procedure is nil")
	}
	if manager.config.Admission != nil {
		if err := manager.config.Admission.RequireWritable(); err != nil {
			return fmt.Errorf("txn runtime: admission rejected procedure: %w", err)
		}
	}
	managed := &managedProcedure{manager: manager, procedure: procedure}
	err := CallWithOptions(ctx, managed, CallOptions{
		AllowPendingDeltas: true,
	})
	if err != nil {
		managed.releaseReservation()
	}
	return err
}

// MarkDirty 登记可持久化的根 Store。相同锁键在同一 Runtime 内只保留一个目标。
// 纯内存目标不会进入落盘调度。
func (manager *Runtime) MarkDirty(targets ...LockTarget) error {
	if manager == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if err := manager.acceptTargetsLocked(targets); err != nil {
		return err
	}
	manager.markDirtyLocked(targets)
	return nil
}

func (manager *Runtime) markDirtyLocked(targets []LockTarget) {
	for _, target := range targets {
		if !isPersistentTarget(target) {
			continue
		}
		manager.generation++
		manager.targets[target.Key] = targetEntry{target: target, generation: manager.generation}
	}
}

// Pending 返回当前等待 Flush 的持久化实体数量。
func (manager *Runtime) Pending() int {
	if manager == nil {
		return 0
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return len(manager.targets)
}

// Flush 依次落盘当前待处理实体。每个实体在自身本机锁内执行，成功后清理
// Store delta 并解除登记；失败的实体保留，以便下一次 Flush 重试。
func (manager *Runtime) Flush(ctx context.Context) error {
	if manager == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if manager.writer == nil {
		return storage.ErrNilDeltaApplier
	}
	entries := manager.snapshotTargets()
	var errs []error
	for _, entry := range entries {
		if err := manager.flushTarget(ctx, entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Run 按 interval 持续 Flush，直到 ctx 取消。单个 Runtime 同一时刻只能运行一个
// 调度循环；单次 Flush 失败只记录日志并保留待处理 Store，下一轮继续重试。
func (manager *Runtime) Run(ctx context.Context, interval time.Duration) error {
	if manager == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if interval <= 0 {
		return fmt.Errorf("txn runtime: flush interval must be greater than 0")
	}
	if !manager.running.CompareAndSwap(false, true) {
		return errors.New("txn runtime: manager is already running")
	}
	defer manager.running.Store(false)
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
				txlog.ErrorContext(ctx, "txn runtime flush failed", "err", err)
			}
		}
	}
}

// Shutdown 在关闭上下文有效期内反复 Flush，直到所有待落盘 Store 已清空。
// 调用方应传入独立于服务运行上下文取消信号的关闭上下文，以确保服务停止时
// 仍可完成最后一次 Redis 落盘。
func (manager *Runtime) Shutdown(ctx context.Context) error {
	if manager == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	retryInterval := manager.config.FlushInterval
	if retryInterval <= 0 {
		return fmt.Errorf("txn runtime: shutdown retry interval must be greater than 0")
	}
	if manager.config.ShutdownFlushTimeout <= 0 {
		return fmt.Errorf("txn runtime: shutdown flush timeout must be greater than 0")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, manager.config.ShutdownFlushTimeout)
	defer cancel()
	for {
		err := manager.Flush(ctx)
		if manager.Pending() == 0 {
			return nil
		}
		if err != nil {
			txlog.ErrorContext(ctx, "txn runtime final flush failed, retry scheduled", "pending", manager.Pending(), "err", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("txn runtime: final flush stopped with %d pending stores: %w", manager.Pending(), ctx.Err())
		case <-time.After(retryInterval):
		}
	}
}

type waitGroupContext interface {
	context.Context
	WaitGroupAdd(int)
	WaitGroupDone()
}

// Start 将后台刷盘循环绑定到服务的 WaitGroupContext。
func (manager *Runtime) Start(ctx waitGroupContext) error {
	if manager == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if ctx == nil {
		return errors.New("txn runtime: lifecycle context is nil")
	}
	if manager.config.FlushInterval <= 0 {
		return errors.New("txn runtime: flush interval must be greater than 0")
	}
	ctx.WaitGroupAdd(1)
	go func() {
		defer ctx.WaitGroupDone()
		if err := manager.Run(ctx, manager.config.FlushInterval); err != nil {
			txlog.ErrorContext(ctx, "txn runtime stopped", "err", err)
		}
	}()
	return nil
}

type managedProcedure struct {
	manager     *Runtime
	procedure   Procedure
	reservation *pendingReservation
}

func (procedure *managedProcedure) Locks(ctx context.Context) ([]LockTarget, error) {
	targets, err := procedure.procedure.Locks(ctx)
	if err != nil {
		return nil, err
	}
	reservation, err := procedure.manager.reserve(targets)
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
		procedure.manager.commitReservation(procedure.reservation, tx.DirtyTargets())
		procedure.reservation = nil
		return nil
	})
	return procedure.procedure.Process(tx)
}

func (procedure *managedProcedure) releaseReservation() {
	if procedure == nil || procedure.reservation == nil {
		return
	}
	procedure.manager.releaseReservation(procedure.reservation)
	procedure.reservation = nil
}

type pendingReservation struct {
	keys []LockKey
}

func (manager *Runtime) reserve(targets []LockTarget) (*pendingReservation, error) {
	if manager == nil {
		return nil, errors.New("txn runtime: runtime is nil")
	}
	keys := persistentTargetKeys(targets)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if err := manager.acceptTargetsLocked(targets); err != nil {
		return nil, err
	}
	for _, key := range keys {
		manager.reserved[key]++
	}
	return &pendingReservation{keys: keys}, nil
}

func (manager *Runtime) releaseReservation(reservation *pendingReservation) {
	if manager == nil || reservation == nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.releaseReservationLocked(reservation)
}

func (manager *Runtime) commitReservation(reservation *pendingReservation, targets []LockTarget) {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.releaseReservationLocked(reservation)
	manager.markDirtyLocked(targets)
}

func (manager *Runtime) releaseReservationLocked(reservation *pendingReservation) {
	if reservation == nil {
		return
	}
	for _, key := range reservation.keys {
		if manager.reserved[key] <= 1 {
			delete(manager.reserved, key)
			continue
		}
		manager.reserved[key]--
	}
}

func (manager *Runtime) hasCapacityLocked(targets []LockTarget) bool {
	if manager.maxPendingRoots <= 0 {
		return true
	}
	occupied := len(manager.targets)
	for key := range manager.reserved {
		if _, exists := manager.targets[key]; !exists {
			occupied++
		}
	}
	additional := 0
	for _, key := range persistentTargetKeys(targets) {
		if _, exists := manager.targets[key]; exists || manager.reserved[key] > 0 {
			continue
		}
		additional++
	}
	return occupied+additional <= manager.maxPendingRoots
}

func (manager *Runtime) acceptTargetsLocked(targets []LockTarget) error {
	if !manager.hasCapacityLocked(targets) {
		return ErrPendingRootCapacity
	}
	if manager.hasNewPersistentRootLocked(targets) {
		manager.warnMemorySoftLimitIfExceeded()
	}
	return nil
}

func (manager *Runtime) hasNewPersistentRootLocked(targets []LockTarget) bool {
	for _, key := range persistentTargetKeys(targets) {
		if _, exists := manager.targets[key]; exists || manager.reserved[key] > 0 {
			continue
		}
		return true
	}
	return false
}

func (manager *Runtime) warnMemorySoftLimitIfExceeded() {
	if manager.memorySoftLimit == 0 {
		return
	}
	now := time.Now()
	manager.memoryMu.Lock()
	if manager.lastMemoryAt.IsZero() || manager.memorySampleInterval <= 0 || now.Sub(manager.lastMemoryAt) >= manager.memorySampleInterval {
		manager.lastHeapBytes = manager.heapBytes()
		manager.lastMemoryAt = now
	}
	exceeded := manager.lastHeapBytes >= manager.memorySoftLimit
	shouldWarn := exceeded && !manager.memorySoftLimitWarned
	manager.memorySoftLimitWarned = exceeded
	heapBytes := manager.lastHeapBytes
	limit := manager.memorySoftLimit
	manager.memoryMu.Unlock()
	if shouldWarn {
		txlog.Warn("txn runtime pending memory threshold exceeded", "heapBytes", heapBytes, "threshold", limit)
	}
}

func currentHeapBytes() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

type deltaCollapser interface {
	CollapseDeltas()
}

func (manager *Runtime) enforceDeltaBudget(target LockTarget, store storage.DeltaStore, deltas change.DeltaSet) change.DeltaSet {
	if manager == nil || manager.maxRootDeltas <= 0 || len(deltas) <= manager.maxRootDeltas {
		return deltas
	}
	collapser, ok := target.Store.(deltaCollapser)
	if !ok {
		return deltas
	}
	collapser.CollapseDeltas()
	txlog.Warn("txn runtime root deltas collapsed", "scope", target.Key.Scope, "key", target.Key.Value, "budget", manager.maxRootDeltas)
	return store.Deltas()
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
	_, ok := target.Store.(storage.DeltaStore)
	return ok && target.Key.Scope != "" && target.Key.Value != ""
}

func (manager *Runtime) flushTarget(ctx context.Context, entry targetEntry) error {
	target := entry.target
	return WithLocks(ctx, []LockTarget{target}, func(ctx context.Context) error {
		store, ok := target.Store.(storage.DeltaStore)
		if !ok {
			manager.forget(entry)
			return nil
		}
		deltas := manager.enforceDeltaBudget(target, store, store.Deltas())
		if len(deltas) == 0 {
			manager.forget(entry)
			return nil
		}
		if err := storage.SaveDeltas(ctx, manager.writer, store, deltas); err != nil {
			return err
		}
		manager.forget(entry)
		return nil
	})
}

func (manager *Runtime) snapshotTargets() []targetEntry {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	targets := make([]targetEntry, 0, len(manager.targets))
	for _, entry := range manager.targets {
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

func (manager *Runtime) forget(entry targetEntry) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if current, ok := manager.targets[entry.target.Key]; ok && current.generation == entry.generation {
		delete(manager.targets, entry.target.Key)
	}
}
