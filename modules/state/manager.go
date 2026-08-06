// Package state 提供可选的有状态实体脏数据调度。
package state

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

// Manager 聚合当前进程内需要落盘的实体 Store。
//
// 它不保存业务数据副本，也不把 DeltaSet 作为异步消息传递。Flush 会取得与
// Procedure 相同的本机锁，在锁内从 Store 读取当前值并调用 storage.Save。
type Manager struct {
	writer storage.DeltaApplier

	mu         sync.Mutex
	generation uint64
	targets    map[txn.LockKey]targetEntry
	running    atomic.Bool
}

type targetEntry struct {
	target     txn.LockTarget
	generation uint64
}

// NewManager 创建一个状态落盘调度器。writer 通常是已装配的 Fusion strategy。
func NewManager(writer storage.DeltaApplier) *Manager {
	return &Manager{
		writer:  writer,
		targets: make(map[txn.LockKey]targetEntry),
	}
}

// Call 执行一个单机 Procedure。过程成功后，它只登记本次修改的持久化 Store，
// 不在事务提交路径执行 Redis 或 Mongo I/O。
func (manager *Manager) Call(ctx context.Context, procedure txn.Procedure) error {
	if manager == nil {
		return errors.New("state: manager is nil")
	}
	if procedure == nil {
		return errors.New("state: procedure is nil")
	}
	return txn.CallWithOptions(ctx, managedProcedure{manager: manager, procedure: procedure}, txn.CallOptions{
		AllowPendingDeltas: true,
	})
}

// MarkDirty 登记可持久化的根 Store。相同锁键在同一 Manager 内只保留一个目标。
// 纯内存目标不会进入落盘调度。
func (manager *Manager) MarkDirty(targets ...txn.LockTarget) {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, target := range targets {
		if _, ok := target.Store.(storage.DeltaStore); !ok || target.Key.Scope == "" || target.Key.Value == "" {
			continue
		}
		manager.generation++
		manager.targets[target.Key] = targetEntry{target: target, generation: manager.generation}
	}
}

// Pending 返回当前等待 Flush 的持久化实体数量。
func (manager *Manager) Pending() int {
	if manager == nil {
		return 0
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return len(manager.targets)
}

// Flush 依次落盘当前待处理实体。每个实体在自身本机锁内执行，成功后清理
// Store delta 并解除登记；失败的实体保留，以便下一次 Flush 重试。
func (manager *Manager) Flush(ctx context.Context) error {
	if manager == nil {
		return errors.New("state: manager is nil")
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

// Run 按 interval 持续 Flush，直到 ctx 取消。单个 Manager 同一时刻只能运行一个
// 调度循环；单次 Flush 失败只记录日志并保留待处理 Store，下一轮继续重试。
func (manager *Manager) Run(ctx context.Context, interval time.Duration) error {
	if manager == nil {
		return errors.New("state: manager is nil")
	}
	if interval <= 0 {
		return fmt.Errorf("state: flush interval must be greater than 0")
	}
	if !manager.running.CompareAndSwap(false, true) {
		return errors.New("state: manager is already running")
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
				txlog.ErrorContext(ctx, "state flush failed", "err", err)
			}
		}
	}
}

// Shutdown 在关闭上下文有效期内反复 Flush，直到所有待落盘 Store 已清空。
// 调用方应传入独立于服务运行上下文取消信号的关闭上下文，以确保服务停止时
// 仍可完成最后一次 Redis 落盘。
func (manager *Manager) Shutdown(ctx context.Context, retryInterval time.Duration) error {
	if manager == nil {
		return errors.New("state: manager is nil")
	}
	if retryInterval <= 0 {
		return fmt.Errorf("state: shutdown retry interval must be greater than 0")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		err := manager.Flush(ctx)
		if manager.Pending() == 0 {
			return nil
		}
		if err != nil {
			txlog.ErrorContext(ctx, "state final flush failed, retry scheduled", "pending", manager.Pending(), "err", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("state: final flush stopped with %d pending stores: %w", manager.Pending(), ctx.Err())
		case <-time.After(retryInterval):
		}
	}
}

type managedProcedure struct {
	manager   *Manager
	procedure txn.Procedure
}

func (procedure managedProcedure) Locks(ctx context.Context) ([]txn.LockTarget, error) {
	return procedure.procedure.Locks(ctx)
}

func (procedure managedProcedure) Process(tx *txn.Tx) error {
	tx.AfterCommit(func(context.Context) error {
		procedure.manager.MarkDirty(tx.DirtyTargets()...)
		return nil
	})
	return procedure.procedure.Process(tx)
}

func (manager *Manager) flushTarget(ctx context.Context, entry targetEntry) error {
	target := entry.target
	return txn.WithLocks(ctx, []txn.LockTarget{target}, func(ctx context.Context) error {
		store, ok := target.Store.(storage.DeltaStore)
		if !ok {
			manager.forget(entry)
			return nil
		}
		deltas := store.Deltas()
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

func (manager *Manager) snapshotTargets() []targetEntry {
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

func (manager *Manager) forget(entry targetEntry) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if current, ok := manager.targets[entry.target.Key]; ok && current.generation == entry.generation {
		delete(manager.targets, entry.target.Key)
	}
}
