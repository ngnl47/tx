package txn

import (
	"errors"
	"fmt"
	"strings"
)

// acquire 只取得一致性域锁，不访问或登记 Store。动态补锁沿用等待图死锁检测。
func (tx *Tx) acquire(locks Lockeys, trace string) error {
	prepared, err := prepareLockeys(locks)
	if err != nil {
		return err
	}
	fresh := make(Lockeys, 0, len(prepared))
	for _, lock := range prepared {
		if order, ok := tx.locks[lock.identity()]; ok {
			if order != lock.Order {
				return fmt.Errorf("transaction: conflicting order for %s", lock)
			}
		} else {
			fresh = append(fresh, lock)
		}
	}
	held, err := acquireLocks(tx.ctx, tx.id, fresh, trace)
	if err != nil {
		if errors.Is(err, ErrDeadlock) {
			tx.deadlock = err
		}
		return err
	}
	tx.heldLocks.appendSet(&held)
	for _, lock := range fresh {
		tx.locks[lock.identity()] = lock.Order
	}
	return nil
}

// Enlist 将已加载的唯一对象登记到事务，先复用或取得锁，再预留写回和绑定 undo。
func (tx *Tx) Enlist(target StoreTarget) error { return tx.enlist(target, false) }
func (tx *Tx) enlist(target StoreTarget, creating bool) error {
	if tx == nil || !tx.active {
		return ErrInactive
	}
	if tx.deadlock != nil {
		return tx.deadlock
	}
	if isNil(target.Store) || !target.Key.valid() {
		return fmt.Errorf("transaction: invalid Store target")
	}
	if err := tx.acquire(Lockeys{target.Lock}, strings.Join(tx.procedures, " -> ")); err != nil {
		return err
	}
	if existing, ok := tx.stores[target.Key]; ok {
		if !sameStore(existing, target.Store) {
			return fmt.Errorf("transaction: record %s binds different Store", target.Key)
		}
		return nil
	}
	if err := validateCleanStoreTargets([]StoreTarget{target}, tx.options); err != nil {
		return err
	}
	if tx.hooks.reserveDynamicTargets != nil && !creating {
		if err := tx.hooks.reserveDynamicTargets([]StoreTarget{target}); err != nil {
			return err
		}
	}
	tx.stores[target.Key] = target.Store
	tx.targets = append(tx.targets, target)
	target.Store.SetUndoRecorder(targetUndoRecorder{tx: tx, target: target})
	return nil
}

// OnFinish 在根事务及全部业务回调结束后释放使用权；每轮重试独立执行。
func (tx *Tx) OnFinish(fn func()) {
	if tx == nil || !tx.active {
		panic(ErrInactive)
	}
	tx.finish = append(tx.finish, fn)
}
