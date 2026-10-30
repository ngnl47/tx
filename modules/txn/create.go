package txn

import (
	"context"
	"errors"
	"fmt"
)

// ErrInactive 表示操作仍携带已经结束的事务上下文，不能退回普通存储写入。
var ErrInactive = errors.New("transaction: inactive transaction context")

// CreationParticipant 由具备唯一缓存和首次写入能力的提交参与者实现。
type CreationParticipant interface {
	PrepareCreate(context.Context, StoreTarget, any) (CreationReservation, error)
}

// CreationReservation 持有新对象的缓存占位和首次写回预留。
// Validate 在持锁提交前检查身份；Commit 只执行不可失败的内存登记，不能进行 I/O。
type CreationReservation interface {
	Validate() error
	Commit()
	Abort()
}

type creation struct {
	value       StoreProvider
	target      StoreTarget
	reservation CreationReservation
}

// Create 将新根对象登记到当前 Procedure，成功只表示待提交，不立即写入存储。
// 必须通过绑定唯一缓存和 WriteBack 的 Runtime 执行，不支持普通 Run 或结束后的 ctx。
func Create(ctx context.Context, value any) error {
	tx, active := FromContext(ctx)
	if !active {
		return ErrInactive
	}
	lockable, ok := value.(StoreProvider)
	if !ok || isNil(lockable) {
		return errors.New("transaction: creation requires a StoreProvider root")
	}
	if tx.creator == nil {
		return errors.New("transaction: creation participant is not bound")
	}
	return tx.Call(createProcedureCall{value: lockable, original: value})
}

type createProcedureCall struct {
	value    StoreProvider
	original any
}

func (p createProcedureCall) Locks(context.Context) (Lockeys, error) { return Locks(p.value), nil }
func (p createProcedureCall) Process(tx *Tx) error {
	target := p.value.StoreTarget()
	for _, entry := range tx.creations {
		if entry.target.Key == target.Key {
			return fmt.Errorf("transaction: duplicate creation %s", target.Key)
		}
	}
	reservation, err := tx.creator.PrepareCreate(tx.ctx, target, p.original)
	if err != nil {
		return err
	}
	if isNil(reservation) {
		return errors.New("transaction: nil creation reservation")
	}
	if err := tx.enlist(target, true); err != nil {
		reservation.Abort()
		return err
	}
	// 子 savepoint 撤销创建后可能复用同一对象，重新接回 Store 的 undo。
	target.Store.SetUndoRecorder(targetUndoRecorder{tx: tx, target: target})
	tx.creations = append(tx.creations, creation{value: p.value, target: target, reservation: reservation})
	return nil
}

func (tx *Tx) validateCreations() error {
	for _, entry := range tx.creations {
		current := entry.value.StoreTarget()
		if current.Key != entry.target.Key || current.Lock != entry.target.Lock || !sameStore(current.Store, entry.target.Store) {
			return fmt.Errorf("transaction: created root identity changed: %s", entry.target.Key)
		}
		if err := entry.reservation.Validate(); err != nil {
			return err
		}
	}
	return nil
}
