package txn

import (
	"context"
	"errors"
)

// RemovalParticipant 预留删除；提交仅登记内存状态，持久化由写回运行时负责。
type RemovalParticipant interface {
	PrepareRemove(context.Context, LockTarget, any) (CreationReservation, error)
}
type removal struct {
	target      LockTarget
	reservation CreationReservation
}

// Remove 在 Procedure 中登记删除，回滚保留原对象，提交后不再可查询。
func Remove(ctx context.Context, value any) error {
	tx, active := FromContext(ctx)
	if !active {
		return ErrInactive
	}
	v, ok := value.(Lockable)
	if !ok {
		return errors.New("transaction: deletion requires a Lockable root")
	}
	if tx.remover == nil {
		return errors.New("transaction: removal participant is not bound")
	}
	return tx.Call(removeProcedure{v, value})
}

type removeProcedure struct {
	value    Lockable
	original any
}

func (p removeProcedure) Locks(context.Context) ([]LockTarget, error) { return Locks(p.value), nil }
func (p removeProcedure) Process(tx *Tx) error {
	target := p.value.LockTarget()
	for _, r := range tx.removals {
		if r.target.Key == target.Key {
			return nil
		}
	}
	reservation, err := tx.remover.PrepareRemove(tx.ctx, target, p.original)
	if err != nil {
		return err
	}
	tx.removals = append(tx.removals, removal{target, reservation})
	return nil
}
