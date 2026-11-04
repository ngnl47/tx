package txn

import (
	"context"
	"errors"
)

// RemovalParticipant 在对应记录锁内预留删除，复用 CreationReservation 的校验与收尾契约。
// PrepareRemove 成功仅登记待提交删除；回滚撤销预留，提交确认内存删除，最终持久化由写回运行时负责。
// 返回错误前须清理本次未交出的预留。
type RemovalParticipant interface {
	PrepareRemove(context.Context, StoreTarget, any) (CreationReservation, error)
}
type removal struct {
	target      StoreTarget
	reservation CreationReservation
}

// Remove 在 Procedure 中登记删除，回滚保留原对象，提交后不再可查询。
func Remove(ctx context.Context, value any) error {
	tx, active := FromContext(ctx)
	if !active {
		return ErrInactive
	}
	v, ok := value.(StoreProvider)
	if !ok {
		return errors.New("transaction: deletion requires a StoreProvider root")
	}
	if tx.remover == nil {
		return errors.New("transaction: removal participant is not bound")
	}
	return tx.Call(removeProcedure{v, value})
}

type removeProcedure struct {
	value    StoreProvider
	original any
}

func (p removeProcedure) Locks(context.Context) (Lockeys, error) { return Locks(p.value), nil }
func (p removeProcedure) Process(tx *Tx) error {
	target := p.value.StoreTarget()
	if err := tx.enlist(target, false); err != nil {
		return err
	}
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
