package txn

import "context"

// Enlist 将查询取得的唯一对象加入当前事务，复用现有动态锁和写回预留。
func (tx *Tx) Enlist(target LockTarget) error {
	return tx.Call(accessProcedure{target: target})
}

type accessProcedure struct{ target LockTarget }

func (p accessProcedure) Locks(context.Context) ([]LockTarget, error) {
	return []LockTarget{p.target}, nil
}
func (p accessProcedure) Process(*Tx) error { return nil }

// OnFinish 供对象运行时释放使用权；根事务及全部业务回调结束后执行，失败重试每轮独立释放。
func (tx *Tx) OnFinish(fn func()) {
	if tx == nil || !tx.active {
		panic(ErrInactive)
	}
	tx.finish = append(tx.finish, fn)
}
