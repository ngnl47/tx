package runtime

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

// ErrProcedureWrite 防止直接持久化绕过 Procedure 回滚和 WriteBack。
var ErrProcedureWrite = errors.New("runtime: direct persistence inside Procedure is forbidden")

func checkWriteContext(ctx context.Context) error {
	if tx, active := txn.FromContext(ctx); tx != nil {
		if !active {
			return txn.ErrInactive
		}
		return ErrProcedureWrite
	}
	return nil
}

// Save 直接保存未启用 EntityCache 或显式排除缓存的对象，不允许在 Procedure 内调用。
// 共享 GD 的修改由 Procedure 提交给 WriteBack，不通过此入口保存。
// 成功清理 Store 变更，失败保留；成功边界取决于存储模式，Fusion 不等待 Mongo 回写。
func Save[S storage.ChangeStore](ctx context.Context, value storage.StoreProvider[S]) error {
	if err := checkWriteContext(ctx); err != nil {
		return err
	}
	if _, shared := value.(entitycache.Described); shared {
		return entitycache.ErrProcedureRequired
	}
	return storage.Save(ctx, Writer(ctx), value.Store())
}

// Create 在有效 Procedure 内登记创建，成功仅表示预留；根提交后由 WriteBack 条件写入。
// 事务外仅允许非共享对象，走全字段 Upsert，成功清理变更；不保证已有记录被拒绝。
// 两条路径的确认边界不同，均不得把未知写入结果解释为未写入后盲目重投。
func Create[S storage.ChangeStore](ctx context.Context, value storage.StoreProvider[S]) error {
	if tx, active := txn.FromContext(ctx); tx != nil {
		if !active {
			return txn.ErrInactive
		}
		if _, shared := value.(entitycache.Described); shared {
			if _, err := EntityCache(ctx); err != nil {
				return err
			}
		}
		return txn.Create(ctx, value)
	}
	if _, shared := value.(entitycache.Described); shared {
		return entitycache.ErrProcedureRequired
	}
	return storage.Create(ctx, Writer(ctx), value.Store())
}

// Remove 在有效 Procedure 内登记删除，拒绝以独立副本替代共享缓存原对象；
// 此时成功仅表示待提交，回滚保留原对象，根提交后由 WriteBack 持久化删除。
// 事务外仅允许非共享对象，直接调用当前 Writer 的删除能力，成功后清理 Store 变更。
// 直接删除失败保留本地变更标记，不表示后端完全未执行。
func Remove[S storage.ChangeStore](ctx context.Context, value storage.StoreProvider[S]) error {
	if tx, active := txn.FromContext(ctx); tx != nil {
		if !active {
			return txn.ErrInactive
		}
		return txn.Remove(ctx, value)
	}
	if _, shared := value.(entitycache.Described); shared {
		return entitycache.ErrProcedureRequired
	}
	writer, ok := Writer(ctx).(storage.DeleteApplier)
	if !ok {
		return errors.New("runtime: deletion writer is required")
	}
	store := value.Store()
	if err := writer.ApplyDelete(ctx, store.TypeSpec(), store.RecordKey()); err != nil {
		return err
	}
	store.ClearChanges()
	return nil
}
