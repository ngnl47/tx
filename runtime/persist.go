package runtime

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

func checkWriteContext(ctx context.Context) error {
	if tx, active := txn.FromContext(ctx); tx != nil {
		if !active {
			return txn.ErrInactive
		}
		return errors.New("runtime: direct persistence inside Procedure is forbidden")
	}
	return nil
}

// Save 仅保存独占加载的对象；共享 GD 必须通过 Procedure 提交给 WriteBack。
func Save[S storage.ChangeStore](ctx context.Context, value storage.StoreProvider[S]) error {
	if err := checkWriteContext(ctx); err != nil {
		return err
	}
	if _, shared := value.(entitycache.Described); shared {
		return entitycache.ErrProcedureRequired
	}
	return storage.Save(ctx, Writer(ctx), value.Store())
}

// Create 在事务内预留对象身份，提交后交给 WriteBack；独占对象仍支持直接持久化。
func Create[S storage.ChangeStore](ctx context.Context, value storage.StoreProvider[S]) error {
	if tx, active := txn.FromContext(ctx); tx != nil {
		if !active {
			return txn.ErrInactive
		}
		return txn.Create(ctx, value)
	}
	if _, shared := value.(entitycache.Described); shared {
		return entitycache.ErrProcedureRequired
	}
	return storage.Create(ctx, Writer(ctx), value.Store())
}

// Remove 在事务内删除共享实体，拒绝用独立副本替代缓存原对象。
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
