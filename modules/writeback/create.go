package writeback

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

// ErrRecordExists 表示首次写入遇到已有记录，包括上次结果未知后可能已成功的记录。
// 不将其视为成功，也不以覆盖写入解决冲突。
var ErrRecordExists = errors.New("writeback: created record already exists")

// CreationCache 在所属唯一缓存内保留新对象，提交前不得向其他调用者发布。
type CreationCache interface {
	ReserveCreate(context.Context, any) (txn.CreationReservation, error)
}

// BindCreationResolver 绑定按生成描述定位实体的统一入口，不再要求应用逐锁域登记。
func (manager *Manager) BindCreationResolver(cache CreationCache) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.creationResolver != nil {
		return errors.New("writeback: creation resolver is already bound")
	}
	manager.creationResolver = cache
	return nil
}

// PrepareCreate 在事务持锁期间预留写回容量并取得唯一缓存占位。
func (manager *Manager) PrepareCreate(ctx context.Context, target txn.StoreTarget, value any) (txn.CreationReservation, error) {
	store, ok := target.Store.(storage.ChangeStore)
	if !ok || !isPersistentTarget(target) {
		return nil, ErrInvalidTarget
	}
	if _, ok := manager.writer.(storage.DocumentBackfiller); !ok {
		return nil, errors.New("writeback: conditional creation writer is not configured")
	}
	manager.mu.Lock()
	cache := manager.creationResolver
	entry := manager.entries[target.Key]
	manager.mu.Unlock()
	if cache == nil {
		return nil, fmt.Errorf("writeback: creation cache is not bound: %s", target.Key.Type)
	}
	if entry.retained {
		return nil, ErrIdentityConflict
	}
	if err := storage.ValidateRecordKey(store.TypeSpec(), store.RecordKey()); err != nil {
		return nil, err
	}
	pending, err := manager.Prepare(ctx, []txn.StoreTarget{target})
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			pending.Abort()
		}
	}()
	reservation, err := cache.ReserveCreate(ctx, value)
	if err != nil {
		return nil, err
	}
	if isNil(reservation) {
		return nil, errors.New("writeback: nil cache reservation")
	}
	success = true
	return &createReservation{manager: manager, target: target, store: store, key: store.RecordKey(), cache: reservation, pending: pending}, nil
}

type createReservation struct {
	manager *Manager
	target  txn.StoreTarget
	store   storage.ChangeStore
	key     storage.RecordKey
	cache   txn.CreationReservation
	pending txn.CommitReservation
}

func (r *createReservation) Validate() error {
	if !reflect.DeepEqual(r.key, r.store.RecordKey()) {
		return errors.New("writeback: created record key changed")
	}
	return r.cache.Validate()
}
func (r *createReservation) Commit() {
	r.manager.mu.Lock()
	r.manager.markPendingLocked(r.target)
	entry := r.manager.entries[r.target.Key]
	entry.creating = true
	entry.createKey = r.key
	r.manager.entries[r.target.Key] = entry
	r.manager.mu.Unlock()
	r.pending.Commit([]txn.StoreTarget{r.target})
	r.cache.Commit()
}
func (r *createReservation) Abort() { r.cache.Abort(); r.pending.Abort() }

func (manager *Manager) insertStore(ctx context.Context, store storage.ChangeStore) error {
	store.AllTouch()
	doc, err := change.BuildDocument(store, store.FieldNameResolver(), change.FieldNameJSON, store)
	if err != nil {
		return err
	}
	inserted, err := manager.writer.(storage.DocumentBackfiller).BackfillDocumentWithOptions(ctx, store.TypeSpec(), store.RecordKey(), doc.Set, store.DefaultApplyOptions())
	if err != nil {
		return err
	}
	if !inserted {
		return ErrRecordExists
	}
	store.ClearChanges()
	return nil
}
