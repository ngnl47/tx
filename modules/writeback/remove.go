package writeback

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

// RemovalReservation 保留删除身份，持久化成功后才允许同 ID 重新加载或创建。
type RemovalReservation interface {
	txn.CreationReservation
	Complete()
}
type RemovalCache interface {
	ReserveRemove(context.Context, any) (RemovalReservation, error)
}

// PrepareRemove 复用原对象的写回预留，删除覆盖尚未持久化的创建/修改。
func (m *Manager) PrepareRemove(ctx context.Context, target txn.StoreTarget, value any) (txn.CreationReservation, error) {
	if _, ok := m.writer.(storage.DeleteApplier); !ok {
		return nil, errors.New("writeback: deletion writer is not configured")
	}
	cache, ok := m.creationResolver.(RemovalCache)
	if !ok {
		return nil, errors.New("writeback: removal cache is not bound")
	}
	pending, err := m.Prepare(ctx, []txn.StoreTarget{target})
	if err != nil {
		return nil, err
	}
	r, err := cache.ReserveRemove(ctx, value)
	if err != nil {
		pending.Abort()
		return nil, err
	}
	return &removeReservation{m, target, pending, r}, nil
}

type removeReservation struct {
	manager *Manager
	target  txn.StoreTarget
	pending txn.CommitReservation
	cache   RemovalReservation
}

func (r *removeReservation) Validate() error { return r.cache.Validate() }
func (r *removeReservation) Abort()          { r.cache.Abort(); r.pending.Abort() }
func (r *removeReservation) Commit() {
	r.pending.Commit([]txn.StoreTarget{r.target})
	r.manager.mu.Lock()
	entry := r.manager.entries[r.target.Key]
	entry.removal = r.cache
	r.manager.entries[r.target.Key] = entry
	r.manager.mu.Unlock()
	r.cache.Commit()
}

func (m *Manager) deleteLocked(ctx context.Context, target txn.StoreTarget) error {
	m.mu.Lock()
	root := m.entries[target.Key]
	m.mu.Unlock()
	store := target.Store.(storage.ChangeStore)
	if err := m.writer.(storage.DeleteApplier).ApplyDelete(ctx, store.TypeSpec(), store.RecordKey()); err != nil {
		return err
	}
	store.ClearChanges()
	m.mu.Lock()
	delete(m.pending, target.Key)
	delete(m.entries, target.Key)
	m.mu.Unlock()
	root.removal.Complete()
	return nil
}
