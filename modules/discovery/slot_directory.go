package discovery

import (
	"context"
	"sync"
)

// SlotDirectory 保存由服务发现 Watch 维护的逻辑槽位内存快照。
// ResolveSlot 只读本地内存，不会回源持久 SlotStore。
type SlotDirectory struct {
	mu          sync.RWMutex
	ready       chan struct{}
	readyOnce   sync.Once
	available   bool
	assignments map[string]SlotAssignment
}

func NewSlotDirectory() *SlotDirectory {
	return &SlotDirectory{
		ready:       make(chan struct{}),
		assignments: make(map[string]SlotAssignment),
	}
}

// Ready 在首份完整槽位快照生效后关闭。
func (d *SlotDirectory) Ready() <-chan struct{} {
	if d == nil {
		return nil
	}
	return d.ready
}

// Replace 原子替换当前槽位快照。空快照仍表示目录已经完成同步并且可用。
func (d *SlotDirectory) Replace(assignments []SlotAssignment) error {
	if d == nil {
		return ErrSlotDirectoryNil
	}
	next := make(map[string]SlotAssignment, len(assignments))
	for _, assignment := range assignments {
		if err := assignment.Validate(); err != nil {
			return err
		}
		if _, exists := next[assignment.SlotID]; exists {
			return ErrInvalidSlotAssignment
		}
		next[assignment.SlotID] = assignment
	}
	d.mu.Lock()
	d.assignments = next
	d.available = true
	d.mu.Unlock()
	d.readyOnce.Do(func() { close(d.ready) })
	return nil
}

// Invalidate 使目录立即失败关闭，并丢弃可能已经过期的槽位归属。
func (d *SlotDirectory) Invalidate() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.assignments = make(map[string]SlotAssignment)
	d.available = false
	d.mu.Unlock()
}

func (d *SlotDirectory) resolve(slotID string) (SlotAssignment, error) {
	if d == nil {
		return SlotAssignment{}, ErrSlotDirectoryNil
	}
	if !validSlotID(slotID) {
		return SlotAssignment{}, ErrInvalidSlotAssignment
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if !d.available {
		return SlotAssignment{}, ErrSlotDirectoryUnavailable
	}
	assignment, exists := d.assignments[slotID]
	if !exists {
		return SlotAssignment{}, ErrSlotNotFound
	}
	return assignment, nil
}

// CachedSlotStore 将持久写入与 Watch 内存读取组合为 SlotStore。
// CreateSlot 和 PromoteSlot 成功后，新的归属由 etcd Watch 异步进入目录。
type CachedSlotStore struct {
	store     SlotStore
	directory *SlotDirectory
}

func NewCachedSlotStore(store SlotStore, directory *SlotDirectory) (*CachedSlotStore, error) {
	if store == nil {
		return nil, ErrSlotStoreNil
	}
	if directory == nil {
		return nil, ErrSlotDirectoryNil
	}
	return &CachedSlotStore{store: store, directory: directory}, nil
}

func (s *CachedSlotStore) ResolveSlot(_ context.Context, slotID string) (SlotAssignment, error) {
	if s == nil || s.store == nil {
		return SlotAssignment{}, ErrSlotStoreNil
	}
	return s.directory.resolve(slotID)
}

func (s *CachedSlotStore) CreateSlot(ctx context.Context, assignment SlotAssignment) error {
	if s == nil || s.store == nil {
		return ErrSlotStoreNil
	}
	return s.store.CreateSlot(ctx, assignment)
}

func (s *CachedSlotStore) PromoteSlot(ctx context.Context, current, next SlotAssignment) error {
	if s == nil || s.store == nil {
		return ErrSlotStoreNil
	}
	return s.store.PromoteSlot(ctx, current, next)
}

var _ SlotStore = (*CachedSlotStore)(nil)
