package discovery

import (
	"context"
	"sync"
)

// SlotDirectory 保存由服务发现 Watch 维护的逻辑槽位内存快照。
// ResolveSlot 只读本地内存，不会回源持久 SlotStore。
type SlotDirectory struct {
	publish        func([]SlotAssignment, int64) error // Runtime 单轮 Watch 写入入口。
	publishChanges func([]slotChange, int64) error
	mu             sync.RWMutex
	ready          chan struct{}
	readyOnce      sync.Once
	available      bool
	assignments    map[string]SlotAssignment
	revisions      map[string]int64
	invalid        map[string]bool
	revision       int64
	changed        chan struct{}
}

// slotChange 仅由已解码并校验的 Watch 事件构造；空 owner 是释放，不能当作删除。
type slotChange struct {
	assignment SlotAssignment
	revision   int64 // 当前事件的版本；同一批事件可能来自多个版本。
	deleted    bool
}

func NewSlotDirectory() *SlotDirectory {
	return &SlotDirectory{
		ready:       make(chan struct{}),
		assignments: make(map[string]SlotAssignment),
		revisions:   make(map[string]int64),
		invalid:     make(map[string]bool),
		changed:     make(chan struct{}),
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
	return d.ReplaceSnapshot(assignments, 0)
}

// ReplaceSnapshot 按权威快照版本更新目录，保留更晚的主动查询结果。
func (d *SlotDirectory) ReplaceSnapshot(assignments []SlotAssignment, revision int64) error {
	if d == nil {
		return ErrSlotDirectoryNil
	}
	if d.publish != nil {
		return d.publish(assignments, revision)
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
	if revision > 0 && revision < d.revision {
		d.mu.Unlock()
		return nil
	}
	for slot, value := range d.assignments {
		if d.revisions[slot] > revision && revision > 0 {
			next[slot] = value
		}
		if next[slot] != value {
			delete(d.invalid, slot)
		}
	}
	for slot := range d.revisions {
		if d.revisions[slot] <= revision {
			delete(d.revisions, slot)
		}
	}
	d.revision = revision
	d.assignments = next
	d.available = true
	d.notifyLocked()
	d.mu.Unlock()
	d.readyOnce.Do(func() { close(d.ready) })
	return nil
}

// applyChanges 在同一次发布中只修改变化项，不遍历未变化的目录或主动查询版本表。
func (d *SlotDirectory) applyChanges(changes []slotChange, revision int64) error {
	if d.publishChanges != nil {
		return d.publishChanges(changes, revision)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.available {
		return ErrSlotDirectoryUnavailable
	}
	if revision < d.revision || len(changes) == 0 {
		return nil
	}
	for _, change := range changes {
		value := change.assignment
		if d.revisions[value.SlotID] > change.revision {
			continue
		}
		if change.deleted {
			delete(d.assignments, value.SlotID)
			delete(d.invalid, value.SlotID)
		} else {
			if d.assignments[value.SlotID] != value {
				delete(d.invalid, value.SlotID)
			}
			d.assignments[value.SlotID] = value
		}
		delete(d.revisions, value.SlotID)
	}
	d.revision = revision
	d.notifyLocked()
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
	clear(d.revisions)
	clear(d.invalid)
	d.notifyLocked()
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
	if d.invalid[slotID] {
		return assignment, ErrSlotRouteStale
	}
	return assignment, nil
}

// CachedSlotStore 提供 Watch 缓存读取和冷路径刷新，不暴露槽位写入。
type CachedSlotStore struct {
	store     SlotReader
	directory *SlotDirectory
}

func NewCachedSlotStore(store SlotReader, directory *SlotDirectory) (*CachedSlotStore, error) {
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

var _ SlotReader = (*CachedSlotStore)(nil)

// invalidateAssignment 只撤销失败发送使用的归属，迟到错误不影响新位置。
func (d *SlotDirectory) invalidateAssignment(observed SlotAssignment) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.available || d.assignments[observed.SlotID] != observed || observed.Generation == 0 {
		return false
	}
	if !d.invalid[observed.SlotID] {
		d.invalid[observed.SlotID] = true
		d.notifyLocked()
	}
	return true
}
func (d *SlotDirectory) mergeRead(value SlotAssignment, revision int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.available {
		return ErrSlotDirectoryUnavailable
	}
	if revision < d.revision || revision < d.revisions[value.SlotID] {
		return nil
	}
	changed := d.assignments[value.SlotID] != value || d.invalid[value.SlotID]
	d.assignments[value.SlotID] = value
	d.revisions[value.SlotID] = revision
	delete(d.invalid, value.SlotID)
	if changed {
		d.notifyLocked()
	}
	return nil
}
func (d *SlotDirectory) notifyLocked() { close(d.changed); d.changed = make(chan struct{}) }
func (d *SlotDirectory) changes() <-chan struct{} {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.changed
}

// refreshSlot 只在失败或冷路径使用，不能通过单键查询恢复整个目录的可用性。
func (s *CachedSlotStore) refreshSlot(ctx context.Context, slot string) (SlotAssignment, error) {
	if _, err := s.directory.resolve(slot); err == ErrSlotDirectoryUnavailable {
		return SlotAssignment{}, err
	}
	reader, ok := s.store.(VersionedSlotReader)
	if !ok {
		return s.store.ResolveSlot(ctx, slot)
	}
	value, revision, err := reader.ResolveSlotAtRevision(ctx, slot)
	if err != nil {
		return SlotAssignment{}, err
	}
	if err := ctx.Err(); err != nil {
		return SlotAssignment{}, err
	}
	if err := s.directory.mergeRead(value, revision); err != nil {
		return SlotAssignment{}, err
	}
	return s.directory.resolve(slot)
}
