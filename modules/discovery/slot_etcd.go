package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	txetcd "github.com/ngnl47/tx/etcd"
)

// EtcdSlotStore 将逻辑槽位映射持久化在实例 lease 之外。运行实例消失不会删除
// assignment，发布控制面必须通过 PromoteSlot 明确交接到新的 generation。
type EtcdSlotStore struct {
	registry *EtcdRegistry
}

// Slots 返回与当前 Registry 共用 etcd 客户端的逻辑槽位存储。
func (r *EtcdRegistry) Slots() *EtcdSlotStore {
	if r == nil {
		return nil
	}
	return &EtcdSlotStore{registry: r}
}

func (s *EtcdSlotStore) ResolveSlot(ctx context.Context, slotID string) (SlotAssignment, error) {
	key, err := s.slotKey(slotID)
	if err != nil {
		return SlotAssignment{}, err
	}
	value, ok, err := s.registry.client.Get(ctx, key)
	if err != nil {
		return SlotAssignment{}, err
	}
	if !ok {
		return SlotAssignment{}, ErrSlotNotFound
	}
	return decodeSlotAssignment(key, value.Value)
}

// Watch 先载入完整槽位快照，再持续把 etcd 变更写入本地目录。方法退出前会使
// 目录失效，调用方不得在 Watch 中断期间继续使用旧归属。
func (s *EtcdSlotStore) Watch(ctx context.Context, directory *SlotDirectory) error {
	if directory == nil {
		return ErrSlotDirectoryNil
	}
	prefix, err := s.slotPrefix()
	if err != nil {
		return err
	}
	snapshot, err := s.registry.client.Snapshot(ctx, prefix)
	if err != nil {
		return err
	}
	current := make(map[string]SlotAssignment, len(snapshot.Values))
	for _, item := range snapshot.Values {
		assignment, decodeErr := decodeSlotAssignment(item.Key, item.Value)
		if decodeErr != nil {
			return decodeErr
		}
		current[assignment.SlotID] = assignment
	}
	if err := replaceSlotDirectory(directory, current); err != nil {
		return err
	}
	defer directory.Invalidate()

	watch := s.registry.client.WatchPrefix(ctx, prefix, snapshot.Revision+1)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response, ok := <-watch:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ErrEtcdWatchClosed
			}
			if response.Err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return mapEtcdWatchError(response.Err)
			}
			for _, event := range response.Events {
				switch event.Type {
				case txetcd.EventPut:
					assignment, decodeErr := decodeSlotAssignment(event.Value.Key, event.Value.Value)
					if decodeErr != nil {
						return decodeErr
					}
					current[assignment.SlotID] = assignment
				case txetcd.EventDelete:
					slotID := strings.TrimPrefix(event.Value.Key, prefix)
					if !validSlotID(slotID) {
						return ErrInvalidSlotAssignment
					}
					delete(current, slotID)
				default:
					return ErrEtcdWatchClosed
				}
			}
			if err := replaceSlotDirectory(directory, current); err != nil {
				return err
			}
		}
	}
}

func (s *EtcdSlotStore) CreateSlot(ctx context.Context, assignment SlotAssignment) error {
	if err := assignment.Validate(); err != nil {
		return err
	}
	key, err := s.slotKey(assignment.SlotID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(assignment)
	if err != nil {
		return err
	}
	ok, err := s.writeAssignment(ctx, key, 0, assignment, payload)
	if err != nil {
		return err
	}
	if !ok {
		return ErrSlotConflict
	}
	return nil
}

func (s *EtcdSlotStore) PromoteSlot(ctx context.Context, current, next SlotAssignment) error {
	if err := ValidateSlotPromotion(current, next); err != nil {
		return err
	}
	key, err := s.slotKey(current.SlotID)
	if err != nil {
		return err
	}
	value, ok, err := s.registry.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return ErrSlotNotFound
	}
	stored, err := decodeSlotAssignment(key, value.Value)
	if err != nil {
		return err
	}
	if stored != current {
		return ErrSlotConflict
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	ok, err = s.writeAssignment(ctx, key, value.ModRevision, next, payload)
	if err != nil {
		return err
	}
	if !ok {
		return ErrSlotConflict
	}
	return nil
}

func (s *EtcdSlotStore) slotKey(slotID string) (string, error) {
	if !validSlotID(slotID) {
		return "", ErrInvalidSlotAssignment
	}
	prefix, err := s.slotPrefix()
	if err != nil {
		return "", err
	}
	return prefix + slotID, nil
}

func (s *EtcdSlotStore) slotPrefix() (string, error) {
	if s == nil || s.registry == nil || s.registry.client == nil {
		return "", ErrEtcdClientNil
	}
	return strings.TrimSuffix(s.registry.config.Prefix, "/instances") + "/slots/", nil
}

func decodeSlotAssignment(key string, payload []byte) (SlotAssignment, error) {
	var assignment SlotAssignment
	if json.Unmarshal(payload, &assignment) != nil || assignment.Validate() != nil || !strings.HasSuffix(key, "/"+assignment.SlotID) {
		return SlotAssignment{}, ErrInvalidSlotAssignment
	}
	return assignment, nil
}

func validSlotID(slotID string) bool {
	return strings.TrimSpace(slotID) != "" && slotID == strings.TrimSpace(slotID) && !strings.Contains(slotID, "/")
}

func replaceSlotDirectory(directory *SlotDirectory, current map[string]SlotAssignment) error {
	assignments := make([]SlotAssignment, 0, len(current))
	for _, assignment := range current {
		assignments = append(assignments, assignment)
	}
	return directory.Replace(assignments)
}

// CheckSlotCapacity 供交接前预检；最终写入仍须原子复核，预检不占位。
func (s *EtcdSlotStore) CheckSlotCapacity(ctx context.Context, next SlotAssignment) error {
	_, _, err := s.capacitySnapshot(ctx, next)
	return err
}

// writeAssignment 只使用现有实例规格与 assignment；无配额时保持单键 CAS。
func (s *EtcdSlotStore) writeAssignment(ctx context.Context, key string, expected int64, next SlotAssignment, payload []byte) (bool, error) {
	root := strings.TrimSuffix(s.registry.config.Prefix, "/instances") + "/"
	for {
		snapshot, limited, err := s.capacitySnapshot(ctx, next)
		if err != nil {
			return false, err
		}
		if !limited {
			if expected == 0 {
				_, ok, err := s.registry.client.Create(ctx, key, payload, 0)
				return ok, err
			}
			return s.registry.client.CompareAndSwap(ctx, key, expected, payload)
		}
		var actual int64
		for _, v := range snapshot.Values {
			if v.Key == key {
				actual = v.ModRevision
				break
			}
		}
		if actual != expected {
			return false, ErrSlotConflict
		}
		ok, err := s.registry.client.CompareAndSwapWithinSnapshot(ctx, root, snapshot.Revision, key, expected, payload)
		if err != nil || ok {
			return ok, err
		}
		// 明确比较失败才重读；网络结果未知时直接返回，不能改派或重发业务。
	}
}

func (s *EtcdSlotStore) capacitySnapshot(ctx context.Context, next SlotAssignment) (txetcd.Snapshot, bool, error) {
	memberKey := s.registry.instanceKey(next.RuntimeInstanceID)
	member, exists, err := s.registry.client.Get(ctx, memberKey)
	if err != nil {
		return txetcd.Snapshot{}, false, err
	}
	var instance Instance
	if exists {
		instance, err = s.registry.decodeInstance(member)
		if err != nil {
			return txetcd.Snapshot{}, false, err
		}
	}
	if instance.MaxSlotAssignments == 0 {
		return txetcd.Snapshot{}, false, nil
	}
	prefix, _ := s.slotPrefix()
	root := strings.TrimSuffix(s.registry.config.Prefix, "/instances") + "/"
	// ponytail: 有限归属的冷分配扫描现有快照；规模需要时再引入事务索引，不增加第二份权威表。
	snapshot, err := s.registry.client.Snapshot(ctx, root)
	if err != nil {
		return snapshot, true, err
	}
	used := 0
	memberFound := false
	for _, v := range snapshot.Values {
		if v.Key == memberKey {
			if !bytes.Equal(v.Value, member.Value) {
				return snapshot, true, ErrSlotRuntimeUnavailable
			}
			memberFound = true
		}
		if !strings.HasPrefix(v.Key, prefix) {
			continue
		}
		assignment, err := decodeSlotAssignment(v.Key, v.Value)
		if err != nil {
			return snapshot, true, err
		}
		if assignment.SlotID != next.SlotID && assignment.RuntimeInstanceID == next.RuntimeInstanceID {
			used++
		}
	}
	if !memberFound {
		return snapshot, true, ErrSlotRuntimeUnavailable
	}
	if used >= instance.MaxSlotAssignments {
		return snapshot, true, ErrSlotCapacityUnavailable
	}
	return snapshot, true, nil
}
