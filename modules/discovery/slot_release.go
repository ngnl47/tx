package discovery

import (
	"context"
	"encoding/json"
	"errors"
)

// SlotReleaser 在进程完成排水和持久化后条件释放其仍拥有的槽位。
// 自定义 SlotStore 须显式实现；实例失联不能调用该能力代替收尾证明。
type SlotReleaser interface {
	ReleaseSlots(context.Context, string) error
}

// ReleaseSlot 对完整观察归属作 CAS，保留递增代次；重复释放同一代次可安全重试。
func (s *EtcdSlotStore) ReleaseSlot(ctx context.Context, current SlotAssignment) error {
	if current.Validate() != nil || current.Released() || current.Generation == ^uint64(0) {
		return ErrInvalidSlotAssignment
	}
	key, err := s.slotKey(current.SlotID)
	if err != nil {
		return err
	}
	value, exists, err := s.registry.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrSlotNotFound
	}
	stored, err := decodeSlotAssignment(key, value.Value)
	if err != nil {
		return err
	}
	next := SlotAssignment{SlotID: current.SlotID, Generation: current.Generation + 1}
	if stored == next {
		return nil
	}
	if stored != current {
		return ErrSlotConflict
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	ok, err := s.registry.client.CompareAndSwap(ctx, key, value.ModRevision, payload)
	if err != nil {
		return err
	}
	if !ok {
		return ErrSlotConflict
	}
	return nil
}

// ReleaseSlots 逐槽释放；部分成功或响应未知时，重试只处理仍属于该启动身份的记录。
func (s *EtcdSlotStore) ReleaseSlots(ctx context.Context, instance string) error {
	if instance == "" {
		return ErrInvalidSlotAssignment
	}
	prefix, err := s.slotPrefix()
	if err != nil {
		return err
	}
	snapshot, err := s.registry.client.Snapshot(ctx, prefix)
	if err != nil {
		return err
	}
	for _, value := range snapshot.Values {
		current, err := decodeSlotAssignment(value.Key, value.Value)
		if err != nil {
			return err
		}
		if current.RuntimeInstanceID != instance {
			continue
		}
		if err = s.ReleaseSlot(ctx, current); err != nil {
			if errors.Is(err, ErrSlotConflict) {
				actual, readErr := s.ResolveSlot(ctx, current.SlotID)
				if readErr == nil && actual.RuntimeInstanceID != instance {
					continue
				}
				if readErr != nil {
					return readErr
				}
			}
			return err
		}
	}
	return nil
}
