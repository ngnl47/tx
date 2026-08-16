package discovery

import (
	"context"
	"encoding/json"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
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
	response, err := s.registry.client.Get(ctx, key)
	if err != nil {
		return SlotAssignment{}, err
	}
	if len(response.Kvs) == 0 {
		return SlotAssignment{}, ErrSlotNotFound
	}
	return decodeSlotAssignment(key, response.Kvs[0].Value)
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
	response, err := s.registry.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(payload))).
		Commit()
	if err != nil {
		return err
	}
	if !response.Succeeded {
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
	currentResponse, err := s.registry.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(currentResponse.Kvs) == 0 {
		return ErrSlotNotFound
	}
	stored, err := decodeSlotAssignment(key, currentResponse.Kvs[0].Value)
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
	response, err := s.registry.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", currentResponse.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(key, string(payload))).
		Commit()
	if err != nil {
		return err
	}
	if !response.Succeeded {
		return ErrSlotConflict
	}
	return nil
}

func (s *EtcdSlotStore) slotKey(slotID string) (string, error) {
	if s == nil || s.registry == nil || s.registry.client == nil {
		return "", ErrEtcdClientNil
	}
	if !validSlotID(slotID) {
		return "", ErrInvalidSlotAssignment
	}
	return strings.TrimSuffix(s.registry.config.Prefix, "/instances") + "/slots/" + slotID, nil
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
