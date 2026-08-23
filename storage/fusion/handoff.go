package fusion

import (
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/task"
)

const defaultHandoffMaxPendingPerSlot = 1024

type handoffMarker struct {
	Type     string `json:"type"`
	ID       any    `json:"id"`
	ShardKey any    `json:"shardKey,omitempty"`
}

func encodeHandoffMarker(spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	typeName := delayedOperationType(DelayedOperation{Type: spec.Type.Ref, Key: key})
	item := task.DelayTask{Type: typeName, ID: key.ID, ShardKey: key.ShardKey, Action: string(DelayedSave)}
	if err := task.ValidateDelayTask(item); err != nil {
		return "", err
	}
	return txredis.EncodeHandoffMarker(spec.Type.Ref, key)
}

func (s Strategy) handoffMaxPendingPerSlot() int {
	if s.HandoffMaxPendingPerSlot > 0 {
		return s.HandoffMaxPendingPerSlot
	}
	return defaultHandoffMaxPendingPerSlot
}
