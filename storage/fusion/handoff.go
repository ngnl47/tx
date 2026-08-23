package fusion

import (
	"errors"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/task"
)

const defaultHandoffMaxPendingPerSlot = 1024

// HandoffRetryer 接收 Redis 热写完成后未能完成的交接记录。
// 普通业务保存不直接使用该接口。
type HandoffRetryer interface {
	QueueHandoffRetry(txredis.HandoffEntry) error
}

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

func (s Strategy) queueHandoffRetry(marker string, receipt txredis.HandoffReceipt, cause error) error {
	if isNilFusionDependency(s.HandoffRetryer) {
		return errors.Join(cause, ErrHandoffRetryerNotConfigured)
	}
	if err := s.HandoffRetryer.QueueHandoffRetry(txredis.HandoffEntry{Marker: marker, Token: receipt.Token}); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
