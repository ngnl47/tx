package fusion

import (
	"context"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Strategy 实现 fusion 模式的变更落地策略。
//
// Redis 适配器负责即时写入热数据；MongoQueue 负责把持久化保存推迟到
// delaySave 配置之后执行。
type Strategy struct {
	Redis      storage.ChangeApplier
	RedisStore storage.StoreAdapter
	MongoQueue DelayQueue
}

// ApplyChanges 应用一次对象变更。
func (s Strategy) ApplyChanges(ctx context.Context, spec meta.TypeSpec, id any, set change.ChangeSet) error {
	if set.Empty() {
		return nil
	}
	key := storage.RecordKey{
		TypeName: spec.Type.Ref.FullName,
		ID:       id,
	}
	if s.Redis != nil {
		if err := s.Redis.ApplyChanges(ctx, spec, key, set); err != nil {
			return err
		}
	}
	if s.MongoQueue == nil {
		return nil
	}
	return s.MongoQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedSave,
		Type:     spec.Type.Ref,
		ID:       id,
		Changes:  set,
		RunAfter: spec.Storage.DelaySave.Duration,
	})
}

// ApplyDelete 应用一次对象删除。
//
// RedisStore 存在时会先删除热数据；MongoQueue 存在时会把持久化删除延迟
// 到 delayDelete 配置之后执行。
func (s Strategy) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if s.RedisStore != nil {
		if err := s.RedisStore.Delete(ctx, key); err != nil {
			return err
		}
	}
	if s.MongoQueue == nil {
		return nil
	}
	return s.MongoQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedDelete,
		Type:     spec.Type.Ref,
		ID:       key.ID,
		RunAfter: spec.Storage.DelayDelete.Duration,
	})
}
