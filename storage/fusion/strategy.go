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
	Redis      storage.ChangeApplierWithOptions
	RedisStore storage.StoreAdapter
	MongoQueue DelayQueue
}

// ApplyChangesWithOptions 应用一次对象变更，并把写入选项限定传递给 Redis。
func (s Strategy) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet, options storage.ApplyOptions) error {
	if set.Empty() {
		return nil
	}
	if s.Redis != nil {
		if err := s.Redis.ApplyChangesWithOptions(ctx, spec, key, set, options); err != nil {
			return err
		}
	}
	if s.MongoQueue == nil {
		return nil
	}
	return s.MongoQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedSave,
		Type:     spec.Type.Ref,
		ID:       key.ID,
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
