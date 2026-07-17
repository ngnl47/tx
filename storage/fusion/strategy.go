package fusion

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// DocumentLoader 是生成 Store 读取单条文档所需的最小接口。
type DocumentLoader interface {
	LoadDocument(context.Context, meta.TypeSpec, storage.RecordKey, ...string) (map[string]any, bool, error)
}

// BatchDocumentLoader 是生成 Store 批量读取文档所需的最小接口。
type BatchDocumentLoader interface {
	LoadDocuments(context.Context, meta.TypeSpec, []storage.RecordKey, ...string) ([]map[string]any, []bool, error)
}

// Strategy 实现 fusion 模式的变更落地策略。
//
// Redis 适配器负责即时写入热数据；MongoQueue 负责把持久化保存推迟到
// delaySave 配置之后执行。
type Strategy struct {
	Redis        storage.ChangeApplierWithOptions
	RedisDeleter storage.DeleteApplier
	RedisLoader  DocumentLoader
	MongoLoader  DocumentLoader
	MongoQueue   DelayQueue
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
// RedisDeleter 存在时会先删除热数据；MongoQueue 存在时会把持久化删除延迟
// 到 delayDelete 配置之后执行。
func (s Strategy) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if s.RedisDeleter != nil {
		if err := s.RedisDeleter.ApplyDelete(ctx, spec, key); err != nil {
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

// LoadDocument 优先从 Redis 读取，未命中时回源 Mongo。
func (s Strategy) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	if s.RedisLoader != nil {
		doc, exists, err := s.RedisLoader.LoadDocument(ctx, spec, key, fields...)
		if err != nil {
			return nil, false, err
		}
		if exists {
			return doc, true, nil
		}
	}
	if s.MongoLoader == nil {
		return nil, false, nil
	}
	return s.MongoLoader.LoadDocument(ctx, spec, key, fields...)
}

// LoadDocuments 批量读取文档，Redis 缺失的行再批量回源 Mongo。
func (s Strategy) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(keys))
	exists := make([]bool, len(keys))
	if len(keys) == 0 {
		return docs, exists, nil
	}

	missingIndexes := make([]int, 0, len(keys))
	missingKeys := make([]storage.RecordKey, 0, len(keys))
	if redisBatch, ok := s.RedisLoader.(BatchDocumentLoader); ok {
		redisDocs, redisExists, err := redisBatch.LoadDocuments(ctx, spec, keys, fields...)
		if err != nil {
			return nil, nil, err
		}
		if len(redisDocs) != len(keys) || len(redisExists) != len(keys) {
			return nil, nil, fmt.Errorf("fusion: redis batch loader returned docs=%d exists=%d for %d keys", len(redisDocs), len(redisExists), len(keys))
		}
		copy(docs, redisDocs)
		copy(exists, redisExists)
		for i, ok := range redisExists {
			if ok {
				continue
			}
			missingIndexes = append(missingIndexes, i)
			missingKeys = append(missingKeys, keys[i])
		}
	} else {
		for i, key := range keys {
			missingIndexes = append(missingIndexes, i)
			missingKeys = append(missingKeys, key)
		}
	}

	if len(missingKeys) == 0 || s.MongoLoader == nil {
		return docs, exists, nil
	}
	mongoBatch, ok := s.MongoLoader.(BatchDocumentLoader)
	if !ok {
		for pos, key := range missingKeys {
			doc, found, err := s.MongoLoader.LoadDocument(ctx, spec, key, fields...)
			if err != nil {
				return nil, nil, err
			}
			idx := missingIndexes[pos]
			docs[idx] = doc
			exists[idx] = found
		}
		return docs, exists, nil
	}
	mongoDocs, mongoExists, err := mongoBatch.LoadDocuments(ctx, spec, missingKeys, fields...)
	if err != nil {
		return nil, nil, err
	}
	if len(mongoDocs) != len(missingKeys) || len(mongoExists) != len(missingKeys) {
		return nil, nil, fmt.Errorf("fusion: mongo batch loader returned docs=%d exists=%d for %d keys", len(mongoDocs), len(mongoExists), len(missingKeys))
	}
	for pos, idx := range missingIndexes {
		docs[idx] = mongoDocs[pos]
		exists[idx] = mongoExists[pos]
	}
	return docs, exists, nil
}
