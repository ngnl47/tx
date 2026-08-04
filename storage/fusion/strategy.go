package fusion

import (
	"context"
	"fmt"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

const (
	defaultLoadLockTTL  = 10 * time.Second
	defaultLoadLockWait = 8 * time.Second
)

type LoadOptions struct {
	DontLoadToRedis bool
}

// Strategy 实现 fusion 模式的变更落地策略。
//
// Redis 适配器负责即时写入热数据；MongoQueue 负责把持久化保存推迟到
// delaySave 配置之后执行。
type Strategy struct {
	Redis         storage.DeltaApplier
	RedisDeleter  storage.DeleteApplier
	RedisLoader   storage.DocumentLoader
	RedisReplacer any
	RedisLocker   storage.RecordLocker
	MongoLoader   storage.DocumentLoader
	MongoDeleter  storage.DeleteApplier
	MongoQueue    DelayQueue
	LoadLockTTL   time.Duration
	LoadLockWait  time.Duration
}

// ApplyDeltasWithOptions 应用一次 Delta 变更，并把写入选项限定传递给 Redis。
func (s Strategy) ApplyDeltasWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver, options storage.ApplyOptions) error {
	if len(deltas) == 0 {
		return nil
	}
	if s.Redis != nil {
		if err := s.Redis.ApplyDeltasWithOptions(ctx, spec, key, deltas, resolver, values, options); err != nil {
			return err
		}
	}
	if s.MongoQueue == nil {
		return nil
	}
	return s.enqueueDelayedWrites(ctx, spec, key)
}

// ApplyDelete 应用一次对象删除。
//
// RedisDeleter 存在时会先删除热数据；MongoDeleter 存在时会立即删除持久化数据。
// storage.delayDelTime 只表示 Redis 热数据 TTL，不参与 Mongo 删除延迟。
func (s Strategy) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if redisDeleter := s.redisDeleter(); redisDeleter != nil {
		if err := redisDeleter.ApplyDelete(ctx, spec, key); err != nil {
			return err
		}
	}
	if s.MongoDeleter != nil {
		return s.MongoDeleter.ApplyDelete(ctx, spec, key)
	}
	return nil
}

// LoadDocument 优先从 Redis 读取，未命中时回源 Mongo。
func (s Strategy) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	return s.LoadDocumentWithOptions(ctx, spec, key, LoadOptions{}, fields...)
}

func (s Strategy) LoadDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, options LoadOptions, fields ...string) (map[string]any, bool, error) {
	if redisLoader := s.redisLoader(); redisLoader != nil {
		doc, exists, err := redisLoader.LoadDocument(ctx, spec, key, fields...)
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
	if options.DontLoadToRedis {
		return s.MongoLoader.LoadDocument(ctx, spec, key, fields...)
	}
	return s.loadMongoDocumentAndBackfillRedis(ctx, spec, key, fields...)
}

// LoadDocuments 批量读取文档，Redis 缺失的行再批量回源 Mongo。
func (s Strategy) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	return s.LoadDocumentsWithOptions(ctx, spec, keys, LoadOptions{}, fields...)
}

func (s Strategy) LoadDocumentsWithOptions(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, options LoadOptions, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(keys))
	exists := make([]bool, len(keys))
	if len(keys) == 0 {
		return docs, exists, nil
	}

	missingIndexes := make([]int, 0, len(keys))
	missingKeys := make([]storage.RecordKey, 0, len(keys))
	redisLoader := s.redisLoader()
	if redisBatch, ok := redisLoader.(storage.BatchDocumentLoader); ok {
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
		if err := s.loadRedisDocumentsOneByOne(ctx, spec, keys, redisLoader, docs, exists, &missingIndexes, &missingKeys, fields...); err != nil {
			return nil, nil, err
		}
	}

	if len(missingKeys) == 0 || s.MongoLoader == nil {
		return docs, exists, nil
	}
	if options.DontLoadToRedis {
		return s.loadMongoMissingDocuments(ctx, spec, missingIndexes, missingKeys, docs, exists, fields...)
	}
	if s.redisReplacerWithOptions() == nil && s.redisReplacer() == nil {
		return s.loadMongoMissingDocuments(ctx, spec, missingIndexes, missingKeys, docs, exists, fields...)
	}
	if err := s.loadMissingDocumentsWithProtectedBackfill(ctx, spec, missingIndexes, missingKeys, docs, exists, fields...); err != nil {
		return nil, nil, err
	}
	return docs, exists, nil
}

func (s Strategy) loadMongoMissingDocuments(ctx context.Context, spec meta.TypeSpec, missingIndexes []int, missingKeys []storage.RecordKey, docs []map[string]any, exists []bool, fields ...string) ([]map[string]any, []bool, error) {
	mongoBatch, ok := s.MongoLoader.(storage.BatchDocumentLoader)
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

func (s Strategy) loadMissingDocumentsWithProtectedBackfill(ctx context.Context, spec meta.TypeSpec, missingIndexes []int, missingKeys []storage.RecordKey, docs []map[string]any, exists []bool, fields ...string) error {
	for pos, idx := range missingIndexes {
		doc, found, err := s.loadMongoDocumentAndBackfillRedis(ctx, spec, missingKeys[pos], fields...)
		if err != nil {
			return err
		}
		docs[idx] = doc
		exists[idx] = found
	}
	return nil
}

func (s Strategy) loadMongoDocumentAndBackfillRedis(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	replacer := s.redisReplacerWithOptions()
	if replacer == nil {
		if s.redisReplacer() != nil {
			return nil, false, fmt.Errorf("fusion: redis backfill requires ReplaceDocumentWithOptions to set delayDelTime TTL")
		}
		return s.MongoLoader.LoadDocument(ctx, spec, key, fields...)
	}
	locker := s.redisLocker()
	if locker == nil {
		return nil, false, fmt.Errorf("fusion: redis backfill requires record lock")
	}
	lock, err := locker.LockRecord(ctx, spec, key, storage.LockOptions{TTL: s.loadLockTTL(), Wait: s.loadLockWait()})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = lock.Unlock(ctx) }()

	if redisLoader := s.redisLoader(); redisLoader != nil {
		doc, exists, err := redisLoader.LoadDocument(ctx, spec, key, fields...)
		if err != nil {
			return nil, false, err
		}
		if exists {
			return doc, true, nil
		}
	}
	ttl := int64(spec.Storage.DelayDelete.Duration.Seconds())
	if ttl <= 0 {
		return nil, false, fmt.Errorf("fusion: storage delayDelTime must be greater than 0 for redis backfill")
	}
	doc, exists, err := s.MongoLoader.LoadDocument(ctx, spec, key)
	if err != nil || !exists {
		return doc, exists, err
	}
	options := storage.ApplyOptions{
		Upsert: true,
		TTL:    ttl,
	}
	if err := replacer.ReplaceDocumentWithOptions(ctx, spec, key, doc, options); err != nil {
		return nil, false, err
	}
	return projectDocument(spec, doc, fields), true, nil
}

func (s Strategy) loadRedisDocumentsOneByOne(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, loader storage.DocumentLoader, docs []map[string]any, exists []bool, missingIndexes *[]int, missingKeys *[]storage.RecordKey, fields ...string) error {
	for i, key := range keys {
		if loader == nil {
			*missingIndexes = append(*missingIndexes, i)
			*missingKeys = append(*missingKeys, key)
			continue
		}
		doc, found, err := loader.LoadDocument(ctx, spec, key, fields...)
		if err != nil {
			return err
		}
		if found {
			docs[i] = doc
			exists[i] = true
			continue
		}
		*missingIndexes = append(*missingIndexes, i)
		*missingKeys = append(*missingKeys, key)
	}
	return nil
}

func (s Strategy) redisLoader() storage.DocumentLoader {
	if s.RedisLoader != nil {
		return s.RedisLoader
	}
	loader, _ := s.Redis.(storage.DocumentLoader)
	return loader
}

func (s Strategy) redisReplacerWithOptions() storage.DocumentReplacerWithOptions {
	if replacer, ok := s.RedisReplacer.(storage.DocumentReplacerWithOptions); ok {
		return replacer
	}
	replacer, _ := s.Redis.(storage.DocumentReplacerWithOptions)
	return replacer
}

func (s Strategy) redisReplacer() storage.DocumentReplacer {
	if replacer, ok := s.RedisReplacer.(storage.DocumentReplacer); ok {
		return replacer
	}
	replacer, _ := s.Redis.(storage.DocumentReplacer)
	return replacer
}

func (s Strategy) redisDeleter() storage.DeleteApplier {
	if s.RedisDeleter != nil {
		return s.RedisDeleter
	}
	deleter, _ := s.Redis.(storage.DeleteApplier)
	return deleter
}

func (s Strategy) enqueueDelayedWrites(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if s.MongoQueue == nil {
		return nil
	}
	if err := s.MongoQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedSave,
		Type:     spec.Type.Ref,
		Key:      key,
		RunAfter: spec.Storage.DelaySave.Duration,
	}); err != nil {
		return err
	}
	if spec.Storage.DelayDelete.Duration <= 0 {
		return nil
	}
	return s.MongoQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedDelete,
		Type:     spec.Type.Ref,
		Key:      key,
		RunAfter: delayedDeleteAfter(spec),
	})
}

func (s Strategy) redisLocker() storage.RecordLocker {
	if s.RedisLocker != nil {
		return s.RedisLocker
	}
	locker, _ := s.Redis.(storage.RecordLocker)
	return locker
}

func (s Strategy) loadLockTTL() time.Duration {
	if s.LoadLockTTL > 0 {
		return s.LoadLockTTL
	}
	return defaultLoadLockTTL
}

func (s Strategy) loadLockWait() time.Duration {
	if s.LoadLockWait > 0 {
		return s.LoadLockWait
	}
	return defaultLoadLockWait
}

func delayedDeleteAfter(spec meta.TypeSpec) time.Duration {
	runAfter := spec.Storage.DelayDelete.Duration
	if spec.Storage.DelaySave.Duration > 0 {
		runAfter += spec.Storage.DelaySave.Duration
	}
	return runAfter
}

func projectDocument(spec meta.TypeSpec, doc map[string]any, fields []string) map[string]any {
	if len(fields) == 0 || doc == nil {
		return doc
	}
	out := make(map[string]any, len(fields))
	for _, field := range fields {
		name := spec.JSONName(field)
		if value, ok := doc[name]; ok {
			out[name] = value
		}
	}
	return out
}
