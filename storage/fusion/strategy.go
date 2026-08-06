package fusion

import (
	"context"
	"fmt"
	"reflect"
	"strings"
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

// Validate 校验 Fusion 运行时的完整装配。项目启动时必须调用该方法，避免缺失
// Redis/Mongo/延迟队列组件的半装配实例进入业务服务。
func (s Strategy) Validate() error {
	missing := make([]string, 0, 8)
	if isNilFusionDependency(s.Redis) {
		missing = append(missing, "redis delta applier")
	}
	if isNilFusionDependency(s.redisLoader()) {
		missing = append(missing, "redis document loader")
	}
	if isNilFusionDependency(s.redisDeleter()) {
		missing = append(missing, "redis deleter")
	}
	if isNilFusionDependency(s.redisReplacerWithOptions()) {
		missing = append(missing, "redis document replacer")
	}
	if isNilFusionDependency(s.redisLocker()) {
		missing = append(missing, "redis record locker")
	}
	if isNilFusionDependency(s.MongoLoader) {
		missing = append(missing, "mongo document loader")
	}
	if isNilFusionDependency(s.MongoDeleter) {
		missing = append(missing, "mongo deleter")
	}
	if isNilFusionDependency(s.MongoQueue) {
		missing = append(missing, "mongo delayed queue")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s", ErrStrategyDependenciesNotConfigured, strings.Join(missing, ", "))
	}
	return nil
}

// ApplyDeltasWithOptions 应用一次 Delta 变更，并把写入选项限定传递给 Redis。
func (s Strategy) ApplyDeltasWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver, options storage.ApplyOptions) error {
	if len(deltas) == 0 {
		return nil
	}
	if isNilFusionDependency(s.Redis) {
		return ErrRedisNotConfigured
	}
	if isNilFusionDependency(s.MongoQueue) {
		return ErrMongoQueueNotConfigured
	}
	if err := s.Redis.ApplyDeltasWithOptions(ctx, spec, key, deltas, resolver, values, options); err != nil {
		return err
	}
	return s.enqueueDelayedWrites(ctx, spec, key)
}

// ApplyDelete 应用一次对象删除。
//
// 删除要求 Redis 与 Mongo 删除器均已装配。Redis 会先删除热数据，随后 Mongo
// 立即删除持久化数据。storage.delayDelTime 只表示 Redis 热数据 TTL，不参与 Mongo
// 删除延迟。
func (s Strategy) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	redisDeleter, mongoDeleter, err := s.deleteDependencies()
	if err != nil {
		return err
	}
	if err := redisDeleter.ApplyDelete(ctx, spec, key); err != nil {
		return err
	}
	return mongoDeleter.ApplyDelete(ctx, spec, key)
}

// LoadDocument 优先从 Redis 读取，未命中时回源 Mongo。
func (s Strategy) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	return s.LoadDocumentWithOptions(ctx, spec, key, LoadOptions{}, fields...)
}

func (s Strategy) LoadDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, options LoadOptions, fields ...string) (map[string]any, bool, error) {
	redisLoader, err := s.readDependencies()
	if err != nil {
		return nil, false, err
	}
	doc, exists, err := redisLoader.LoadDocument(ctx, spec, key, fields...)
	if err != nil {
		return nil, false, err
	}
	if exists {
		return doc, true, nil
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

	redisLoader, err := s.readDependencies()
	if err != nil {
		return nil, nil, err
	}
	missingIndexes := make([]int, 0, len(keys))
	missingKeys := make([]storage.RecordKey, 0, len(keys))
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

	if len(missingKeys) == 0 {
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

func (s Strategy) readDependencies() (storage.DocumentLoader, error) {
	redisLoader := s.redisLoader()
	if isNilFusionDependency(redisLoader) {
		return nil, fmt.Errorf("%w: redis document loader", ErrStrategyDependenciesNotConfigured)
	}
	if isNilFusionDependency(s.MongoLoader) {
		return nil, fmt.Errorf("%w: mongo document loader", ErrStrategyDependenciesNotConfigured)
	}
	return redisLoader, nil
}

func (s Strategy) deleteDependencies() (storage.DeleteApplier, storage.DeleteApplier, error) {
	redisDeleter := s.redisDeleter()
	if isNilFusionDependency(redisDeleter) {
		return nil, nil, fmt.Errorf("%w: redis deleter", ErrStrategyDependenciesNotConfigured)
	}
	if isNilFusionDependency(s.MongoDeleter) {
		return nil, nil, fmt.Errorf("%w: mongo deleter", ErrStrategyDependenciesNotConfigured)
	}
	return redisDeleter, s.MongoDeleter, nil
}

func isNilFusionDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
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
	mongoBatch, ok := s.MongoLoader.(storage.BatchDocumentLoader)
	if ok {
		return s.loadMissingDocumentsWithBatchBackfill(ctx, spec, missingIndexes, missingKeys, docs, exists, mongoBatch, fields...)
	}
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

func (s Strategy) loadMissingDocumentsWithBatchBackfill(ctx context.Context, spec meta.TypeSpec, missingIndexes []int, missingKeys []storage.RecordKey, docs []map[string]any, exists []bool, mongo storage.BatchDocumentLoader, fields ...string) error {
	mongoDocs, mongoExists, err := mongo.LoadDocuments(ctx, spec, missingKeys)
	if err != nil {
		return err
	}
	if len(mongoDocs) != len(missingKeys) || len(mongoExists) != len(missingKeys) {
		return fmt.Errorf("fusion: mongo batch loader returned docs=%d exists=%d for %d keys", len(mongoDocs), len(mongoExists), len(missingKeys))
	}
	for pos, idx := range missingIndexes {
		doc, found, err := s.backfillMongoDocumentAfterRedisCheck(ctx, spec, missingKeys[pos], mongoDocs[pos], mongoExists[pos], fields...)
		if err != nil {
			return err
		}
		docs[idx] = doc
		exists[idx] = found
	}
	return nil
}

func (s Strategy) backfillMongoDocumentAfterRedisCheck(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, mongoDocument map[string]any, mongoExists bool, fields ...string) (map[string]any, bool, error) {
	replacer := s.redisReplacerWithOptions()
	if replacer == nil {
		return nil, false, fmt.Errorf("fusion: redis backfill requires ReplaceDocumentWithOptions to set delayDelTime TTL")
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
	if !mongoExists {
		return nil, false, nil
	}
	if mongoDocument == nil {
		return nil, false, fmt.Errorf("fusion: mongo batch loader returned an existing document without data")
	}
	ttl := int64(spec.Storage.DelayDelete.Duration.Seconds())
	if ttl <= 0 {
		return nil, false, fmt.Errorf("fusion: storage delayDelTime must be greater than 0 for redis backfill")
	}
	options := storage.ApplyOptions{
		Upsert: true,
		TTL:    ttl,
	}
	if err := replacer.ReplaceDocumentWithOptions(ctx, spec, key, mongoDocument, options); err != nil {
		return nil, false, err
	}
	return projectDocument(spec, mongoDocument, fields), true, nil
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
	if isNilFusionDependency(s.MongoQueue) {
		return ErrMongoQueueNotConfigured
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
