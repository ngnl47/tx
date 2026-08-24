package fusion

import (
	"context"
	"errors"
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
	SkipRedisBackfill bool
}

// Strategy 实现 fusion 模式的变更落地策略。
//
// Redis 适配器负责即时写入热数据；DelaySubmitter 只把根记录登记到进程内
// 分发器，实际延迟任务发布不阻塞业务保存。
type Strategy struct {
	RedisApplier   storage.ChangeApplier
	RedisLoader    storage.DocumentLoader
	RedisReplacer  storage.DocumentReplacerWithOptions
	RedisLocker    storage.RecordLocker
	MongoLoader    storage.DocumentLoader
	DelaySubmitter DelayTaskSubmitter
	DirtyTTL       time.Duration
	LoadLockTTL    time.Duration
	LoadLockWait   time.Duration
}

// Validate 校验 Fusion 运行时的完整装配。项目启动时必须调用该方法，避免缺失
// Redis/Mongo/延迟队列组件的半装配实例进入业务服务。
func (s Strategy) Validate() error {
	missing := make([]string, 0, 8)
	if isNilFusionDependency(s.RedisApplier) {
		missing = append(missing, "redis change applier")
	}
	if isNilFusionDependency(s.redisLoader()) {
		missing = append(missing, "redis document loader")
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
	if isNilFusionDependency(s.DelaySubmitter) {
		missing = append(missing, "delay task submitter")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s", ErrStrategyDependenciesNotConfigured, strings.Join(missing, ", "))
	}
	return nil
}

// ApplyChangesWithOptions 应用一次规范化变更。Fusion 会用 dirtyTTL 保护
// Redis 脏热文档，直到延迟任务完成 Mongo 回写和版本保护的 TTL 收敛。
func (s Strategy) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	if err := change.ValidateSource(source); err != nil {
		return err
	}
	if !source.HasChanges() {
		return nil
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	dirtyTTL, err := (Config{DirtyTTL: s.DirtyTTL}).validateDirtyTTL()
	if err != nil {
		return err
	}
	if err := validateDirtyTTLForType(dirtyTTL, spec); err != nil {
		return err
	}
	redisWriter, err := s.writeDependencies()
	if err != nil {
		return err
	}
	// Fusion 使用独立的安全 TTL 保护尚未确认写入 Mongo 的热数据；业务传入的
	// TTL 不得缩短这段可靠回写窗口。
	options.TTL = dirtyTTL
	if err := redisWriter.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options); err != nil {
		return err
	}
	s.DelaySubmitter.Submit(spec, key)
	return nil
}

// LoadDocument 优先从 Redis 读取，未命中时回源 Mongo。
func (s Strategy) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	return s.LoadDocumentWithOptions(ctx, spec, key, LoadOptions{}, fields...)
}

func (s Strategy) LoadDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, options LoadOptions, fields ...string) (map[string]any, bool, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return nil, false, err
	}
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, false, err
	}
	redisLoader, err := s.readDependencies()
	if err != nil {
		return nil, false, err
	}
	doc, exists, err := loadFusionDocument(ctx, redisLoader, "redis document loader", spec, key, fields...)
	if err != nil {
		return nil, false, err
	}
	if exists {
		return doc, true, nil
	}
	if options.SkipRedisBackfill {
		return loadFusionDocument(ctx, s.MongoLoader, "mongo document loader", spec, key, fields...)
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
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, nil, err
	}
	if len(keys) == 0 {
		return docs, exists, nil
	}
	if err := storage.ValidateRecordKeys(spec, keys); err != nil {
		return nil, nil, err
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
		if err := validateFusionDocumentResults("redis batch loader", redisDocs, redisExists, len(keys)); err != nil {
			return nil, nil, err
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
	if options.SkipRedisBackfill {
		return s.loadMongoMissingDocuments(ctx, spec, missingIndexes, missingKeys, docs, exists, fields...)
	}
	if err := s.requireRedisBackfillDependencies(); err != nil {
		return nil, nil, err
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

func (s Strategy) writeDependencies() (storage.ChangeApplier, error) {
	if isNilFusionDependency(s.RedisApplier) {
		return nil, ErrRedisNotConfigured
	}
	if isNilFusionDependency(s.MongoLoader) {
		return nil, fmt.Errorf("%w: mongo document loader", ErrStrategyDependenciesNotConfigured)
	}
	if isNilFusionDependency(s.DelaySubmitter) {
		return nil, ErrDelayTaskSubmitterNotConfigured
	}
	return s.RedisApplier, nil
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
			doc, found, err := loadFusionDocument(ctx, s.MongoLoader, "mongo document loader", spec, key, fields...)
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
	if err := validateFusionDocumentResults("mongo batch loader", mongoDocs, mongoExists, len(missingKeys)); err != nil {
		return nil, nil, err
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
	if err := validateFusionDocumentResults("mongo batch loader", mongoDocs, mongoExists, len(missingKeys)); err != nil {
		return err
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

func (s Strategy) backfillMongoDocumentAfterRedisCheck(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, mongoDocument map[string]any, mongoExists bool, fields ...string) (document map[string]any, exists bool, err error) {
	if err := s.requireRedisBackfillDependencies(); err != nil {
		return nil, false, err
	}
	replacer := s.redisReplacerWithOptions()
	locker := s.redisLocker()
	lock, err := locker.LockRecord(ctx, spec, key, storage.LockOptions{TTL: s.loadLockTTL(), Wait: s.loadLockWait()})
	if err != nil {
		return nil, false, err
	}
	if isNilFusionDependency(lock) {
		return nil, false, fmt.Errorf("%w: redis backfill received nil record lock", ErrStrategyDependenciesNotConfigured)
	}
	defer func() { releaseRecordLock(ctx, lock, &err) }()

	if redisLoader := s.redisLoader(); !isNilFusionDependency(redisLoader) {
		document, exists, err = loadFusionDocument(ctx, redisLoader, "redis document loader", spec, key, fields...)
		if err != nil {
			return nil, false, err
		}
		if exists {
			return document, true, nil
		}
	}
	if err := storage.ValidateDocumentResult(mongoDocument, mongoExists); err != nil {
		return nil, false, fmt.Errorf("fusion: mongo batch loader: %w", err)
	}
	if !mongoExists {
		return nil, false, nil
	}
	ttl, err := s.redisBackfillTTL(spec)
	if err != nil {
		return nil, false, err
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

func (s Strategy) loadMongoDocumentAndBackfillRedis(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (document map[string]any, exists bool, err error) {
	if err := s.requireRedisBackfillDependencies(); err != nil {
		return nil, false, err
	}
	replacer := s.redisReplacerWithOptions()
	locker := s.redisLocker()
	lock, err := locker.LockRecord(ctx, spec, key, storage.LockOptions{TTL: s.loadLockTTL(), Wait: s.loadLockWait()})
	if err != nil {
		return nil, false, err
	}
	if isNilFusionDependency(lock) {
		return nil, false, fmt.Errorf("%w: redis backfill received nil record lock", ErrStrategyDependenciesNotConfigured)
	}
	defer func() { releaseRecordLock(ctx, lock, &err) }()

	if redisLoader := s.redisLoader(); !isNilFusionDependency(redisLoader) {
		document, exists, err = loadFusionDocument(ctx, redisLoader, "redis document loader", spec, key, fields...)
		if err != nil {
			return nil, false, err
		}
		if exists {
			return document, true, nil
		}
	}
	ttl, err := s.redisBackfillTTL(spec)
	if err != nil {
		return nil, false, err
	}
	document, exists, err = loadFusionDocument(ctx, s.MongoLoader, "mongo document loader", spec, key)
	if err != nil || !exists {
		return document, exists, err
	}
	options := storage.ApplyOptions{
		Upsert: true,
		TTL:    ttl,
	}
	if err := replacer.ReplaceDocumentWithOptions(ctx, spec, key, document, options); err != nil {
		return nil, false, err
	}
	return projectDocument(spec, document, fields), true, nil
}

func releaseRecordLock(ctx context.Context, lock storage.RecordLock, result *error) {
	if unlockErr := lock.Unlock(ctx); unlockErr != nil {
		unlockErr = fmt.Errorf("fusion: unlock record lock: %w", unlockErr)
		if *result == nil {
			*result = unlockErr
			return
		}
		*result = errors.Join(*result, unlockErr)
	}
}

func (s Strategy) redisBackfillTTL(spec meta.TypeSpec) (time.Duration, error) {
	ttl := spec.Storage.DelayDelete.Duration
	if ttl <= 0 {
		dirtyTTL, err := (Config{DirtyTTL: s.DirtyTTL}).validateDirtyTTL()
		if err != nil {
			return 0, err
		}
		if err := validateDirtyTTLForType(dirtyTTL, spec); err != nil {
			return 0, err
		}
		ttl = dirtyTTL
	}
	if err := storage.ValidateExpiringTTL(ttl); err != nil {
		return 0, fmt.Errorf("fusion: redis backfill TTL: %w", err)
	}
	return ttl, nil
}

func (s Strategy) loadRedisDocumentsOneByOne(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, loader storage.DocumentLoader, docs []map[string]any, exists []bool, missingIndexes *[]int, missingKeys *[]storage.RecordKey, fields ...string) error {
	for i, key := range keys {
		if isNilFusionDependency(loader) {
			*missingIndexes = append(*missingIndexes, i)
			*missingKeys = append(*missingKeys, key)
			continue
		}
		doc, found, err := loadFusionDocument(ctx, loader, "redis document loader", spec, key, fields...)
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
	if !isNilFusionDependency(s.RedisLoader) {
		return s.RedisLoader
	}
	loader, _ := s.RedisApplier.(storage.DocumentLoader)
	if isNilFusionDependency(loader) {
		return nil
	}
	return loader
}

func loadFusionDocument(ctx context.Context, loader storage.DocumentLoader, source string, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	document, exists, err := loader.LoadDocument(ctx, spec, key, fields...)
	if err != nil {
		return nil, false, err
	}
	if err := storage.ValidateDocumentResult(document, exists); err != nil {
		return nil, false, fmt.Errorf("fusion: %s: %w", source, err)
	}
	return document, exists, nil
}

func validateFusionDocumentResults(source string, documents []map[string]any, exists []bool, expected int) error {
	if len(documents) != expected || len(exists) != expected {
		return fmt.Errorf("fusion: %s returned docs=%d exists=%d for %d keys", source, len(documents), len(exists), expected)
	}
	if err := storage.ValidateDocumentResults(documents, exists); err != nil {
		return fmt.Errorf("fusion: %s: %w", source, err)
	}
	return nil
}

func (s Strategy) redisReplacerWithOptions() storage.DocumentReplacerWithOptions {
	if !isNilFusionDependency(s.RedisReplacer) {
		return s.RedisReplacer
	}
	replacer, _ := s.RedisApplier.(storage.DocumentReplacerWithOptions)
	if isNilFusionDependency(replacer) {
		return nil
	}
	return replacer
}

func (s Strategy) requireRedisBackfillDependencies() error {
	if isNilFusionDependency(s.redisReplacerWithOptions()) {
		return fmt.Errorf("%w: redis document replacer with options", ErrStrategyDependenciesNotConfigured)
	}
	if isNilFusionDependency(s.redisLocker()) {
		return fmt.Errorf("%w: redis record locker", ErrStrategyDependenciesNotConfigured)
	}
	return nil
}

func enqueueDelayedWrites(ctx context.Context, queue DelayQueue, spec meta.TypeSpec, key storage.RecordKey) error {
	if isNilFusionDependency(queue) {
		return ErrDelayQueueNotConfigured
	}
	return queue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedSave,
		Type:     spec.Type.Ref,
		Key:      key,
		RunAfter: spec.Storage.DelaySave.Duration,
	})
}

func (s Strategy) redisLocker() storage.RecordLocker {
	if !isNilFusionDependency(s.RedisLocker) {
		return s.RedisLocker
	}
	locker, _ := s.RedisApplier.(storage.RecordLocker)
	if isNilFusionDependency(locker) {
		return nil
	}
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
