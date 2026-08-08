package fusion

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/task"
)

type TypeRegistry interface {
	Lookup(meta.TypeRef) (meta.TypeSpec, bool)
}

type StorageTaskHandler struct {
	types               TypeRegistry
	redisLoader         storage.DocumentLoader
	mongoReplace        storage.DocumentReplacer
	redisRevisionReader storage.RecordRevisionReader
	redisRevisionDelete storage.RevisionDeleteApplier
	redisLocker         storage.RecordLocker
	delayQueue          DelayQueue
}

type DelayTaskHandlerOptions struct {
	RedisRevisionReader storage.RecordRevisionReader
	RedisRevisionDelete storage.RevisionDeleteApplier
	RedisLocker         storage.RecordLocker
	DelayQueue          DelayQueue
}

func NewDelayTaskHandler(types TypeRegistry, redisLoader storage.DocumentLoader, mongoReplace storage.DocumentReplacer, redisLocker storage.RecordLocker) *StorageTaskHandler {
	return NewDelayTaskHandlerWithOptions(types, redisLoader, mongoReplace, DelayTaskHandlerOptions{RedisLocker: redisLocker})
}

func NewDelayTaskHandlerWithOptions(types TypeRegistry, redisLoader storage.DocumentLoader, mongoReplace storage.DocumentReplacer, options DelayTaskHandlerOptions) *StorageTaskHandler {
	return &StorageTaskHandler{
		types:               types,
		redisLoader:         redisLoader,
		mongoReplace:        mongoReplace,
		redisRevisionReader: options.RedisRevisionReader,
		redisRevisionDelete: options.RedisRevisionDelete,
		redisLocker:         options.RedisLocker,
		delayQueue:          options.DelayQueue,
	}
}

func (h *StorageTaskHandler) HandleDelayTask(ctx context.Context, item task.DelayTask) error {
	if err := task.ValidateDelayTask(item); err != nil {
		return err
	}
	if h == nil || isNilFusionDependency(h.types) {
		return ErrDelayTaskDependenciesNotConfigured
	}
	spec, ok := h.lookup(item.Type)
	if !ok {
		return fmt.Errorf("fusion: unknown delayed task type %q", item.Type)
	}
	key := storage.RecordKey{
		TypeName: item.Type,
		ID:       item.ID,
		ShardKey: item.ShardKey,
	}
	switch DelayedOperationKind(item.Action) {
	case DelayedSave:
		return h.handleSave(ctx, spec, key)
	case DelayedDelete:
		return h.handleDelete(ctx, spec, key)
	default:
		return fmt.Errorf("fusion: unknown delayed task action %q", item.Action)
	}
}

func (h *StorageTaskHandler) handleSave(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (err error) {
	if isNilFusionDependency(h.redisLoader) || isNilFusionDependency(h.mongoReplace) || isNilFusionDependency(h.redisLocker) {
		return fmt.Errorf("%w: delayed save requires redis loader, mongo replacer and record locker", ErrDelayTaskDependenciesNotConfigured)
	}
	lock, err := h.redisLocker.LockRecord(ctx, spec, key, storage.LockOptions{TTL: defaultLoadLockTTL, Wait: defaultLoadLockWait})
	if err != nil {
		return err
	}
	if isNilFusionDependency(lock) {
		return fmt.Errorf("%w: delayed save received nil record lock", ErrDelayTaskDependenciesNotConfigured)
	}
	defer func() { releaseRecordLock(ctx, lock, &err) }()
	doc, exists, err := h.redisLoader.LoadDocument(ctx, spec, key)
	if err != nil {
		return err
	}
	if err := storage.ValidateDocumentResult(doc, exists); err != nil {
		return fmt.Errorf("fusion: delayed save redis document loader: %w", err)
	}
	if !exists {
		return nil
	}
	return h.mongoReplace.ReplaceDocument(ctx, spec, key, doc)
}

func (h *StorageTaskHandler) handleDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (err error) {
	if isNilFusionDependency(h.redisLoader) || isNilFusionDependency(h.mongoReplace) || isNilFusionDependency(h.redisRevisionReader) || isNilFusionDependency(h.redisRevisionDelete) || isNilFusionDependency(h.redisLocker) {
		return fmt.Errorf("%w: delayed delete requires redis loader, mongo replacer, revision reader, revision deleter and record locker", ErrDelayTaskDependenciesNotConfigured)
	}
	lock, err := h.redisLocker.LockRecord(ctx, spec, key, storage.LockOptions{TTL: defaultLoadLockTTL, Wait: defaultLoadLockWait})
	if err != nil {
		return err
	}
	if isNilFusionDependency(lock) {
		return fmt.Errorf("%w: delayed delete received nil record lock", ErrDelayTaskDependenciesNotConfigured)
	}
	defer func() { releaseRecordLock(ctx, lock, &err) }()

	revision, err := h.redisRevisionReader.ReadRecordRevision(ctx, spec, key)
	if err != nil {
		return err
	}
	doc, exists, err := h.redisLoader.LoadDocument(ctx, spec, key)
	if err != nil {
		return err
	}
	if err := storage.ValidateDocumentResult(doc, exists); err != nil {
		return fmt.Errorf("fusion: delayed delete redis document loader: %w", err)
	}
	if !exists {
		return nil
	}
	if err := h.mongoReplace.ReplaceDocument(ctx, spec, key, doc); err != nil {
		return err
	}
	deleted, _, err := h.redisRevisionDelete.ApplyDeleteWithRevision(ctx, spec, key, revision)
	if err != nil || deleted {
		return err
	}
	return h.requeueDelete(ctx, spec, key)
}

func (h *StorageTaskHandler) requeueDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if spec.Storage.DelayDelete.Duration <= 0 {
		return nil
	}
	if isNilFusionDependency(h.delayQueue) {
		return fmt.Errorf("%w: delayed delete requeue requires delay queue", ErrDelayTaskDependenciesNotConfigured)
	}
	return h.delayQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedDelete,
		Type:     spec.Type.Ref,
		Key:      key,
		RunAfter: delayedDeleteAfter(spec),
	})
}

func (h *StorageTaskHandler) lookup(typeName string) (meta.TypeSpec, bool) {
	if isNilFusionDependency(h.types) {
		return meta.TypeSpec{}, false
	}
	return h.types.Lookup(meta.TypeRef{FullName: typeName})
}
