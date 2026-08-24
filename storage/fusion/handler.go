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
	TypeSpecs() []meta.TypeSpec
}

type StorageTaskHandler struct {
	types               TypeRegistry
	redisLoader         storage.DocumentLoader
	mongoReplace        storage.DocumentReplacer
	redisRevisionReader storage.RecordRevisionReader
	redisRevisionTTL    storage.RevisionTTLApplier
	redisLocker         storage.RecordLocker
	delayedSaveQueue    DelayedSaveQueue
}

type DelayTaskHandlerOptions struct {
	RedisRevisionReader storage.RecordRevisionReader
	RedisRevisionTTL    storage.RevisionTTLApplier
	RedisLocker         storage.RecordLocker
	DelayedSaveQueue    DelayedSaveQueue
}

func NewDelayTaskHandler(types TypeRegistry, redisLoader storage.DocumentLoader, mongoReplace storage.DocumentReplacer, redisLocker storage.RecordLocker) *StorageTaskHandler {
	revisionReader, _ := redisLoader.(storage.RecordRevisionReader)
	revisionTTL, _ := redisLoader.(storage.RevisionTTLApplier)
	return NewDelayTaskHandlerWithOptions(types, redisLoader, mongoReplace, DelayTaskHandlerOptions{
		RedisRevisionReader: revisionReader,
		RedisRevisionTTL:    revisionTTL,
		RedisLocker:         redisLocker,
	})
}

func NewDelayTaskHandlerWithOptions(types TypeRegistry, redisLoader storage.DocumentLoader, mongoReplace storage.DocumentReplacer, options DelayTaskHandlerOptions) *StorageTaskHandler {
	return &StorageTaskHandler{
		types:               types,
		redisLoader:         redisLoader,
		mongoReplace:        mongoReplace,
		redisRevisionReader: options.RedisRevisionReader,
		redisRevisionTTL:    options.RedisRevisionTTL,
		redisLocker:         options.RedisLocker,
		delayedSaveQueue:    options.DelayedSaveQueue,
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
		ID:       item.ID,
		ShardKey: item.ShardKey,
	}
	if item.Action != delayedSaveAction {
		return fmt.Errorf("fusion: unknown delayed task action %q", item.Action)
	}
	return h.handleSave(ctx, spec, key)
}

func (h *StorageTaskHandler) handleSave(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (err error) {
	if isNilFusionDependency(h.redisLoader) || isNilFusionDependency(h.mongoReplace) || isNilFusionDependency(h.redisLocker) {
		return fmt.Errorf("%w: delayed save requires redis loader, mongo replacer and record locker", ErrDelayTaskDependenciesNotConfigured)
	}
	convergeTTL := spec.Storage.DelayDelete.Duration > 0
	if convergeTTL && (isNilFusionDependency(h.redisRevisionReader) || isNilFusionDependency(h.redisRevisionTTL)) {
		return fmt.Errorf("%w: delayed save TTL convergence requires revision reader and revision TTL applier", ErrDelayTaskDependenciesNotConfigured)
	}
	lock, err := h.redisLocker.LockRecord(ctx, spec, key, storage.LockOptions{TTL: defaultLoadLockTTL, Wait: defaultLoadLockWait})
	if err != nil {
		return err
	}
	if isNilFusionDependency(lock) {
		return fmt.Errorf("%w: delayed save received nil record lock", ErrDelayTaskDependenciesNotConfigured)
	}
	defer func() { releaseRecordLock(ctx, lock, &err) }()
	var revision string
	if convergeTTL {
		revision, err = h.redisRevisionReader.ReadRecordRevision(ctx, spec, key)
		if err != nil {
			return err
		}
	}
	doc, exists, err := h.redisLoader.LoadDocument(ctx, spec, key)
	if err != nil {
		return err
	}
	if err := storage.ValidateDocumentResult(doc, exists); err != nil {
		return fmt.Errorf("fusion: delayed save redis document loader: %w", err)
	}
	if !exists {
		return fmt.Errorf("fusion: delayed save redis document: %w", storage.ErrNotExists)
	}
	if err := h.mongoReplace.ReplaceDocument(ctx, spec, key, doc); err != nil {
		return err
	}
	if !convergeTTL {
		return nil
	}
	applied, _, err := h.redisRevisionTTL.ApplyTTLWithRevision(ctx, spec, key, revision, spec.Storage.DelayDelete.Duration)
	if err != nil || applied {
		return err
	}
	return h.requeueSave(ctx, spec, key)
}

func (h *StorageTaskHandler) requeueSave(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if isNilFusionDependency(h.delayedSaveQueue) {
		return fmt.Errorf("%w: delayed save requeue requires delay queue", ErrDelayTaskDependenciesNotConfigured)
	}
	return h.delayedSaveQueue.Enqueue(ctx, DelayedSave{
		Type:     spec.Type.Ref,
		Key:      key,
		RunAfter: spec.Storage.DelaySave.Duration,
	})
}

func (h *StorageTaskHandler) lookup(typeName string) (meta.TypeSpec, bool) {
	if isNilFusionDependency(h.types) {
		return meta.TypeSpec{}, false
	}
	return h.types.Lookup(meta.TypeRef{FullName: typeName})
}
