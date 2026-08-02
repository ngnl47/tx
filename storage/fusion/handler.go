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

func NewDelayTaskHandler(types TypeRegistry, redisLoader storage.DocumentLoader, mongoReplace storage.DocumentReplacer) *StorageTaskHandler {
	return NewDelayTaskHandlerWithOptions(types, redisLoader, mongoReplace, DelayTaskHandlerOptions{})
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
	if h == nil {
		return nil
	}
	spec, ok := h.lookup(item.Type)
	if !ok {
		return fmt.Errorf("fusion: unknown delayed task type %q", item.Type)
	}
	if item.ID == nil {
		return fmt.Errorf("fusion: missing delayed task record id")
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

func (h *StorageTaskHandler) handleSave(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if h.redisLoader == nil || h.mongoReplace == nil {
		return nil
	}
	doc, exists, err := h.redisLoader.LoadDocument(ctx, spec, key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return h.mongoReplace.ReplaceDocument(ctx, spec, key, compactDocument(doc))
}

func (h *StorageTaskHandler) handleDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if h.redisLoader == nil || h.mongoReplace == nil || h.redisRevisionReader == nil || h.redisRevisionDelete == nil || h.redisLocker == nil {
		return nil
	}
	lock, err := h.redisLocker.LockRecord(ctx, spec, key, storage.LockOptions{TTL: defaultLoadLockTTL, Wait: defaultLoadLockWait})
	if err != nil {
		return err
	}
	defer func() { _ = lock.Unlock(ctx) }()

	revision, err := h.redisRevisionReader.ReadRecordRevision(ctx, spec, key)
	if err != nil {
		return err
	}
	doc, exists, err := h.redisLoader.LoadDocument(ctx, spec, key)
	if err != nil || !exists {
		return err
	}
	if err := h.mongoReplace.ReplaceDocument(ctx, spec, key, compactDocument(doc)); err != nil {
		return err
	}
	deleted, _, err := h.redisRevisionDelete.ApplyDeleteWithRevision(ctx, spec, key, revision)
	if err != nil || deleted {
		return err
	}
	return h.requeueDelete(ctx, spec, key)
}

func (h *StorageTaskHandler) requeueDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if h.delayQueue == nil || spec.Storage.DelayDelete.Duration < 0 {
		return nil
	}
	return h.delayQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedDelete,
		Type:     spec.Type.Ref,
		Key:      key,
		RunAfter: delayedDeleteAfter(spec),
	})
}

func (h *StorageTaskHandler) lookup(typeName string) (meta.TypeSpec, bool) {
	if h.types == nil {
		return meta.TypeSpec{}, false
	}
	return h.types.Lookup(meta.TypeRef{FullName: typeName})
}

func compactDocument(doc map[string]any) map[string]any {
	out := make(map[string]any, len(doc))
	for key, value := range doc {
		if value == nil {
			continue
		}
		out[key] = compactValue(value)
	}
	return out
}

func compactValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return compactDocument(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = compactValue(item)
		}
		return out
	default:
		return value
	}
}
