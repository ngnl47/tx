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

type DocumentReplacer interface {
	ReplaceDocument(context.Context, meta.TypeSpec, storage.RecordKey, map[string]any) error
}

type StorageTaskHandler struct {
	types        TypeRegistry
	redisLoader  DocumentLoader
	mongoReplace DocumentReplacer
	mongoDelete  storage.DeleteApplier
}

func NewDelayTaskHandler(types TypeRegistry, redisLoader DocumentLoader, mongoReplace DocumentReplacer, mongoDelete storage.DeleteApplier) *StorageTaskHandler {
	return &StorageTaskHandler{
		types:        types,
		redisLoader:  redisLoader,
		mongoReplace: mongoReplace,
		mongoDelete:  mongoDelete,
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
	return h.mongoReplace.ReplaceDocument(ctx, spec, key, doc)
}

func (h *StorageTaskHandler) handleDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if h.mongoDelete == nil {
		return nil
	}
	return h.mongoDelete.ApplyDelete(ctx, spec, key)
}

func (h *StorageTaskHandler) lookup(typeName string) (meta.TypeSpec, bool) {
	if h.types == nil {
		return meta.TypeSpec{}, false
	}
	return h.types.Lookup(meta.TypeRef{FullName: typeName})
}
