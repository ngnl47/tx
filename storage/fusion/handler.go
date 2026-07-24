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
	types        TypeRegistry
	redisLoader  storage.DocumentLoader
	mongoReplace storage.DocumentReplacer
}

func NewDelayTaskHandler(types TypeRegistry, redisLoader storage.DocumentLoader, mongoReplace storage.DocumentReplacer) *StorageTaskHandler {
	return &StorageTaskHandler{
		types:        types,
		redisLoader:  redisLoader,
		mongoReplace: mongoReplace,
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
