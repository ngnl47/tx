package mongo

import (
	"context"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Updater 是 MongoDB 按 ID 更新所需的最小接口。
//
// runtime 基建不直接依赖具体驱动；官方 MongoDB driver 适配见 storage/mongo/mongodriver。
type Updater interface {
	UpdateByID(context.Context, string, any, map[string]any) error
}

// FieldUpdater 是 MongoDB 按业务字段更新所需的最小接口。
type FieldUpdater interface {
	UpdateByField(context.Context, string, string, any, map[string]any) error
}

// Replacer 是 MongoDB 按 ID 整文档替换所需的最小接口。
type Replacer interface {
	ReplaceByID(context.Context, string, any, map[string]any) error
}

// FieldReplacer 是 MongoDB 按业务字段整文档替换所需的最小接口。
type FieldReplacer interface {
	ReplaceByField(context.Context, string, string, any, map[string]any) error
}

// Deleter 是 MongoDB 按 ID 删除所需的最小接口。
type Deleter interface {
	DeleteByID(context.Context, string, any) error
}

// FieldDeleter 是 MongoDB 按业务字段删除所需的最小接口。
type FieldDeleter interface {
	DeleteByField(context.Context, string, string, any) error
}

// Finder 是 MongoDB 按 ID 读取所需的最小接口。
type Finder interface {
	FindByID(context.Context, string, any, ...string) (map[string]any, bool, error)
}

// FieldFinder 是 MongoDB 按业务字段读取所需的最小接口。
type FieldFinder interface {
	FindByField(context.Context, string, string, any, ...string) (map[string]any, bool, error)
}

// BatchFinder 是 MongoDB 按 ID 批量读取所需的最小接口。
type BatchFinder interface {
	FindManyByID(context.Context, string, []any, ...string) ([]map[string]any, []bool, error)
}

// FieldBatchFinder 是 MongoDB 按业务字段批量读取所需的最小接口。
type FieldBatchFinder interface {
	FindManyByField(context.Context, string, string, []any, ...string) ([]map[string]any, []bool, error)
}

// Adapter 把 ChangeSet 应用到 MongoDB 更新文档。
type Adapter struct {
	client   any
	resolver change.FieldResolver
}

// NewAdapter 创建 Mongo ChangeSet 适配器。
func NewAdapter(client any) *Adapter {
	return &Adapter{client: client}
}

// NewAdapterWithResolver 创建带字段编号解析器的 Mongo ChangeSet 适配器。
func NewAdapterWithResolver(client any, resolver change.FieldResolver) *Adapter {
	return &Adapter{client: client, resolver: resolver}
}

// ApplyChangesWithOptions 根据 Mongo 元数据和记录 ID 执行更新。
func (a *Adapter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet, _ storage.ApplyOptions) error {
	if set.Empty() {
		return nil
	}
	update := BuildUpdateDocumentWithResolver(set, a.resolver)
	if len(update) == 0 {
		return nil
	}
	collection := mongoCollection(spec)
	if field, ok := mongoBusinessIDField(spec); ok {
		updater, ok := a.client.(FieldUpdater)
		if !ok {
			return nil
		}
		return updater.UpdateByField(ctx, collection, field, key.ID, update)
	}
	updater, ok := a.client.(Updater)
	if !ok {
		return nil
	}
	return updater.UpdateByID(ctx, collection, key.ID, update)
}

// ReplaceDocument 按记录键替换一条 Mongo 文档。
func (a *Adapter) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	if doc == nil {
		return nil
	}
	collection := mongoCollection(spec)
	if field, ok := mongoBusinessIDField(spec); ok {
		replacer, ok := a.client.(FieldReplacer)
		if !ok {
			return nil
		}
		return replacer.ReplaceByField(ctx, collection, field, key.ID, doc)
	}
	replacer, ok := a.client.(Replacer)
	if !ok {
		return nil
	}
	return replacer.ReplaceByID(ctx, collection, key.ID, doc)
}

// ApplyDelete 按记录键删除一条 Mongo 文档。
func (a *Adapter) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	collection := mongoCollection(spec)
	if field, ok := mongoBusinessIDField(spec); ok {
		deleter, ok := a.client.(FieldDeleter)
		if !ok {
			return nil
		}
		return deleter.DeleteByField(ctx, collection, field, key.ID)
	}
	deleter, ok := a.client.(Deleter)
	if !ok {
		return nil
	}
	return deleter.DeleteByID(ctx, collection, key.ID)
}

// LoadDocument 按记录键读取单条 Mongo 文档。
func (a *Adapter) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	collection := mongoCollection(spec)
	if field, ok := mongoBusinessIDField(spec); ok {
		finder, ok := a.client.(FieldFinder)
		if !ok {
			return nil, false, nil
		}
		return finder.FindByField(ctx, collection, field, key.ID, fields...)
	}
	finder, ok := a.client.(Finder)
	if !ok {
		return nil, false, nil
	}
	return finder.FindByID(ctx, collection, key.ID, fields...)
}

// LoadDocuments 按记录键批量读取 Mongo 文档。
func (a *Adapter) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(keys))
	exists := make([]bool, len(keys))
	if len(keys) == 0 {
		return docs, exists, nil
	}
	collection := mongoCollection(spec)
	if field, ok := mongoBusinessIDField(spec); ok {
		if finder, ok := a.client.(FieldBatchFinder); ok {
			values := make([]any, len(keys))
			for i, key := range keys {
				values[i] = key.ID
			}
			return finder.FindManyByField(ctx, collection, field, values, fields...)
		}
		singleFinder, ok := a.client.(FieldFinder)
		if !ok {
			return docs, exists, nil
		}
		for i, key := range keys {
			doc, found, err := singleFinder.FindByField(ctx, collection, field, key.ID, fields...)
			if err != nil {
				return nil, nil, err
			}
			docs[i] = doc
			exists[i] = found
		}
		return docs, exists, nil
	}
	finder, ok := a.client.(BatchFinder)
	if ok {
		ids := make([]any, len(keys))
		for i, key := range keys {
			ids[i] = key.ID
		}
		return finder.FindManyByID(ctx, collection, ids, fields...)
	}
	singleFinder, ok := a.client.(Finder)
	if !ok {
		return docs, exists, nil
	}
	for i, key := range keys {
		doc, found, err := singleFinder.FindByID(ctx, collection, key.ID, fields...)
		if err != nil {
			return nil, nil, err
		}
		docs[i] = doc
		exists[i] = found
	}
	return docs, exists, nil
}

func mongoCollection(spec meta.TypeSpec) string {
	if spec.Mongo != nil && spec.Mongo.Collection != "" {
		return spec.Mongo.Collection
	}
	return spec.Type.Ref.Name
}

func mongoBusinessIDField(spec meta.TypeSpec) (string, bool) {
	if spec.Mongo == nil || !spec.Mongo.UseMongoObjectID || spec.Mongo.IDField == "" {
		return "", false
	}
	return spec.Mongo.IDField, true
}
