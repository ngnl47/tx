package mongo

import (
	"context"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Updater 是 MongoDB 按 ID 更新所需的最小接口。
//
// 官方 MongoDB driver 适配层后续只需要把 UpdateByID 转成这里的签名即可。
type Updater interface {
	UpdateByID(context.Context, string, any, map[string]any) error
}

// Finder 是 MongoDB 按 ID 读取所需的最小接口。
type Finder interface {
	FindByID(context.Context, string, any, ...string) (map[string]any, bool, error)
}

// BatchFinder 是 MongoDB 按 ID 批量读取所需的最小接口。
type BatchFinder interface {
	FindManyByID(context.Context, string, []any, ...string) ([]map[string]any, []bool, error)
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
	updater, ok := a.client.(Updater)
	if !ok || set.Empty() {
		return nil
	}
	update := BuildUpdateDocumentWithResolver(set, a.resolver)
	if len(update) == 0 {
		return nil
	}
	return updater.UpdateByID(ctx, mongoCollection(spec), key.ID, update)
}

// LoadDocument 按记录键读取单条 Mongo 文档。
func (a *Adapter) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	finder, ok := a.client.(Finder)
	if !ok {
		return nil, false, nil
	}
	return finder.FindByID(ctx, mongoCollection(spec), key.ID, fields...)
}

// LoadDocuments 按记录键批量读取 Mongo 文档。
func (a *Adapter) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(keys))
	exists := make([]bool, len(keys))
	if len(keys) == 0 {
		return docs, exists, nil
	}
	finder, ok := a.client.(BatchFinder)
	if ok {
		ids := make([]any, len(keys))
		for i, key := range keys {
			ids[i] = key.ID
		}
		return finder.FindManyByID(ctx, mongoCollection(spec), ids, fields...)
	}
	singleFinder, ok := a.client.(Finder)
	if !ok {
		return docs, exists, nil
	}
	collection := mongoCollection(spec)
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
