package mongo

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Filter 描述一条 Mongo 文档的等值过滤条件。
//
// runtime 基建不直接依赖具体驱动；官方 MongoDB driver 适配见 storage/mongo/mongodriver。
type Filter struct {
	Field string
	Value any
}

// ValuesFilter 描述一批 Mongo 文档的等值集合过滤条件。
type ValuesFilter struct {
	Field  string
	Values []any
}

// Updater 是 MongoDB 更新所需的最小接口。
type Updater interface {
	UpdateOne(context.Context, string, Filter, map[string]any, storage.ApplyOptions) error
}

// Replacer 是 MongoDB 整文档替换所需的最小接口。
type Replacer interface {
	ReplaceOne(context.Context, string, Filter, map[string]any) error
}

// Deleter 是 MongoDB 删除所需的最小接口。
type Deleter interface {
	DeleteOne(context.Context, string, Filter) error
}

// Finder 是 MongoDB 读取所需的最小接口。
type Finder interface {
	FindOne(context.Context, string, Filter, ...string) (map[string]any, bool, error)
}

// BatchFinder 是 MongoDB 批量读取所需的最小接口。
type BatchFinder interface {
	FindMany(context.Context, string, ValuesFilter, ...string) ([]map[string]any, []bool, error)
}

// Adapter 把 DeltaSet 应用到 MongoDB 更新文档。
type Adapter struct {
	client any
}

// NewAdapter 创建 Mongo DeltaSet 适配器。
func NewAdapter(client any) *Adapter {
	return &Adapter{client: client}
}

// ApplyDeltasWithOptions 根据 Mongo 元数据和记录 ID 执行 Delta 更新。
func (a *Adapter) ApplyDeltasWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver, options storage.ApplyOptions) error {
	if len(deltas) == 0 {
		return nil
	}
	update, err := BuildUpdateDocumentWithDeltas(spec, deltas, resolver, values)
	if err != nil {
		return err
	}
	if len(update) == 0 {
		return nil
	}
	updater, ok := a.client.(Updater)
	if !ok {
		return unsupportedClientError("Updater")
	}
	return updater.UpdateOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), update, options)
}

// ReplaceDocument 按记录键替换一条 Mongo 文档。
func (a *Adapter) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	if doc == nil {
		return nil
	}
	replacer, ok := a.client.(Replacer)
	if !ok {
		return unsupportedClientError("Replacer")
	}
	return replacer.ReplaceOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), doc)
}

// ApplyDelete 按记录键删除一条 Mongo 文档。
func (a *Adapter) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	deleter, ok := a.client.(Deleter)
	if !ok {
		return unsupportedClientError("Deleter")
	}
	return deleter.DeleteOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key))
}

// LoadDocument 按记录键读取单条 Mongo 文档。
func (a *Adapter) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	finder, ok := a.client.(Finder)
	if !ok {
		return nil, false, unsupportedClientError("Finder")
	}
	return finder.FindOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), mongoProjectionFields(spec, fields)...)
}

// LoadDocuments 按记录键批量读取 Mongo 文档。
func (a *Adapter) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(keys))
	exists := make([]bool, len(keys))
	if len(keys) == 0 {
		return docs, exists, nil
	}
	collection := mongoCollection(spec)
	projection := mongoProjectionFields(spec, fields)
	finder, ok := a.client.(BatchFinder)
	if ok {
		return finder.FindMany(ctx, collection, mongoRecordValuesFilter(spec, keys), projection...)
	}
	singleFinder, ok := a.client.(Finder)
	if !ok {
		return nil, nil, unsupportedClientError("BatchFinder or Finder")
	}
	for i, key := range keys {
		doc, found, err := singleFinder.FindOne(ctx, collection, mongoRecordFilter(spec, key), projection...)
		if err != nil {
			return nil, nil, err
		}
		docs[i] = doc
		exists[i] = found
	}
	return docs, exists, nil
}

func unsupportedClientError(capability string) error {
	return fmt.Errorf("mongo: adapter client does not implement %s: %w", capability, ErrUnsupportedClient)
}

func mongoCollection(spec meta.TypeSpec) string {
	if spec.Mongo != nil && spec.Mongo.Collection != "" {
		return spec.Mongo.Collection
	}
	return spec.Type.Ref.Name
}

func mongoRecordFilter(spec meta.TypeSpec, key storage.RecordKey) Filter {
	return Filter{Field: mongoRecordFilterField(spec), Value: key.ID}
}

func mongoRecordValuesFilter(spec meta.TypeSpec, keys []storage.RecordKey) ValuesFilter {
	values := make([]any, len(keys))
	for i, key := range keys {
		values[i] = key.ID
	}
	return ValuesFilter{Field: mongoRecordFilterField(spec), Values: values}
}

func mongoRecordFilterField(spec meta.TypeSpec) string {
	if spec.Mongo != nil && spec.Mongo.IDField != "" {
		return spec.BSONName(spec.Mongo.IDField)
	}
	return "_id"
}
