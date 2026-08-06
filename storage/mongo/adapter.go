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
	if options.Upsert {
		deltas = withoutUpsertIDDelta(deltas, resolver)
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

// withoutUpsertIDDelta 移除首次创建时由等值 filter 自动提供的 Mongo _id。
// 常规更新仍由 BuildUpdateDocumentWithDeltas 拒绝修改 _id。
func withoutUpsertIDDelta(deltas change.DeltaSet, resolver change.FieldNameResolver) change.DeltaSet {
	out := make(change.DeltaSet, 0, len(deltas))
	for _, delta := range deltas {
		bsonPath := delta.Path.FullPath(resolver, change.NameBackendBSON)
		if len(bsonPath) == 1 && bsonPath[0] == "_id" && (delta.Kind == change.DeltaKindSet || delta.Kind == change.DeltaKindReplace) {
			continue
		}
		out = append(out, delta)
	}
	return out
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
	bsonDocument, err := bsonDocumentFromCanonical(spec, doc)
	if err != nil {
		return err
	}
	return replacer.ReplaceOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), bsonDocument)
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
	document, exists, err := finder.FindOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), mongoProjectionFields(spec, fields)...)
	if err != nil {
		return nil, false, err
	}
	if err := storage.ValidateDocumentResult(document, exists); err != nil {
		return nil, false, fmt.Errorf("mongo: finder: %w", err)
	}
	if !exists {
		return nil, false, nil
	}
	canonical, err := canonicalDocumentFromBSON(spec, document, fields)
	if err != nil {
		return nil, false, err
	}
	return canonical, true, nil
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
		documents, exists, err := finder.FindMany(ctx, collection, mongoRecordValuesFilter(spec, keys), projection...)
		if err != nil {
			return nil, nil, err
		}
		if len(documents) != len(keys) || len(exists) != len(keys) {
			return nil, nil, fmt.Errorf("mongo: batch finder returned documents=%d exists=%d for keys=%d", len(documents), len(exists), len(keys))
		}
		if err := storage.ValidateDocumentResults(documents, exists); err != nil {
			return nil, nil, fmt.Errorf("mongo: batch finder: %w", err)
		}
		for i, found := range exists {
			if !found {
				continue
			}
			canonical, err := canonicalDocumentFromBSON(spec, documents[i], fields)
			if err != nil {
				return nil, nil, err
			}
			documents[i] = canonical
		}
		return documents, exists, nil
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
		if err := storage.ValidateDocumentResult(doc, found); err != nil {
			return nil, nil, fmt.Errorf("mongo: finder result at index %d: %w", i, err)
		}
		if found {
			doc, err = canonicalDocumentFromBSON(spec, doc, fields)
			if err != nil {
				return nil, nil, err
			}
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
