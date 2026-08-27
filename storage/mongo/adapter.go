package mongo

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Filter 描述一条 Mongo 文档的等值过滤条件。
//
// runtime 基建不直接依赖具体驱动；官方 MongoDB driver 适配见 storage/mongo/mongodriver。
type Filter struct {
	// Field 是记录主键在 BSON 文档中的字段名。
	Field string
	// Value 是主键字段必须匹配的值。
	Value any
	// Conditions 是与主键同时参与原子匹配的附加等值条件。
	Conditions []Equality
}

// Equality 描述附加在记录键之后的等值条件。
type Equality struct {
	// Field 是附加等值条件使用的 BSON 字段名。
	Field string
	// Value 是该字段必须匹配的当前值。
	Value any
}

// Fence 描述一次条件写入必须匹配的当前字段值。
type Fence struct {
	// Path 是条件写入需要校验的结构化字段路径。
	Path change.Path
	// Expected 是该路径在写入发生前必须匹配的当前值。
	Expected any
}

// FencedChangeApplier 是 Mongo 条件更新使用的窄接口。
// 它把记录键与 Fence 合并为同一次原子过滤，不把代际语义扩散到通用存储层。
type FencedChangeApplier interface {
	storage.ChangeApplier
	ApplyChangesWithFence(context.Context, meta.TypeSpec, storage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, Fence, storage.ApplyOptions) error
}

// ValuesFilter 描述一批 Mongo 文档的等值集合过滤条件。
type ValuesFilter struct {
	// Field 是批量读取时使用的主键 BSON 字段名。
	Field string
	// Values 是需要匹配的主键集合；结果顺序由上层存储契约恢复。
	Values []any
}

// UpdateResult 描述 MongoDB 单文档更新结果。
//
// MatchedCount 为匹配到的记录数；UpsertedCount 为本次创建的记录数。
// ModifiedCount 不进入该抽象：把字段写成当前已有值仍是一次成功更新。
type UpdateResult struct {
	// MatchedCount 是本次更新匹配到的现有文档数量。
	MatchedCount int64
	// UpsertedCount 是本次更新新建的文档数量。
	UpsertedCount int64
}

// ReplaceResult 描述 MongoDB 单文档整文档替换结果。
type ReplaceResult struct {
	// MatchedCount 是本次替换匹配到的现有文档数量。
	MatchedCount int64
	// UpsertedCount 是本次替换新建的文档数量。
	UpsertedCount int64
}

// DeleteResult 描述 MongoDB 单文档删除结果。
type DeleteResult struct {
	// DeletedCount 是本次调用实际删除的文档数量。
	DeletedCount int64
}

// Updater 是 MongoDB 更新所需的最小接口。
type Updater interface {
	UpdateOne(context.Context, string, Filter, map[string]any, storage.ApplyOptions) (UpdateResult, error)
}

// Replacer 是 MongoDB 整文档替换所需的最小接口。
type Replacer interface {
	ReplaceOne(context.Context, string, Filter, map[string]any) (ReplaceResult, error)
}

// Deleter 是 MongoDB 删除所需的最小接口。
type Deleter interface {
	DeleteOne(context.Context, string, Filter) (DeleteResult, error)
}

// Finder 是 MongoDB 读取所需的最小接口。
type Finder interface {
	FindOne(context.Context, string, Filter, ...string) (map[string]any, bool, error)
}

// BatchFinder 是 MongoDB 批量读取所需的最小接口。
type BatchFinder interface {
	FindMany(context.Context, string, ValuesFilter, ...string) ([]map[string]any, []bool, error)
}

// IndexManager 创建由生成元数据声明的 Mongo 索引。
type IndexManager interface {
	EnsureIndexes(context.Context, string, []IndexModel) error
}

// Client 是生产 Mongo adapter 所需能力的完整编译期契约。
type Client interface {
	Updater
	Replacer
	Deleter
	Finder
	BatchFinder
}

// Adapter 把规范化变更流应用到 MongoDB 更新文档。
type Adapter struct {
	client Client
}

var _ FencedChangeApplier = (*Adapter)(nil)

// NewAdapter 创建 Mongo 变更适配器。
func NewAdapter(client Client) (*Adapter, error) {
	if isNilMongoClient(client) {
		return nil, ErrClientNotConfigured
	}
	return &Adapter{client: client}, nil
}

// ApplyChangesWithOptions 根据 Mongo 元数据和记录 ID 执行变更更新。
func (a *Adapter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	return a.applyChanges(ctx, spec, key, source, resolver, values, nil, options)
}

// ApplyChangesWithFence 在同一次 Mongo update 中校验记录键和代际字段。
func (a *Adapter) ApplyChangesWithFence(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, fence Fence, options storage.ApplyOptions) error {
	return a.applyChanges(ctx, spec, key, source, resolver, values, &fence, options)
}

func (a *Adapter) applyChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, fence *Fence, options storage.ApplyOptions) error {
	if err := change.ValidateSource(source); err != nil {
		return err
	}
	if !source.HasChanges() {
		return nil
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	update, err := buildUpdateDocument(spec, source, resolver, values, options.Upsert)
	if err != nil {
		return err
	}
	if len(update) == 0 {
		if options.Upsert {
			return fmt.Errorf("mongo: upsert has no mutable changes after excluding immutable record id")
		}
		return storage.ErrNoEffectiveChanges
	}
	client, err := a.clientFor()
	if err != nil {
		return err
	}
	filter := mongoRecordFilter(spec, key)
	if fence != nil {
		condition, err := mongoFenceCondition(*fence, resolver)
		if err != nil {
			return err
		}
		if condition.Field == filter.Field {
			return fmt.Errorf("mongo: fence field %q duplicates record key", condition.Field)
		}
		filter.Conditions = []Equality{condition}
	}
	result, err := client.UpdateOne(ctx, mongoCollection(spec), filter, update, options)
	if err != nil {
		return err
	}
	if fence != nil {
		return validateFencedUpdateResult(result, options.Upsert)
	}
	return validateUpdateResult(result, options.Upsert)
}

func mongoFenceCondition(fence Fence, resolver change.FieldNameResolver) (Equality, error) {
	if !fence.Path.Valid() {
		return Equality{}, fmt.Errorf("%w: fence %#v", change.ErrInvalidPath, fence.Path)
	}
	path := fence.Path.FullPath(resolver, change.FieldNameBSON)
	if len(path) == 0 {
		return Equality{}, fmt.Errorf("mongo: cannot resolve BSON fence path for %#v", fence.Path)
	}
	return Equality{Field: path.String(), Value: fence.Expected}, nil
}

// ReplaceDocument 按记录键替换一条 Mongo 文档。
func (a *Adapter) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	if doc == nil {
		return storage.ErrNilDocument
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	client, err := a.clientFor()
	if err != nil {
		return err
	}
	bsonDocument, err := bsonDocumentFromCanonical(spec, doc)
	if err != nil {
		return err
	}
	result, err := client.ReplaceOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), bsonDocument)
	if err != nil {
		return err
	}
	if result.MatchedCount == 1 && result.UpsertedCount == 0 {
		return nil
	}
	if result.MatchedCount == 0 && result.UpsertedCount == 1 {
		return nil
	}
	return fmt.Errorf("mongo: unexpected replace result matched=%d upserted=%d", result.MatchedCount, result.UpsertedCount)
}

// ApplyDelete 按记录键删除一条 Mongo 文档。
func (a *Adapter) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	client, err := a.clientFor()
	if err != nil {
		return err
	}
	result, err := client.DeleteOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key))
	if err != nil {
		return err
	}
	if result.DeletedCount == 0 || result.DeletedCount == 1 {
		return nil
	}
	return fmt.Errorf("mongo: unexpected delete result deleted=%d", result.DeletedCount)
}

func validateUpdateResult(result UpdateResult, upsert bool) error {
	if result.MatchedCount == 1 && result.UpsertedCount == 0 {
		return nil
	}
	if upsert && result.MatchedCount == 0 && result.UpsertedCount == 1 {
		return nil
	}
	if !upsert && result.MatchedCount == 0 && result.UpsertedCount == 0 {
		return storage.ErrNotExists
	}
	return fmt.Errorf("mongo: unexpected update result matched=%d upserted=%d upsert=%t", result.MatchedCount, result.UpsertedCount, upsert)
}

func validateFencedUpdateResult(result UpdateResult, upsert bool) error {
	if result.MatchedCount == 1 && result.UpsertedCount == 0 {
		return nil
	}
	if upsert && result.MatchedCount == 0 && result.UpsertedCount == 1 {
		return nil
	}
	if result.MatchedCount == 0 && result.UpsertedCount == 0 {
		return ErrFenceRejected
	}
	return fmt.Errorf("mongo: unexpected fenced update result matched=%d upserted=%d upsert=%t", result.MatchedCount, result.UpsertedCount, upsert)
}

// LoadDocument 按记录键读取单条 Mongo 文档。
func (a *Adapter) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return nil, false, err
	}
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, false, err
	}
	client, err := a.clientFor()
	if err != nil {
		return nil, false, err
	}
	document, exists, err := client.FindOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), mongoProjectionFields(spec, fields)...)
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
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, nil, err
	}
	if len(keys) == 0 {
		return docs, exists, nil
	}
	if err := storage.ValidateRecordKeys(spec, keys); err != nil {
		return nil, nil, err
	}
	collection := mongoCollection(spec)
	projection := mongoProjectionFields(spec, fields)
	client, err := a.clientFor()
	if err != nil {
		return nil, nil, err
	}
	documents, exists, err := client.FindMany(ctx, collection, mongoRecordValuesFilter(spec, keys), projection...)
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

func (a *Adapter) clientFor() (Client, error) {
	if a == nil || isNilMongoClient(a.client) {
		return nil, ErrClientNotConfigured
	}
	return a.client, nil
}

func isNilMongoClient(client Client) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
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
