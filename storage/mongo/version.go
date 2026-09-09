package mongo

import (
	"context"
	"fmt"
	"math"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"go.mongodb.org/mongo-driver/bson"
)

// VersionedChangeApplier 为已有文档提供版本预读与原子条件增量更新。
// 与创建分离，调用方负责根对象互斥及失败后保留变更。
type VersionedChangeApplier interface {
	ApplyVersionedChanges(context.Context, meta.TypeSpec, storage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver) error
}

// ApplyVersionedChanges 对已有文档执行乐观锁增量更新；调用方须在根对象锁内调用。
// 失败不清理变更，下次由调用方携带最新未确认变更重新执行，不在内部重投旧快照。
func (a *Adapter) ApplyVersionedChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver) error {
	if err := change.ValidateSource(source); err != nil {
		return err
	}
	if !source.HasChanges() {
		return nil
	}
	version, err := a.ReadDocumentVersion(ctx, spec, key)
	if err != nil {
		return err
	}
	if !version.Exists {
		return storage.ErrNotExists
	}
	update, err := buildUpdateDocument(spec, source, resolver, values, false)
	if err != nil {
		return err
	}
	if len(update) == 0 {
		return storage.ErrNoEffectiveChanges
	}
	set, _ := update["$set"].(map[string]any)
	if set == nil {
		set = make(map[string]any)
		update["$set"] = set
	}
	set["_tx.version"] = version.Value + 1
	client, err := a.clientFor()
	if err != nil {
		return err
	}
	backend, ok := client.(DurableUpdater)
	if !ok {
		return fmt.Errorf("mongo: client does not support durable updates")
	}
	result, err := backend.UpdateOneDurable(ctx, mongoCollection(spec), documentVersionFilter(spec, key, version), update, storage.ApplyOptions{})
	if err != nil {
		return err
	}
	return validateFencedUpdateResult(result, false)
}

// ApplyVersionedChanges 在选定的 Mongo 分片内完成版本读取与增量写回。
func (c *Cluster) ApplyVersionedChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver) error {
	a, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return a.ApplyVersionedChanges(ctx, spec, key, source, resolver, values)
}

// VersionedClient 提供持久化确认的插入和禁止 upsert 的条件全文替换。
// 首次创建依赖记录标识的唯一索引；并发创建冲突必须返回错误，不能覆盖已有文档。
type VersionedClient interface {
	InsertDocument(context.Context, string, map[string]any) error
	ReplaceOneConditional(context.Context, string, Filter, map[string]any) (ReplaceResult, error)
}

func (a *Adapter) ReadDocumentVersion(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (storage.DocumentVersion, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return storage.DocumentVersion{}, err
	}
	for _, field := range spec.Type.Fields {
		if spec.BSONName(field.Name) == "_tx" {
			return storage.DocumentVersion{}, fmt.Errorf("mongo: _tx is reserved for storage metadata")
		}
	}
	client, err := a.clientFor()
	if err != nil {
		return storage.DocumentVersion{}, err
	}
	doc, exists, err := client.FindOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), "_tx")
	if err != nil || !exists {
		return storage.DocumentVersion{}, err
	}
	var position struct {
		Version int64 `bson:"version"`
	}
	if value, ok := doc["_tx"]; ok {
		encoded, err := bson.Marshal(value)
		if err != nil {
			return storage.DocumentVersion{}, err
		}
		if err := bson.Unmarshal(encoded, &position); err != nil {
			return storage.DocumentVersion{}, err
		}
	}
	if position.Version < 0 || position.Version == math.MaxInt64 {
		return storage.DocumentVersion{}, fmt.Errorf("mongo: invalid or exhausted document version %d", position.Version)
	}
	return storage.DocumentVersion{Exists: true, Value: position.Version}, nil
}

func (a *Adapter) ReplaceDocumentAtVersion(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, version storage.DocumentVersion) error {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	if doc == nil {
		return storage.ErrNilDocument
	}
	if version.Value < 0 || version.Value == math.MaxInt64 || (!version.Exists && version.Value != 0) {
		return fmt.Errorf("mongo: invalid document version %+v", version)
	}
	for _, field := range spec.Type.Fields {
		if spec.BSONName(field.Name) == "_tx" {
			return fmt.Errorf("mongo: _tx is reserved for storage metadata")
		}
	}
	client, err := a.clientFor()
	if err != nil {
		return err
	}
	backend, ok := client.(VersionedClient)
	if !ok {
		return fmt.Errorf("mongo: client does not support versioned replacement")
	}
	document, err := bsonDocumentFromCanonical(spec, doc)
	if err != nil {
		return err
	}
	document["_tx"] = bson.M{"version": version.Value + 1}
	if !version.Exists {
		return backend.InsertDocument(ctx, mongoCollection(spec), document)
	}
	result, err := backend.ReplaceOneConditional(ctx, mongoCollection(spec), documentVersionFilter(spec, key, version), document)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 || result.UpsertedCount != 0 {
		return ErrFenceRejected
	}
	return nil
}

func documentVersionFilter(spec meta.TypeSpec, key storage.RecordKey, version storage.DocumentVersion) Filter {
	filter := mongoRecordFilter(spec, key)
	var expected any = version.Value
	if version.Value == 0 {
		expected = bson.M{"$in": bson.A{nil, int64(0)}}
	}
	filter.Conditions = append(filter.Conditions, Equality{Field: "_tx.version", Value: expected})
	return filter
}

func (c *Cluster) ReadDocumentVersion(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (storage.DocumentVersion, error) {
	a, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return storage.DocumentVersion{}, err
	}
	return a.ReadDocumentVersion(ctx, spec, key)
}

func (c *Cluster) ReplaceDocumentAtVersion(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, version storage.DocumentVersion) error {
	a, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return a.ReplaceDocumentAtVersion(ctx, spec, key, doc, version)
}
