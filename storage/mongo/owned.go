package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"go.mongodb.org/mongo-driver/bson"
)

// WritePosition 是框架内部的所有权及写入顺序，不要求 GD 声明对应字段。
// Owner 来自外部已确认的单调所有权代次；Sequence 在该所有权内单调增加。
type WritePosition struct {
	Owner    int64
	Sequence int64
}

// DurableUpdater 为已确认持久化的直接写回提供明确的 Mongo 确认边界。
type DurableUpdater interface {
	UpdateOneDurable(context.Context, string, Filter, map[string]any, storage.ApplyOptions) (UpdateResult, error)
}

// OwnershipStore 是有状态写回使用的窄存储能力，不包含 Actor 或 GD 类型专用逻辑。
type OwnershipStore interface {
	ClaimOwnership(context.Context, meta.TypeSpec, storage.RecordKey, int64) (int64, error)
	ApplyOwnedChanges(context.Context, meta.TypeSpec, storage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, WritePosition, storage.ApplyOptions) error
}

// ClaimOwnership 只允许晋升所有权；同代次重新加载时返回已写入序号，不清零。
// 调用方必须先由控制面确认归属，成功后重新加载业务数据，再开放业务入口。
func (a *Adapter) ClaimOwnership(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, owner int64) (int64, error) {
	if owner <= 0 {
		return 0, errors.New("mongo: owner must be positive")
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return 0, err
	}
	for _, field := range spec.Type.Fields {
		if spec.BSONName(field.Name) == "_tx" {
			return 0, errors.New("mongo: _tx is reserved for storage metadata")
		}
	}
	client, err := a.clientFor()
	if err != nil {
		return 0, err
	}
	durable, ok := client.(DurableUpdater)
	if !ok {
		return 0, errors.New("mongo: client does not support durable updates")
	}
	filter := mongoRecordFilter(spec, key)
	filter.Conditions = append(filter.Conditions, Equality{Field: "_tx.owner", Value: map[string]any{"$not": map[string]any{"$gte": owner}}})
	result, err := durable.UpdateOneDurable(ctx, mongoCollection(spec), filter, map[string]any{"$set": map[string]any{"_tx.owner": owner, "_tx.sequence": int64(0)}}, storage.ApplyOptions{})
	if err != nil {
		return 0, err
	}
	if result.MatchedCount == 1 {
		return 0, nil
	}
	doc, exists, err := client.FindOne(ctx, mongoCollection(spec), mongoRecordFilter(spec, key), "_tx")
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, ErrFenceRejected
	}
	encoded, err := bson.Marshal(doc["_tx"])
	if err != nil {
		return 0, err
	}
	var position WritePosition
	if err := bson.Unmarshal(encoded, &position); err != nil {
		return 0, err
	}
	if position.Owner != owner || position.Sequence < 0 {
		return 0, ErrFenceRejected
	}
	return position.Sequence, nil
}

// ClaimOwnership 将所有权声明交给记录所属分片。
func (c *Cluster) ClaimOwnership(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, owner int64) (int64, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return 0, err
	}
	return adapter.ClaimOwnership(ctx, spec, key, owner)
}

// ApplyOwnedChanges 在一次持久化更新中校验所有权和序号，拒绝迟到旧写入。
// 调用方在不确定失败后必须保留全部未确认变更，再以更大序号重试；不允许独立重投已过期快照。
func (a *Adapter) ApplyOwnedChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, position WritePosition, options storage.ApplyOptions) error {
	if err := change.ValidateSource(source); err != nil {
		return err
	}
	if !source.HasChanges() {
		return nil
	}
	if position.Owner <= 0 || position.Sequence <= 0 {
		return errors.New("mongo: owner and sequence must be positive")
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	if options.Upsert {
		return errors.New("mongo: owned changes require an existing claimed document")
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
	set["_tx.sequence"] = position.Sequence
	filter := mongoRecordFilter(spec, key)
	filter.Conditions = append(filter.Conditions, Equality{Field: "_tx.owner", Value: position.Owner}, Equality{Field: "_tx.sequence", Value: map[string]any{"$lt": position.Sequence}})
	client, err := a.clientFor()
	if err != nil {
		return err
	}
	durable, ok := client.(DurableUpdater)
	if !ok {
		return fmt.Errorf("mongo: client does not support durable updates")
	}
	result, err := durable.UpdateOneDurable(ctx, mongoCollection(spec), filter, update, options)
	if err != nil {
		return err
	}
	return validateFencedUpdateResult(result, false)
}

// ApplyOwnedChanges 把所有权保护写入路由到记录所属的唯一分片。
func (c *Cluster) ApplyOwnedChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, position WritePosition, options storage.ApplyOptions) error {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ApplyOwnedChanges(ctx, spec, key, source, resolver, values, position, options)
}
