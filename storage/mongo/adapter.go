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

// Adapter 把 ChangeSet 应用到 MongoDB 更新文档。
type Adapter struct {
	updater  Updater
	resolver change.FieldResolver
}

// NewAdapter 创建 Mongo ChangeSet 适配器。
func NewAdapter(updater Updater) *Adapter {
	return &Adapter{updater: updater}
}

// NewAdapterWithResolver 创建带字段编号解析器的 Mongo ChangeSet 适配器。
func NewAdapterWithResolver(updater Updater, resolver change.FieldResolver) *Adapter {
	return &Adapter{updater: updater, resolver: resolver}
}

// ApplyChangesWithOptions 根据 Mongo 元数据和记录 ID 执行更新。
func (a *Adapter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet, _ storage.ApplyOptions) error {
	if a.updater == nil || set.Empty() {
		return nil
	}
	update := BuildUpdateDocumentWithResolver(set, a.resolver)
	if len(update) == 0 {
		return nil
	}
	collection := spec.Type.Ref.Name
	if spec.Mongo != nil && spec.Mongo.Collection != "" {
		collection = spec.Mongo.Collection
	}
	return a.updater.UpdateByID(ctx, collection, key.ID, update)
}
