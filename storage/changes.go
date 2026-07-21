package storage

import (
	"context"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// ApplyOptions 描述变更落地时可选的通用写入语义。
type ApplyOptions struct {
	Upsert bool
	TTL    int64
}

// ChangeApplierWithOptions 定义支持通用写入选项的变更落地适配器。
type ChangeApplierWithOptions interface {
	ApplyChangesWithOptions(context.Context, meta.TypeSpec, RecordKey, change.ChangeSet, ApplyOptions) error
}

// DeleteApplier 定义按元数据和记录键删除存储记录的适配器。
type DeleteApplier interface {
	ApplyDelete(context.Context, meta.TypeSpec, RecordKey) error
}

// ChangeDocument 是以点分隔字段路径为 key 的通用变更文档。
type ChangeDocument map[string]any

// ChangeSetToDocument 把字段变更集合转换成通用文档。
//
// 示例：Path{"items", "1", "quantity"} 会变成
// "items.1.quantity"，便于后续转换为 Mongo `$set` 或 Redis JSON 文档 patch。
func ChangeSetToDocument(set change.ChangeSet) ChangeDocument {
	return ChangeSetToDocumentWithResolver(set, nil)
}

// ChangeSetToDocumentWithResolver 把字段变更集合转换成通用文档，并用 resolver
// 把 FieldID 解析为字段别名。
//
// 示例：Field=items，Path{"1", "quantity"} 会变成
// "items.1.quantity"。
func ChangeSetToDocumentWithResolver(set change.ChangeSet, resolver change.FieldResolver) ChangeDocument {
	doc := ChangeDocument{}
	for _, fieldChange := range set.Changes() {
		path := fieldChange.FullPath(resolver)
		if len(path) == 0 {
			continue
		}
		doc[path.String()] = fieldChange.Value
	}
	return doc
}
