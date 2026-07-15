package storage

import (
	"context"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// ChangeApplier 定义消费 ChangeSet 的落库适配器。
//
// Redis、Mongo、Fusion 都实现这个接口后，生成 Store 无需关心具体后端。
type ChangeApplier interface {
	ApplyChanges(context.Context, meta.TypeSpec, RecordKey, change.ChangeSet) error
}

// ChangeDocument 是以点分隔字段路径为 key 的通用变更文档。
type ChangeDocument map[string]any

// ChangeSetToDocument 把字段变更集合转换成通用文档。
//
// 示例：Path{"items", "1", "quantity"} 会变成
// "items.1.quantity"，便于后续转换为 Mongo `$set` 或 Redis hash 字段。
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
