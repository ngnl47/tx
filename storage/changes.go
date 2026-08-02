package storage

import (
	"context"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// ApplyOptions 描述变更落地时可选的通用写入语义。
type ApplyOptions struct {
	Upsert bool
	TTL    int64
}

// ChangeApplier 定义支持通用写入选项的变更落地适配器。
type ChangeApplier interface {
	ApplyChangesWithOptions(context.Context, meta.TypeSpec, RecordKey, change.ChangeSet, ApplyOptions) error
}

// DeltaApplier 定义支持 DeltaSet 直连落地的适配器。
type DeltaApplier interface {
	ApplyDeltasWithOptions(context.Context, meta.TypeSpec, RecordKey, change.DeltaSet, change.FieldNameResolver, ApplyOptions) error
}

// DocumentLoader 定义按记录键读取单条文档的适配器。
type DocumentLoader interface {
	LoadDocument(context.Context, meta.TypeSpec, RecordKey, ...string) (map[string]any, bool, error)
}

// BatchDocumentLoader 定义按记录键批量读取文档的适配器。
type BatchDocumentLoader interface {
	LoadDocuments(context.Context, meta.TypeSpec, []RecordKey, ...string) ([]map[string]any, []bool, error)
}

// DocumentReplacerWithOptions 定义按记录键整文档替换且支持写入选项的适配器。
type DocumentReplacerWithOptions interface {
	ReplaceDocumentWithOptions(context.Context, meta.TypeSpec, RecordKey, map[string]any, ApplyOptions) error
}

// DocumentReplacer 定义按记录键整文档替换的适配器。
type DocumentReplacer interface {
	ReplaceDocument(context.Context, meta.TypeSpec, RecordKey, map[string]any) error
}

// LockOptions 描述记录级分布式锁的等待策略。
type LockOptions struct {
	TTL  time.Duration
	Wait time.Duration
}

// RecordLock 表示已持有的一条记录级锁。
type RecordLock interface {
	Unlock(context.Context) error
}

// RecordLocker 定义按记录键加锁的适配器。
type RecordLocker interface {
	LockRecord(context.Context, meta.TypeSpec, RecordKey, LockOptions) (RecordLock, error)
}

// DeleteApplier 定义按元数据和记录键删除存储记录的适配器。
type DeleteApplier interface {
	ApplyDelete(context.Context, meta.TypeSpec, RecordKey) error
}

// RevisionDeleteApplier 定义带 Redis 生命周期版本校验的删除适配器。
type RevisionDeleteApplier interface {
	ApplyDeleteWithRevision(context.Context, meta.TypeSpec, RecordKey, string) (bool, string, error)
}

// RecordRevisionReader 定义读取存储记录生命周期版本的适配器。
type RecordRevisionReader interface {
	ReadRecordRevision(context.Context, meta.TypeSpec, RecordKey) (string, error)
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
