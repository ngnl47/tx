package storage

import (
	"context"

	"github.com/ngnl47/tx/meta"
)

// DocumentVersion 是一次完整文档读取之前取得的持久化基线，不包含集群归属信息。
// Exists 为 false 表示记录不存在；存在但尚无版本字段的记录使用版本 0。
type DocumentVersion struct {
	Exists bool
	Value  int64
}

// VersionedDocumentReplacer 先取得版本，再由调用方读取最新数据并执行条件替换。
// 条件冲突必须返回错误，不能自动改用新版本重写已经读取的旧快照。
type VersionedDocumentReplacer interface {
	ReadDocumentVersion(context.Context, meta.TypeSpec, RecordKey) (DocumentVersion, error)
	ReplaceDocumentAtVersion(context.Context, meta.TypeSpec, RecordKey, map[string]any, DocumentVersion) error
}
