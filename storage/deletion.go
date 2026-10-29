package storage

import (
	"context"
	"github.com/ngnl47/tx/meta"
)

// DeletionMarker 在热存储保留删除意图，直到冷存储删除成功；标记不自动过期。
// 普通读取返回 ErrDeletionPending，避免被解释为可回源的缓存未命中。
type DeletionMarker interface {
	MarkDeletion(context.Context, meta.TypeSpec, RecordKey) error
}
