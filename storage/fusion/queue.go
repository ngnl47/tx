package fusion

import (
	"context"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// DelayedOperationKind 描述延迟队列中的落地动作。
type DelayedOperationKind string

const (
	// DelayedSave 表示延迟保存 MongoDB 持久化数据。
	DelayedSave DelayedOperationKind = "save"
	// DelayedDelete 表示延迟删除 MongoDB 持久化数据。
	DelayedDelete DelayedOperationKind = "delete"
)

// DelayedOperation 是 fusion 模式写入 MongoDB 前的队列任务。
type DelayedOperation struct {
	Kind     DelayedOperationKind
	Type     meta.TypeRef
	ID       any
	Changes  change.ChangeSet
	RunAfter time.Duration
}

// DelayQueue 定义 fusion 模式使用的延迟保存/删除队列。
type DelayQueue interface {
	Enqueue(context.Context, DelayedOperation) error
}
