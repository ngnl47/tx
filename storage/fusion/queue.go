package fusion

import (
	"context"
	"fmt"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/task"
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
	Kind      DelayedOperationKind
	Type      meta.TypeRef
	Key       storage.RecordKey
	ID        any
	Changes   change.ChangeSet
	RunAfter  time.Duration
	KeySuffix string
}

// DelayQueue 定义 fusion 模式使用的延迟保存/删除队列。
type DelayQueue interface {
	Enqueue(context.Context, DelayedOperation) error
}

type TaskQueueOptions struct {
	Now func() time.Time
}

type TaskQueue struct {
	backend task.DelayTaskBackend
	now     func() time.Time
}

func NewTaskQueue(backend task.DelayTaskBackend) *TaskQueue {
	return NewTaskQueueWithOptions(backend, TaskQueueOptions{})
}

func NewTaskQueueWithOptions(backend task.DelayTaskBackend, options TaskQueueOptions) *TaskQueue {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &TaskQueue{backend: backend, now: now}
}

func (q *TaskQueue) Enqueue(ctx context.Context, op DelayedOperation) error {
	if q == nil || q.backend == nil {
		return nil
	}
	return q.backend.Enqueue(ctx, task.DelayTask{
		Type:      delayedTaskType(op.Type),
		ID:        delayedTaskValue(op.Key.ID),
		ShardKey:  delayedTaskValue(op.Key.ShardKey),
		KeySuffix: op.KeySuffix,
		Action:    string(op.Kind),
		DueAt:     q.now().Add(op.RunAfter),
	})
}

func delayedTaskType(ref meta.TypeRef) string {
	switch {
	case ref.FullName != "":
		return ref.FullName
	case ref.ImportPath != "" && ref.Name != "":
		return ref.ImportPath + "." + ref.Name
	case ref.Package != "" && ref.Name != "":
		return ref.Package + "." + ref.Name
	default:
		return ref.Name
	}
}

func delayedTaskValue(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
