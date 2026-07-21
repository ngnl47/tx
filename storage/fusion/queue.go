package fusion

import (
	"context"
	"fmt"
	"time"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/task"
)

// DelayedOperationKind 描述延迟队列中的落地动作。
type DelayedOperationKind string

const (
	// DelayedSave 表示延迟保存 MongoDB 持久化数据。
	DelayedSave DelayedOperationKind = "save"
)

// DelayedOperation 是 fusion 模式写入 MongoDB 前的延迟保存任务。
type DelayedOperation struct {
	Kind      DelayedOperationKind
	Type      meta.TypeRef
	Key       storage.RecordKey
	RunAfter  time.Duration
	KeySuffix string
}

// DelayQueue 定义 fusion 模式使用的延迟保存队列。
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
	if op.Kind != DelayedSave {
		return fmt.Errorf("fusion: unknown delayed operation kind %q", op.Kind)
	}
	taskType := delayedOperationType(op)
	if taskType == "" {
		return fmt.Errorf("fusion: missing delayed operation type")
	}
	if op.Key.ID == nil {
		return fmt.Errorf("fusion: missing delayed operation record id")
	}
	return q.backend.Enqueue(ctx, task.DelayTask{
		Type:      taskType,
		ID:        op.Key.ID,
		ShardKey:  op.Key.ShardKey,
		KeySuffix: op.KeySuffix,
		Action:    string(op.Kind),
		DueAt:     q.now().Add(op.RunAfter),
	})
}

func delayedOperationType(op DelayedOperation) string {
	if typ := delayedTaskType(op.Type); typ != "" {
		return typ
	}
	return op.Key.TypeName
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
