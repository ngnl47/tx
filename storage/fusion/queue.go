package fusion

import (
	"context"
	"time"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/task"
)

const delayedSaveAction = "save"

// DelayedSave 是 Fusion 模式写入 MongoDB 前的延迟保存任务。
// Fusion 不支持物理删除，因此该结构不再暴露多动作状态。
type DelayedSave struct {
	Type     meta.TypeRef
	Key      storage.RecordKey
	RunAfter time.Duration
}

// DelayedSaveQueue 定义 Fusion 模式使用的延迟保存队列。
type DelayedSaveQueue interface {
	Enqueue(context.Context, DelayedSave) error
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

func (q *TaskQueue) Enqueue(ctx context.Context, save DelayedSave) error {
	if q == nil || isNilFusionDependency(q.backend) {
		return ErrTaskBackendNotConfigured
	}
	item := task.DelayTask{
		Type:     delayedTaskType(save.Type),
		ID:       save.Key.ID,
		ShardKey: save.Key.ShardKey,
		Action:   delayedSaveAction,
		DueAt:    q.now().Add(save.RunAfter),
	}
	if err := task.ValidateDelayTask(item); err != nil {
		return err
	}
	return q.backend.Enqueue(ctx, item)
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
