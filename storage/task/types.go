package task

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

type DelayTask struct {
	Type      string
	ID        any
	ShardKey  any
	KeySuffix string
	Action    string
	DueAt     time.Time
}

// IdempotencyKey 根据记录定位和动作生成无歧义的任务标识。
func (t DelayTask) IdempotencyKey() string {
	parts := []string{t.Type, fmt.Sprint(t.ID), fmt.Sprint(t.ShardKey), t.Action}
	if t.KeySuffix != "" {
		parts = append(parts, t.KeySuffix)
	}
	key := make([]byte, 0, 32)
	for _, part := range parts {
		key = strconv.AppendInt(key, int64(len(part)), 10)
		key = append(key, ':')
		key = append(key, part...)
	}
	return string(key)
}

type DelayTaskLease struct {
	Task     DelayTask
	Token    string
	Attempts int
}

type DelayTaskBackend interface {
	Enqueue(ctx context.Context, task DelayTask) error
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]DelayTaskLease, error)
	Complete(ctx context.Context, lease DelayTaskLease) error
	Retry(ctx context.Context, lease DelayTaskLease, next time.Time, reason error) error
}
