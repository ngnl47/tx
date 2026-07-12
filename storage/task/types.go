package task

import (
	"context"
	"time"
)

type DelayTask struct {
	Type      string
	ID        string
	ShardKey  string
	KeySuffix string
	Action    string
	DueAt     time.Time
}

func (t DelayTask) IdempotencyKey() string {
	key := t.Type + ":" + t.ID + ":" + t.ShardKey + ":" + t.Action
	if t.KeySuffix != "" {
		key += ":" + t.KeySuffix
	}
	return key
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
