package task

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidDelayTask = errors.New("task: invalid delayed task")

type DelayTask struct {
	Type      string
	ID        any
	ShardKey  any
	KeySuffix string
	Action    string
	DueAt     time.Time
}

// ValidateDelayTask 校验延迟任务的稳定定位字段。ShardKey 与 DueAt 由不同后端
// 自行解释，允许为空；Type、ID 和 Action 缺失会让任务无法可靠消费或去重。
func ValidateDelayTask(item DelayTask) error {
	if strings.TrimSpace(item.Type) == "" {
		return fmt.Errorf("%w: missing type", ErrInvalidDelayTask)
	}
	if isNilTaskID(item.ID) {
		return fmt.Errorf("%w: missing id", ErrInvalidDelayTask)
	}
	if value, ok := item.ID.(string); ok && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: empty string id", ErrInvalidDelayTask)
	}
	if strings.TrimSpace(item.Action) == "" {
		return fmt.Errorf("%w: missing action", ErrInvalidDelayTask)
	}
	return nil
}

func isNilTaskID(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
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
