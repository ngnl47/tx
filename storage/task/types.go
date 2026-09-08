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

var (
	ErrInvalidDelayTask      = errors.New("task: invalid delayed task")
	ErrInvalidDelayTaskLease = errors.New("task: invalid delayed task lease")
	ErrLeaseLost             = errors.New("task: delayed task lease lost")
)

type DelayTask struct {
	Type      string
	ID        any
	ShardKey  any
	KeySuffix string
	Action    string
	DueAt     time.Time
}

// ValidateDelayTask 校验延迟任务的稳定定位字段。ID 和可选的 ShardKey
// 只接受内置 string/整数标量，保证任务持久化后仍能恢复原始键类型；
// DueAt 由不同后端自行解释，Type、ID 和 Action 缺失会让任务无法可靠消费或去重。
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
	if err := validateTaskScalar(item.ID); err != nil {
		return fmt.Errorf("%w: invalid id: %v", ErrInvalidDelayTask, err)
	}
	if !isNilTaskID(item.ShardKey) {
		if err := validateTaskScalar(item.ShardKey); err != nil {
			return fmt.Errorf("%w: invalid shard key: %v", ErrInvalidDelayTask, err)
		}
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
	parts := []string{t.Type, taskScalarIdentity(t.ID), taskScalarIdentity(t.ShardKey), t.Action}
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

// ValidateDelayTaskLease 校验 Complete 与 Retry 使用的租约定位信息。
// 这里仅拒绝无法定位的调用；租约所有权由后端判断。
func ValidateDelayTaskLease(lease DelayTaskLease) error {
	if err := ValidateDelayTask(lease.Task); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDelayTaskLease, err)
	}
	if strings.TrimSpace(lease.Token) == "" {
		return fmt.Errorf("%w: missing token", ErrInvalidDelayTaskLease)
	}
	if lease.Attempts <= 0 {
		return fmt.Errorf("%w: attempts must be greater than 0", ErrInvalidDelayTaskLease)
	}
	return nil
}

// DelayTaskBackend 管理可重试的延迟任务。ClaimDue 允许同时返回已成功取得的
// lease 和非 nil error；调用方必须先结算这些 lease，再处理本轮 claim 错误。
type DelayTaskBackend interface {
	Enqueue(ctx context.Context, task DelayTask) error
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]DelayTaskLease, error)
	Complete(ctx context.Context, lease DelayTaskLease) error
	// Retry 在租约已失效时返回 ErrLeaseLost，不修改当前所有者的任务。
	Retry(ctx context.Context, lease DelayTaskLease, next time.Time, reason error) error
}
