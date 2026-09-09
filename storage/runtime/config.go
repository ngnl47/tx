package runtime

import (
	"errors"
	"reflect"
)

var (
	// ErrStorageUnavailable 表示目标存储后端当前不可用。
	ErrStorageUnavailable = errors.New("storage runtime: storage unavailable")
	// ErrNotConfigured 表示调用方尚未装配对应存储能力。
	ErrNotConfigured = errors.New("storage runtime: storage is not configured")
	// ErrWorkerNotConfigured 表示 Fusion 延迟任务消费者尚未装配。
	ErrWorkerNotConfigured = errors.New("storage runtime: delayed task worker is not configured")
)

func isNil(value any) bool {
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
