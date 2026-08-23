package storage

import (
	"errors"
	"fmt"
)

var (
	ErrMissingValue       = errors.New("storage: missing value")
	ErrNotExists          = errors.New("storage: not exists")
	ErrPathTypeMismatch   = errors.New("storage: document path type mismatch")
	ErrNilChangeApplier   = errors.New("storage: change applier is nil")
	ErrNilChangeStore     = errors.New("storage: change store is nil")
	ErrNilDocument        = errors.New("storage: document is nil")
	ErrInvalidRecordKey   = errors.New("storage: invalid record key")
	ErrNoEffectiveChanges = errors.New("storage: changes contain no effective write")
)

// Backend 标识外部持久化依赖，仅用于健康状态分类；存储模式仍由 meta.StorageMode 定义。
type Backend uint8

const (
	BackendUnknown Backend = iota
	BackendRedis
	BackendMongo
)

func (backend Backend) String() string {
	switch backend {
	case BackendRedis:
		return "redis"
	case BackendMongo:
		return "mongo"
	default:
		return "unknown"
	}
}

// BackendError 标记真实外部后端调用返回的错误。
// 校验、映射和调用方 context 错误不得使用该包装。
type BackendError struct {
	Backend   Backend
	Operation string
	Err       error
}

func (err *BackendError) Error() string {
	if err == nil {
		return "storage: backend error"
	}
	if err.Operation == "" {
		return fmt.Sprintf("storage: %s backend: %v", err.Backend, err.Err)
	}
	return fmt.Sprintf("storage: %s backend %s: %v", err.Backend, err.Operation, err.Err)
}

func (err *BackendError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// NewBackendError 包装一次失败的外部后端调用；cause 为 nil 时仍返回 nil。
func NewBackendError(backend Backend, operation string, err error) error {
	if err == nil {
		return nil
	}
	if backend != BackendRedis && backend != BackendMongo {
		return fmt.Errorf("storage: invalid backend %d for %s: %w", backend, operation, err)
	}
	return &BackendError{Backend: backend, Operation: operation, Err: err}
}

// BackendFromError 从 err 中提取第一个已经分类的外部后端。
func BackendFromError(err error) (Backend, bool) {
	var backendErr *BackendError
	if !errors.As(err, &backendErr) || backendErr == nil {
		return BackendUnknown, false
	}
	return backendErr.Backend, true
}
