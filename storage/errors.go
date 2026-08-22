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

// Backend identifies an external persistence dependency. It is used only for
// health classification; storage modes remain defined by meta.StorageMode.
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

// BackendError marks an error returned by an actual external backend call.
// Validation, mapping and caller-context errors must not use this wrapper.
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

// NewBackendError wraps one failed external backend call. A nil cause remains nil.
func NewBackendError(backend Backend, operation string, err error) error {
	if err == nil {
		return nil
	}
	if backend != BackendRedis && backend != BackendMongo {
		return fmt.Errorf("storage: invalid backend %d for %s: %w", backend, operation, err)
	}
	return &BackendError{Backend: backend, Operation: operation, Err: err}
}

// BackendFromError extracts the first classified external backend from err.
func BackendFromError(err error) (Backend, bool) {
	var backendErr *BackendError
	if !errors.As(err, &backendErr) || backendErr == nil {
		return BackendUnknown, false
	}
	return backendErr.Backend, true
}
