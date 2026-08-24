package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/storage"
)

func TestStorageBackendErrorPreservesDependencyAndCause(t *testing.T) {
	cause := errors.New("connection refused")
	err := storage.NewBackendError(storage.BackendRedis, "eval", cause)
	if !errors.Is(err, cause) {
		t.Fatalf("BackendError = %v, want wrapped cause", err)
	}
	backend, ok := storage.BackendFromError(err)
	if !ok || backend != storage.BackendRedis {
		t.Fatalf("BackendFromError = %v, %t, want redis, true", backend, ok)
	}
}

func TestStorageBackendErrorDoesNotClassifyCallerContextError(t *testing.T) {
	for _, contextErr := range []error{context.Canceled, context.DeadlineExceeded} {
		err := storage.NewBackendError(storage.BackendMongo, "find", contextErr)
		if !errors.Is(err, contextErr) {
			t.Fatalf("NewBackendError = %v, want %v", err, contextErr)
		}
		if backend, ok := storage.BackendFromError(err); ok {
			t.Fatalf("BackendFromError = %v, true, want unclassified caller context error", backend)
		}
	}
}

func TestStorageBackendErrorRejectsUnknownDependency(t *testing.T) {
	if err := storage.NewBackendError(storage.BackendUnknown, "write", errors.New("boom")); err == nil {
		t.Fatal("NewBackendError returned nil for unknown backend")
	}
}
