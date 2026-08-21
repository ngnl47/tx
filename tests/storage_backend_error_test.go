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

func TestStorageBackendErrorPreservesRequestCancellation(t *testing.T) {
	err := storage.NewBackendError(storage.BackendMongo, "find", context.Canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BackendError = %v, want context cancellation", err)
	}
}

func TestStorageBackendErrorRejectsUnknownDependency(t *testing.T) {
	if err := storage.NewBackendError(storage.BackendUnknown, "write", errors.New("boom")); err == nil {
		t.Fatal("NewBackendError returned nil for unknown backend")
	}
}
