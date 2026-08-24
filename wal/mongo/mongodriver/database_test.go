package mongodriver

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/storage"
)

func TestDatabaseRejectsMissingMongoDatabase(t *testing.T) {
	database := NewDatabase(nil)
	if err := database.EnsureIndexes(context.Background(), "tx_wal"); err == nil {
		t.Fatal("EnsureIndexes error = nil, want missing database error")
	}
}

func TestWALWriteConcernRequiresMajorityJournal(t *testing.T) {
	if walWriteConcern.W != "majority" {
		t.Fatalf("write concern W = %v, want majority", walWriteConcern.W)
	}
	if walWriteConcern.Journal == nil || !*walWriteConcern.Journal {
		t.Fatalf("write concern Journal = %v, want true", walWriteConcern.Journal)
	}
}

func TestMongoBackendErrorDoesNotClassifyCallerContextError(t *testing.T) {
	err := mongoBackendError("read WAL records", context.DeadlineExceeded)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mongoBackendError = %v, want context.DeadlineExceeded", err)
	}
	if backend, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError = %v, true, want unclassified caller context error", backend)
	}
}
