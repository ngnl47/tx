package mongodriver

import (
	"context"
	"testing"
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
