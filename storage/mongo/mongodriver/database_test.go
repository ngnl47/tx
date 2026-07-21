package mongodriver

import (
	"context"
	"strings"
	"testing"
)

func TestDatabaseReturnsErrorWhenMongoDatabaseMissing(t *testing.T) {
	db := NewDatabase(nil)

	if err := db.UpdateByID(context.Background(), "roles", int64(1001), map[string]any{"$set": map[string]any{"name": "alice"}}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("UpdateByID error = %v, want missing database error", err)
	}
	if err := db.ReplaceByID(context.Background(), "roles", int64(1001), map[string]any{"name": "alice"}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("ReplaceByID error = %v, want missing database error", err)
	}
	if err := db.DeleteByID(context.Background(), "roles", int64(1001)); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("DeleteByID error = %v, want missing database error", err)
	}
	if _, _, err := db.FindByID(context.Background(), "roles", int64(1001)); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("FindByID error = %v, want missing database error", err)
	}
	if _, _, err := db.FindManyByID(context.Background(), "roles", []any{int64(1001)}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("FindManyByID error = %v, want missing database error", err)
	}
}
