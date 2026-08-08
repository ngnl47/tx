package mongodriver

import (
	"context"
	"strings"
	"testing"

	"github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestDatabaseReturnsErrorWhenMongoDatabaseMissing(t *testing.T) {
	db := NewDatabase(nil)

	if _, err := db.UpdateOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}, map[string]any{"$set": map[string]any{"name": "alice"}}, storage.ApplyOptions{}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("UpdateOne error = %v, want missing database error", err)
	}
	if _, err := db.ReplaceOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}, map[string]any{"name": "alice"}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("ReplaceOne error = %v, want missing database error", err)
	}
	if _, err := db.DeleteOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("DeleteOne error = %v, want missing database error", err)
	}
	if _, _, err := db.FindOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("FindOne error = %v, want missing database error", err)
	}
	if _, _, err := db.FindMany(context.Background(), "roles", txmongo.ValuesFilter{Field: "_id", Values: []any{int64(1001)}}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("FindMany error = %v, want missing database error", err)
	}
}

func TestProjectionWithRequiredFieldIncludesFilterField(t *testing.T) {
	fields := []string{"name"}
	projection := projectionWithRequiredField(fields, "roleId")

	if len(projection) != 2 || projection["name"] != 1 || projection["roleId"] != 1 {
		t.Fatalf("projection = %#v, want name and roleId", projection)
	}
	if len(fields) != 1 || fields[0] != "name" {
		t.Fatalf("fields = %#v, want original caller fields unchanged", fields)
	}
}

func TestProjectionWithRequiredFieldKeepsFullDocumentProjectionUnset(t *testing.T) {
	if projection := projectionWithRequiredField(nil, "roleId"); projection != nil {
		t.Fatalf("projection = %#v, want nil for full document", projection)
	}
}
