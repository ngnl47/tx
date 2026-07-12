package tests

import (
	"errors"
	"testing"

	"github.com/ngnl47/gobricks/meta"
	"github.com/ngnl47/gobricks/schema"
)

func TestSchemaDetectsConflictWithAnnotationMetadata(t *testing.T) {
	annotation := codecRoleSpec()
	fromSchema := codecRoleSpec()
	fromSchema.Redis.Key = "other"

	err := schema.CheckConflicts([]meta.TypeSpec{annotation}, []meta.TypeSpec{fromSchema})
	if !errors.Is(err, schema.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}
