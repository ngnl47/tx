package tests

import (
	"errors"
	"testing"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

func TestValidateLoadFieldsRejectsUnknownGoField(t *testing.T) {
	spec := meta.TypeSpec{Type: meta.Type{
		Ref: meta.TypeRef{Name: "GDRole"},
		Fields: []meta.Field{
			{Name: "RoleId"},
			{Name: "Power"},
		},
	}}

	err := storage.ValidateLoadFields(spec, []string{"Pwoer"})
	if !errors.Is(err, storage.ErrUnknownField) {
		t.Fatalf("ValidateLoadFields error = %v, want ErrUnknownField", err)
	}
}

func TestValidateLoadFieldsAcceptsKnownGoFieldsAndFullLoad(t *testing.T) {
	spec := meta.TypeSpec{Type: meta.Type{Fields: []meta.Field{{Name: "RoleId"}, {Name: "Power"}}}}
	if err := storage.ValidateLoadFields(spec, nil); err != nil {
		t.Fatalf("ValidateLoadFields(nil) error = %v", err)
	}
	if err := storage.ValidateLoadFields(spec, []string{"Power", "RoleId", "Power"}); err != nil {
		t.Fatalf("ValidateLoadFields(known) error = %v", err)
	}
}

func TestValidateDocumentResultRejectsInconsistentExistsState(t *testing.T) {
	if err := storage.ValidateDocumentResult(nil, true); err == nil {
		t.Fatal("ValidateDocumentResult(nil, true) error = nil, want existing document without data error")
	}
	if err := storage.ValidateDocumentResult(map[string]any{"name": "alice"}, false); err == nil {
		t.Fatal("ValidateDocumentResult(document, false) error = nil, want missing document with data error")
	}
}

func TestValidateDocumentResultsRejectsInconsistentBatchRow(t *testing.T) {
	err := storage.ValidateDocumentResults(
		[]map[string]any{{"name": "alice"}, nil},
		[]bool{true, true},
	)
	if err == nil {
		t.Fatal("ValidateDocumentResults error = nil, want existing document without data error")
	}
}
