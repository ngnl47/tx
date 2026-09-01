package tests

import (
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestTypeSpecSerializationFieldNames(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:        "DocumentId",
				JSONName:    "documentId",
				BSONName:    "_id",
				MsgpackName: "documentId",
				Type:        "string",
				Exported:    true,
			}},
		},
		Fields: map[string]meta.FieldSpec{"DocumentId": {}},
	}

	field, ok := spec.FieldByName("DocumentId")
	if !ok {
		t.Fatal("FieldByName(DocumentId) returned false")
	}
	field, ok = spec.FieldByJSONName("documentId")
	if !ok || field.Name != "DocumentId" {
		t.Fatalf("FieldByJSONName(documentId) = %#v, %v", field, ok)
	}
	if field.BSONName != "_id" {
		t.Fatalf("BSONName = %q, want _id", field.BSONName)
	}
	if got := spec.JSONName("DocumentId"); got != "documentId" {
		t.Fatalf("JSONName = %q, want documentId", got)
	}
}
