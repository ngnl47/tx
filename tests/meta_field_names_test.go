package tests

import (
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestTypeSpecBackendFieldNames(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{
				Name:        "RoleId",
				JSONName:    "roleId",
				BSONName:    "_id",
				MsgpackName: "roleId",
				RedisName:   "roleId",
				Type:        "int64",
				Exported:    true,
			}},
		},
		Fields: map[string]meta.FieldSpec{"RoleId": {}},
	}

	field, ok := spec.FieldByName("RoleId")
	if !ok {
		t.Fatal("FieldByName(RoleId) returned false")
	}
	if field.BSONName != "_id" {
		t.Fatalf("BSONName = %q, want _id", field.BSONName)
	}
	if got := spec.RedisName("RoleId"); got != "roleId" {
		t.Fatalf("RedisName = %q, want roleId", got)
	}
}
