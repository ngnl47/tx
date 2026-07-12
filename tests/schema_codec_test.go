package tests

import (
	"testing"

	"github.com/ngnl47/gobricks/meta"
	"github.com/ngnl47/gobricks/schema"
)

func TestSchemaCodecExportsAndImportsMetaSpecs(t *testing.T) {
	specs := []meta.TypeSpec{codecRoleSpec()}

	data, err := schema.Export(specs)
	if err != nil {
		t.Fatalf("Export returned error: %v", err)
	}
	imported, err := schema.Import(data)
	if err != nil {
		t.Fatalf("Import returned error: %v", err)
	}
	if len(imported) != 1 {
		t.Fatalf("imported = %#v", imported)
	}
	got := imported[0]
	if got.Type.Ref.FullName != "example.com/project/data.Role" {
		t.Fatalf("type ref = %#v", got.Type.Ref)
	}
	if got.Storage.Mode != meta.StorageModeFusion || got.Redis.Route.By != "roleId" || got.Mongo.Collection != "roles" {
		t.Fatalf("imported spec = %#v", got)
	}
}

func codecRoleSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{FullName: "example.com/project/data.Role"}},
		Storage: meta.StorageSpec{
			Mode: meta.StorageModeFusion,
		},
		Redis: &meta.RedisSpec{
			Route:   meta.RouteSpec{Type: "role", By: "roleId"},
			IDField: "roleId",
			Key:     "role",
		},
		Mongo: &meta.MongoSpec{
			Route:      meta.RouteSpec{Type: "role", By: "roleId"},
			IDField:    "roleId",
			Collection: "roles",
		},
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
}
