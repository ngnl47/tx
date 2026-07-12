package tests

import (
	"errors"
	"testing"

	"github.com/ngnl47/gobricks/anno"
	"github.com/ngnl47/gobricks/meta"
	"github.com/ngnl47/gobricks/storage"
)

func TestStorageResolveBuildsFrozenRegistry(t *testing.T) {
	reg, err := storage.ResolveMeta([]meta.TypeSpec{storageMetaSpec()}, storageMetaTypes(t))
	if err != nil {
		t.Fatalf("ResolveMeta returned error: %v", err)
	}

	cfg, ok := reg.Lookup("example.com/project/data.Role")
	if !ok {
		t.Fatal("Lookup did not find Role config")
	}
	if cfg.Mode != storage.ModeFusion {
		t.Fatalf("mode = %q", cfg.Mode)
	}
	if cfg.Redis.Route.By != "roleId" || cfg.Mongo.Route.By != "roleId" {
		t.Fatalf("routes = %#v %#v", cfg.Redis.Route, cfg.Mongo.Route)
	}
	if cfg.Fields["items"].Redis.SubKey != "items" {
		t.Fatalf("items field config = %#v", cfg.Fields["items"])
	}

	err = reg.Register("example.com/project/data.Other", storage.TypeConfig{})
	if !errors.Is(err, storage.ErrRegistryFrozen) {
		t.Fatalf("expected ErrRegistryFrozen, got %v", err)
	}
}

func TestStorageResolveRejectsInvalidSchema(t *testing.T) {
	spec := storageMetaSpec()
	spec.Redis.IDField = "missing"

	_, err := storage.ResolveMeta([]meta.TypeSpec{spec}, storageMetaTypes(t))
	if !errors.Is(err, anno.ErrUnknownField) {
		t.Fatalf("expected anno.ErrUnknownField, got %v", err)
	}
}

func storageMetaSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{FullName: "example.com/project/data.Role"}},
		Storage: meta.StorageSpec{Mode: meta.StorageModeFusion},
		Redis: &meta.RedisSpec{
			Route:   meta.RouteSpec{Type: "role", By: "roleId"},
			IDField: "roleId",
			Key:     "role",
		},
		Mongo: &meta.MongoSpec{
			Route:      meta.RouteSpec{Type: "role", By: "roleId"},
			IDField:    "roleId",
			Collection: "roles",
			Sharding:   meta.ShardingSpec{Enabled: true},
		},
		Fields: map[string]meta.FieldSpec{
			"items": {
				Redis: &meta.RedisFieldSpec{SubKey: "items"},
			},
			"roleId": {
				Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Unique: true}}},
			},
		},
	}
}

func storageMetaTypes(t *testing.T) *meta.Registry {
	t.Helper()
	reg := meta.NewRegistry()
	err := reg.RegisterType(meta.TypeSpec{
		Type: meta.Type{
			Ref: meta.TypeRef{ImportPath: "example.com/project/data", Package: "data", Name: "Role"},
			Fields: []meta.Field{
				{Name: "RoleID", Alias: "roleId", Type: "int64", Exported: true},
				{Name: "Items", Alias: "items", Type: "[]Item", Exported: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	return reg
}
