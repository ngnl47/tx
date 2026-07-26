package tests

import (
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
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
	if cfg.Redis.Route.By != "RoleID" || cfg.Mongo.Route.By != "RoleID" {
		t.Fatalf("routes = %#v %#v", cfg.Redis.Route, cfg.Mongo.Route)
	}
	if cfg.DelaySave != "10m0s" || cfg.DelayDelete != "24h0m0s" {
		t.Fatalf("delay config = save %q delete %q", cfg.DelaySave, cfg.DelayDelete)
	}
	if cfg.Fields["Items"].Redis.SubKey != "items" {
		t.Fatalf("items field config = %#v", cfg.Fields["Items"])
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
	if !errors.Is(err, meta.ErrUnknownField) {
		t.Fatalf("expected meta.ErrUnknownField, got %v", err)
	}
}

func storageMetaSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{FullName: "example.com/project/data.Role"}},
		Storage: meta.StorageSpec{
			Mode:        meta.StorageModeFusion,
			DelaySave:   meta.Duration{Duration: 10 * time.Minute},
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
		Redis: &meta.RedisSpec{
			Route:   meta.RouteSpec{Type: "role", By: "RoleID"},
			IDField: "RoleID",
			Key:     "role",
		},
		Mongo: &meta.MongoSpec{
			Route:      meta.RouteSpec{Type: "role", By: "RoleID"},
			IDField:    "RoleID",
			Collection: "roles",
			Sharding:   meta.ShardingSpec{Enabled: true},
		},
		Fields: map[string]meta.FieldSpec{
			"Items": {
				Redis: &meta.RedisFieldSpec{SubKey: "items"},
			},
			"RoleID": {
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
				{Name: "RoleID", JSONName: "roleId", BSONName: "roleId", MsgpackName: "roleId", RedisName: "roleId", Type: "int64", Exported: true},
				{Name: "Items", JSONName: "items", BSONName: "items", MsgpackName: "items", RedisName: "items", Type: "[]Item", Exported: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	return reg
}
