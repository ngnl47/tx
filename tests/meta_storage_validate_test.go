package tests

import (
	"errors"
	"strings"
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestValidateRejectsMissingRedisIDField(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", JSONName: "roleId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis:   &meta.RedisSpec{IDField: "missing", Route: meta.RouteSpec{By: "RoleId"}},
	}
	err := meta.Validate(spec, spec)
	if !errors.Is(err, meta.ErrUnknownField) {
		t.Fatalf("err = %v, want ErrUnknownField", err)
	}
}

func TestValidateAcceptsRedisIDFieldGoName(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", JSONName: "roleId", Type: "int64"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis:   &meta.RedisSpec{IDField: "RoleId", Route: meta.RouteSpec{By: "RoleId"}},
	}
	if err := meta.Validate(spec, spec); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestValidateRejectsUnsupportedRouteFieldType(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Ref: meta.TypeRef{Name: "FeatureState"},
			Fields: []meta.Field{
				{Name: "ID", Type: "int64"},
				{Name: "Enabled", Type: "bool"},
			},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis: &meta.RedisSpec{
			IDField: "ID",
			Route:   meta.RouteSpec{By: "Enabled"},
		},
	}

	err := meta.Validate(spec, spec)
	if err == nil {
		t.Fatal("Validate returned nil, want unsupported route field type error")
	}
	for _, want := range []string{"redis.routeBy", "Enabled", "bool", "integer or string"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to contain %q", err, want)
		}
	}
}

func TestValidateAcceptsIntegerAndStringRouteFieldTypes(t *testing.T) {
	for _, fieldType := range []string{"int64", "uint32", "string"} {
		t.Run(fieldType, func(t *testing.T) {
			spec := meta.TypeSpec{
				Type:    meta.Type{Fields: []meta.Field{{Name: "ID", Type: fieldType}}},
				Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
				Redis: &meta.RedisSpec{
					IDField: "ID",
					Route:   meta.RouteSpec{By: "ID"},
				},
			}

			if err := meta.Validate(spec, spec); err != nil {
				t.Fatalf("Validate returned error: %v", err)
			}
		})
	}
}

func TestValidateRejectsRedisIDFieldBackendName(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", JSONName: "roleId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis:   &meta.RedisSpec{IDField: "roleId", Route: meta.RouteSpec{By: "RoleId"}},
	}
	err := meta.Validate(spec, spec)
	if !errors.Is(err, meta.ErrUnknownField) {
		t.Fatalf("err = %v, want ErrUnknownField", err)
	}
}

func TestValidateRejectsFusionWithoutBothStores(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", JSONName: "roleId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeFusion},
		Redis:   &meta.RedisSpec{IDField: "RoleId", Route: meta.RouteSpec{By: "RoleId"}},
	}
	err := meta.Validate(spec, spec)
	if !errors.Is(err, meta.ErrMissingConfig) {
		t.Fatalf("err = %v, want ErrMissingConfig", err)
	}
}

func TestValidateRejectsFusionWithDifferentIDFields(t *testing.T) {
	spec := fusionIdentitySpec()
	spec.Mongo.IDField = "AccountId"

	err := meta.Validate(spec, spec)
	if err == nil {
		t.Fatal("Validate returned nil, want mismatched Fusion idField error")
	}
	for _, want := range []string{"fusion", "idField", "RoleId", "AccountId"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to contain %q", err, want)
		}
	}
}

func TestValidateRejectsFusionWithDifferentRouteFields(t *testing.T) {
	spec := fusionIdentitySpec()
	spec.Mongo.Route.By = "ServerId"

	err := meta.Validate(spec, spec)
	if err == nil {
		t.Fatal("Validate returned nil, want mismatched Fusion routeBy error")
	}
	for _, want := range []string{"fusion", "routeBy", "RoleId", "ServerId"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to contain %q", err, want)
		}
	}
}

func TestValidateAcceptsFusionWithDifferentRouteTypes(t *testing.T) {
	spec := fusionIdentitySpec()
	spec.Redis.Route.Type = "redis-role"
	spec.Mongo.Route.Type = "mongo-role"

	if err := meta.Validate(spec, spec); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func fusionIdentitySpec() meta.TypeSpec {
	fields := []meta.Field{
		{Name: "RoleId", Type: "int64"},
		{Name: "AccountId", Type: "int64"},
		{Name: "ServerId", Type: "int32"},
	}
	return meta.TypeSpec{
		Type:    meta.Type{Fields: fields},
		Storage: meta.StorageSpec{Mode: meta.StorageModeFusion},
		Redis: &meta.RedisSpec{
			IDField: "RoleId",
			Route: meta.RouteSpec{
				Type: "role",
				By:   "RoleId",
			},
		},
		Mongo: &meta.MongoSpec{
			Collection: "roles",
			IDField:    "RoleId",
			Route: meta.RouteSpec{
				Type: "role",
				By:   "RoleId",
			},
		},
	}
}
