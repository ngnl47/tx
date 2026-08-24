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
		Redis: &meta.RedisSpec{
			IDField: "RoleId",
			Key:     "role",
			Route:   meta.RouteSpec{Type: "role", By: "RoleId"},
		},
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
					Key:     "test",
					Route:   meta.RouteSpec{Type: "test", By: "ID"},
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

func TestValidateRejectsBackendOutsideStorageMode(t *testing.T) {
	tests := []struct {
		name       string
		mode       meta.StorageMode
		unexpected string
	}{
		{name: "redis mode with mongodb", mode: meta.StorageModeRedis, unexpected: "mongodb"},
		{name: "mongo mode with redis", mode: meta.StorageModeMongo, unexpected: "redis"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := fusionIdentitySpec()
			spec.Storage.Mode = tt.mode

			err := meta.Validate(spec, spec)
			if err == nil {
				t.Fatal("Validate returned nil, want backend conflict error")
			}
			for _, want := range []string{"storage.mode", string(tt.mode), tt.unexpected} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %q, want it to contain %q", err, want)
				}
			}
		})
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

func TestValidateRejectsMissingStorageLocatorText(t *testing.T) {
	tests := []struct {
		name     string
		property string
		mutate   func(*meta.TypeSpec)
	}{
		{
			name:     "redis route type",
			property: "redis.routeType",
			mutate: func(spec *meta.TypeSpec) {
				spec.Redis.Route.Type = ""
			},
		},
		{
			name:     "redis key",
			property: "redis.key",
			mutate: func(spec *meta.TypeSpec) {
				spec.Redis.Key = ""
			},
		},
		{
			name:     "mongodb route type",
			property: "mongodb.routeType",
			mutate: func(spec *meta.TypeSpec) {
				spec.Mongo.Route.Type = ""
			},
		},
		{
			name:     "mongodb collection",
			property: "mongodb.collection",
			mutate: func(spec *meta.TypeSpec) {
				spec.Mongo.Collection = ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := validFusionStorageSpec()
			tt.mutate(&spec)

			err := meta.Validate(spec, spec)
			if !errors.Is(err, meta.ErrMissingConfig) {
				t.Fatalf("err = %v, want ErrMissingConfig", err)
			}
			if !strings.Contains(err.Error(), tt.property) {
				t.Fatalf("err = %q, want it to contain %q", err, tt.property)
			}
		})
	}
}

func TestValidateRejectsStorageLocatorTextWithSurroundingWhitespace(t *testing.T) {
	tests := []struct {
		name     string
		property string
		mutate   func(*meta.TypeSpec)
	}{
		{
			name:     "redis route type",
			property: "redis.routeType",
			mutate: func(spec *meta.TypeSpec) {
				spec.Redis.Route.Type = " role"
			},
		},
		{
			name:     "redis key",
			property: "redis.key",
			mutate: func(spec *meta.TypeSpec) {
				spec.Redis.Key = "role "
			},
		},
		{
			name:     "mongodb route type",
			property: "mongodb.routeType",
			mutate: func(spec *meta.TypeSpec) {
				spec.Mongo.Route.Type = " role "
			},
		},
		{
			name:     "mongodb collection",
			property: "mongodb.collection",
			mutate: func(spec *meta.TypeSpec) {
				spec.Mongo.Collection = " roles "
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := validFusionStorageSpec()
			tt.mutate(&spec)

			err := meta.Validate(spec, spec)
			if err == nil {
				t.Fatal("Validate returned nil, want surrounding whitespace error")
			}
			for _, want := range []string{tt.property, "surrounding whitespace"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}

func validFusionStorageSpec() meta.TypeSpec {
	return fusionIdentitySpec()
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
			Key:     "role",
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
