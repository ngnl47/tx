package tests

import (
	"errors"
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestValidateRejectsMissingRedisIDField(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", Alias: "roleId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis:   &meta.RedisSpec{IDField: "missing", Route: meta.RouteSpec{By: "roleId"}},
	}
	err := meta.Validate(spec, spec)
	if !errors.Is(err, meta.ErrUnknownField) {
		t.Fatalf("err = %v, want ErrUnknownField", err)
	}
}

func TestValidateAcceptsRedisIDFieldAlias(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", Alias: "roleId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis:   &meta.RedisSpec{IDField: "roleId", Route: meta.RouteSpec{By: "roleId"}},
	}
	if err := meta.Validate(spec, spec); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestValidateRejectsFusionWithoutBothStores(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", Alias: "roleId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeFusion},
		Redis:   &meta.RedisSpec{IDField: "roleId", Route: meta.RouteSpec{By: "roleId"}},
	}
	err := meta.Validate(spec, spec)
	if !errors.Is(err, meta.ErrMissingConfig) {
		t.Fatalf("err = %v, want ErrMissingConfig", err)
	}
}
