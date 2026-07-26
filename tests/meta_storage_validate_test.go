package tests

import (
	"errors"
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
			Fields: []meta.Field{{Name: "RoleId", JSONName: "roleId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis:   &meta.RedisSpec{IDField: "RoleId", Route: meta.RouteSpec{By: "RoleId"}},
	}
	if err := meta.Validate(spec, spec); err != nil {
		t.Fatalf("Validate returned error: %v", err)
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
