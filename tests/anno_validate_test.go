package tests

import (
	"errors"
	"testing"

	"github.com/ngnl47/gobricks/anno"
	"github.com/ngnl47/gobricks/meta"
)

func TestAnnoNormalizeAndValidate(t *testing.T) {
	directives, err := anno.ParseTypeDoc([]string{
		`// @storage:` + "`mode:\"fusion\" delaySaveTime:\"600s\" delayDelTime:\"24h\"`",
		`// @redis:` + "`routeType:\"role\" routeBy:\"roleId\" idField:\"roleId\" key:\"role\" omitempty:\"true\"`",
		`// @mongodb:` + "`routeType:\"role\" routeBy:\"roleId\" idField:\"roleId\" collection:\"roles\" enableSharding:\"true\"`",
	})
	if err != nil {
		t.Fatalf("ParseTypeDoc returned error: %v", err)
	}
	spec, err := meta.Normalize(directives)
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	astType := meta.TypeSpec{Type: meta.Type{
		Ref: meta.TypeRef{FullName: "example.com/project/data.Role"},
		Fields: []meta.Field{
			{Name: "RoleID", Alias: "roleId"},
			{Name: "Items", Alias: "items"},
		},
	}}
	spec.Type = astType.Type
	if err := meta.Validate(spec, astType); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if spec.Storage.Mode != meta.StorageModeFusion || !spec.Redis.OmitEmpty || !spec.Mongo.Sharding.Enabled {
		t.Fatalf("spec = %#v", spec)
	}
}

func TestAnnoValidateRejectsUnknownField(t *testing.T) {
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{FullName: "example.com/project/data.Role"}},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis},
		Redis:   &meta.RedisSpec{IDField: "missing", Route: meta.RouteSpec{Type: "role", By: "roleId"}, Key: "role"},
	}
	astType := meta.TypeSpec{Type: meta.Type{
		Ref:    meta.TypeRef{FullName: "example.com/project/data.Role"},
		Fields: []meta.Field{{Name: "RoleID", Alias: "roleId"}},
	}}

	err := meta.Validate(spec, astType)
	if !errors.Is(err, meta.ErrUnknownField) {
		t.Fatalf("expected ErrUnknownField, got %v", err)
	}
}
