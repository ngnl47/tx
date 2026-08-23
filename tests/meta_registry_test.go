package tests

import (
	"errors"
	"strings"
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestMetaRegistryRegistersAndLooksUpType(t *testing.T) {
	reg := meta.NewRegistry()
	ref := meta.TypeRef{
		ImportPath: "example.com/project/data",
		Package:    "data",
		Name:       "Role",
	}
	spec := meta.TypeSpec{
		Type: meta.Type{
			Ref: ref,
			Fields: []meta.Field{
				{Name: "RoleID", Alias: "roleId", Type: "int64", Exported: true},
				{Name: "items", Alias: "items", Type: "[]Item", Exported: false},
			},
			Methods: []meta.Method{
				{Name: "Reset", Exported: true},
			},
		},
	}

	if err := reg.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}

	got, ok := reg.Lookup(ref)
	if !ok {
		t.Fatal("Lookup did not find registered type")
	}
	if got.Type.Ref.FullName != "example.com/project/data.Role" {
		t.Fatalf("FullName = %q", got.Type.Ref.FullName)
	}
	if got.Type.Fields[0].Alias != "roleId" {
		t.Fatalf("field alias = %q", got.Type.Fields[0].Alias)
	}
}

func TestMetaRegistryRejectsDuplicateRegistration(t *testing.T) {
	reg := meta.NewRegistry()
	ref := meta.TypeRef{ImportPath: "example.com/project/data", Package: "data", Name: "Role"}
	spec := meta.TypeSpec{Type: meta.Type{Ref: ref}}

	if err := reg.RegisterType(spec); err != nil {
		t.Fatalf("first RegisterType returned error: %v", err)
	}
	err := reg.RegisterType(spec)
	if !errors.Is(err, meta.ErrDuplicateType) {
		t.Fatalf("expected ErrDuplicateType, got %v", err)
	}
	if !strings.Contains(err.Error(), "example.com/project/data.Role") {
		t.Fatalf("duplicate error should include type name, got %q", err.Error())
	}
}

func TestMetaRegistryRejectsRegistrationAfterFreeze(t *testing.T) {
	reg := meta.NewRegistry()
	reg.Freeze()

	ref := meta.TypeRef{ImportPath: "example.com/project/data", Package: "data", Name: "Role"}
	err := reg.RegisterType(meta.TypeSpec{Type: meta.Type{Ref: ref}})
	if !errors.Is(err, meta.ErrRegistryFrozen) {
		t.Fatalf("expected ErrRegistryFrozen, got %v", err)
	}
	if !strings.Contains(err.Error(), "example.com/project/data.Role") {
		t.Fatalf("frozen error should include type name, got %q", err.Error())
	}
}

func TestMetaRegistryLookupMiss(t *testing.T) {
	reg := meta.NewRegistry()
	ref := meta.TypeRef{ImportPath: "example.com/project/data", Package: "data", Name: "Missing"}

	if _, ok := reg.Lookup(ref); ok {
		t.Fatal("Lookup found unregistered type")
	}
}

func TestMetaRegistryRejectsDuplicateFieldAlias(t *testing.T) {
	reg := meta.NewRegistry()
	ref := meta.TypeRef{ImportPath: "example.com/project/data", Package: "data", Name: "Role"}

	err := reg.RegisterType(meta.TypeSpec{
		Type: meta.Type{
			Ref: ref,
			Fields: []meta.Field{
				{Name: "RoleID", Alias: "id", Type: "int64", Exported: true},
				{Name: "OwnerID", Alias: "id", Type: "int64", Exported: true},
			},
		},
	})
	if !errors.Is(err, meta.ErrDuplicateFieldAlias) {
		t.Fatalf("expected ErrDuplicateFieldAlias, got %v", err)
	}
	if !strings.Contains(err.Error(), "example.com/project/data.Role") {
		t.Fatalf("field error should include type name, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "OwnerID") {
		t.Fatalf("field error should include field name, got %q", err.Error())
	}
}
