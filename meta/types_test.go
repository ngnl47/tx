package meta_test

import (
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestCloneTypeSpecIsolatesMutableMetadata(t *testing.T) {
	source := meta.TypeSpec{
		Type: meta.Type{
			Ref:     meta.TypeRef{Package: "data", Name: "GDRole", FullName: "data.GDRole"},
			Fields:  []meta.Field{{Name: "RoleId"}},
			Methods: []meta.Method{{Name: "Store"}},
		},
		Redis:  &meta.RedisSpec{Key: "role"},
		Mongo:  &meta.MongoSpec{Collection: "roles"},
		Fusion: &meta.FusionSpec{},
		Fields: map[string]meta.FieldSpec{
			"Items": {
				Redis: &meta.RedisFieldSpec{SubKey: "items"},
				Mongo: &meta.MongoFieldSpec{
					Indexes: []meta.IndexSpec{{
						Keys:    []meta.IndexKeySpec{{Order: "asc"}},
						Weights: []meta.IndexWeightSpec{{Weight: 1}},
					}},
				},
			},
		},
	}

	cloned := meta.CloneTypeSpec(source)
	cloned.Type.Fields[0].Name = "ChangedField"
	cloned.Type.Methods[0].Name = "ChangedMethod"
	cloned.Redis.Key = "changed-role"
	cloned.Mongo.Collection = "changed-roles"
	field := cloned.Fields["Items"]
	field.Redis.SubKey = "changed-items"
	field.Mongo.Indexes[0].Keys[0].Order = "desc"
	field.Mongo.Indexes[0].Weights[0].Weight = 2
	cloned.Fields["Items"] = field
	delete(cloned.Fields, "Items")

	if got := source.Type.Fields[0].Name; got != "RoleId" {
		t.Fatalf("source field name = %q, want RoleId", got)
	}
	if got := source.Type.Methods[0].Name; got != "Store" {
		t.Fatalf("source method name = %q, want Store", got)
	}
	if got := source.Redis.Key; got != "role" {
		t.Fatalf("source Redis key = %q, want role", got)
	}
	if got := source.Mongo.Collection; got != "roles" {
		t.Fatalf("source Mongo collection = %q, want roles", got)
	}
	if got := source.Fields["Items"].Redis.SubKey; got != "items" {
		t.Fatalf("source Redis subKey = %q, want items", got)
	}
	if got := source.Fields["Items"].Mongo.Indexes[0].Keys[0].Order; got != "asc" {
		t.Fatalf("source Mongo index order = %q, want asc", got)
	}
	if got := source.Fields["Items"].Mongo.Indexes[0].Weights[0].Weight; got != 1 {
		t.Fatalf("source Mongo index weight = %d, want 1", got)
	}
	if _, exists := source.Fields["Items"]; !exists {
		t.Fatal("source field metadata was deleted through clone")
	}
}

func TestCloneTypeSpecPreservesTypeRef(t *testing.T) {
	source := meta.TypeSpec{
		Type: meta.Type{
			Ref: meta.TypeRef{Package: " data ", Name: " GDRole "},
		},
	}

	cloned := meta.CloneTypeSpec(source)
	if cloned.Type.Ref != source.Type.Ref {
		t.Fatalf("cloned TypeRef = %#v, want %#v", cloned.Type.Ref, source.Type.Ref)
	}
}

func TestRegistryNormalizesRegisteredTypeRef(t *testing.T) {
	registry := meta.NewRegistry()
	if err := registry.RegisterType(meta.TypeSpec{
		Type: meta.Type{
			Ref: meta.TypeRef{Package: " data ", Name: " GDRole "},
		},
	}); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}

	got, ok := registry.Lookup(meta.TypeRef{Package: "data", Name: "GDRole"})
	if !ok {
		t.Fatal("Lookup normalized type returned false")
	}
	if got.Type.Ref.FullName != "data.GDRole" {
		t.Fatalf("registered TypeRef.FullName = %q, want data.GDRole", got.Type.Ref.FullName)
	}
}

func TestRegistryTypeSpecsReturnsStableClones(t *testing.T) {
	registry := meta.NewRegistry()
	for _, name := range []string{"GDSecond", "GDFirst"} {
		if err := registry.RegisterType(meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: name}}}); err != nil {
			t.Fatalf("RegisterType(%s) returned error: %v", name, err)
		}
	}

	first := registry.TypeSpecs()
	if len(first) != 2 || first[0].Type.Ref.FullName != "data.GDFirst" || first[1].Type.Ref.FullName != "data.GDSecond" {
		t.Fatalf("TypeSpecs = %#v, want stable full-name order", first)
	}
	first[0].Type.Ref.FullName = "mutated"
	second := registry.TypeSpecs()
	if second[0].Type.Ref.FullName != "data.GDFirst" {
		t.Fatalf("TypeSpecs shared mutable metadata: got %q", second[0].Type.Ref.FullName)
	}
}
