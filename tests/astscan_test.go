package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ngnl47/gobricks/astscan"
	"github.com/ngnl47/gobricks/meta"
)

func TestASTScanDirExtractsStructFieldsAliasesAndMethods(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/role.go": `package data

type Base struct {
	BaseID int64 ` + "`json:\"baseId\"`" + `
}

// @storage:` + "`mode:\"fusion\" delaySaveTime:\"600s\" delayDelTime:\"24h\"`" + `
// @redis:` + "`routeType:\"role\" routeBy:\"roleId\" idField:\"roleId\" key:\"role\" omitempty:\"true\"`" + `
// @mongodb:` + "`routeType:\"role\" routeBy:\"roleId\" idField:\"roleId\" collection:\"roles\" enableSharding:\"true\"`" + `
type Role struct {
	Base
	RoleID int64 ` + "`alias:\"roleId\" json:\"role_id\" bson:\"role_id\"`" + `
	Name string ` + "`json:\"name,omitempty\"`" + `
	OwnerID int64 ` + "`bson:\"owner_id\"`" + `
	privateNote string
	Items []string ` + "`json:\"items\" redis:\"subKey:items\" mongodb:\"index,unique\"`" + `
	Ignored string ` + "`json:\"-\"`" + `
}

func (r *Role) Reset() {}
func (Role) PublicValue() {}
func (r *Role) hidden() {}
`,
	})

	set, err := astscan.ScanDir(filepath.Join(root, "data"), astscan.Options{})
	if err != nil {
		t.Fatalf("ScanDir returned error: %v", err)
	}

	spec, ok := set.Lookup(meta.TypeRef{FullName: "example.com/project/data.Role"})
	if !ok {
		t.Fatal("Role type was not scanned")
	}
	if spec.Type.Ref.ImportPath != "example.com/project/data" {
		t.Fatalf("import path = %q", spec.Type.Ref.ImportPath)
	}
	assertField(t, spec, "Base", "base", "Base", true)
	assertField(t, spec, "RoleID", "roleId", "int64", true)
	assertField(t, spec, "Name", "name", "string", true)
	assertField(t, spec, "OwnerID", "ownerID", "int64", true)
	assertField(t, spec, "privateNote", "privateNote", "string", false)
	assertField(t, spec, "Items", "items", "[]string", true)
	assertNoField(t, spec, "Ignored")

	assertMethod(t, spec, "Reset", true)
	assertMethod(t, spec, "PublicValue", true)
	assertMethod(t, spec, "hidden", false)
	if spec.Storage.Mode != "fusion" || spec.Redis.IDField != "roleId" || spec.Mongo.Collection != "roles" {
		t.Fatalf("metadata = %#v", spec)
	}
	if spec.Fields["items"].Redis.SubKey != "items" || !spec.Fields["items"].Mongo.Indexes[0].Unique {
		t.Fatalf("field metadata = %#v", spec.Fields["items"])
	}
}

func TestASTScanFilesUsesExplicitImportPath(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"models/user.go": `package models

type UserProfile struct {
	UserID string
}
`,
	})

	set, err := astscan.ScanFiles([]string{filepath.Join(root, "models/user.go")}, astscan.Options{
		ImportPath: "example.com/manual/models",
	})
	if err != nil {
		t.Fatalf("ScanFiles returned error: %v", err)
	}

	spec, ok := set.Lookup(meta.TypeRef{FullName: "example.com/manual/models.UserProfile"})
	if !ok {
		t.Fatal("UserProfile type was not scanned")
	}
	assertField(t, spec, "UserID", "userID", "string", true)
}

func assertField(t *testing.T, spec meta.TypeSpec, name, alias, typ string, exported bool) {
	t.Helper()
	for _, field := range spec.Type.Fields {
		if field.Name == name {
			if field.Alias != alias || field.Type != typ || field.Exported != exported {
				t.Fatalf("field %s = %#v", name, field)
			}
			return
		}
	}
	t.Fatalf("field %s not found in %#v", name, spec.Type.Fields)
}

func assertNoField(t *testing.T, spec meta.TypeSpec, name string) {
	t.Helper()
	for _, field := range spec.Type.Fields {
		if field.Name == name {
			t.Fatalf("field %s should not be scanned", name)
		}
	}
}

func assertMethod(t *testing.T, spec meta.TypeSpec, name string, exported bool) {
	t.Helper()
	for _, method := range spec.Type.Methods {
		if method.Name == name {
			if method.Exported != exported {
				t.Fatalf("method %s exported = %v", name, method.Exported)
			}
			return
		}
	}
	t.Fatalf("method %s not found in %#v", name, spec.Type.Methods)
}

func writeTempModule(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/project\n\ngo 1.25.6\n"), 0o600)
	if err != nil {
		t.Fatalf("WriteFile go.mod returned error: %v", err)
	}
	for name, content := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("MkdirAll returned error: %v", err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("WriteFile %s returned error: %v", name, err)
		}
	}
	return root
}
