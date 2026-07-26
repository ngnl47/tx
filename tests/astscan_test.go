package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ngnl47/tx/astscan"
	"github.com/ngnl47/tx/meta"
)

func TestASTScanDirExtractsStructFieldsAliasesAndMethods(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/role.go": `package data

type Base struct {
	BaseID int64 ` + "`json:\"baseId\"`" + `
}

// @storage:` + "`mode:\"fusion\" delaySaveTime:\"600s\" delayDelTime:\"24h\"`" + `
// @redis:` + "`routeType:\"role\" routeBy:\"RoleID\" idField:\"RoleID\" key:\"role\" omitempty:\"true\"`" + `
// @mongodb:` + "`routeType:\"role\" routeBy:\"RoleID\" idField:\"RoleID\" collection:\"roles\" enableSharding:\"true\"`" + `
type Role struct {
	Base
	RoleID int64 ` + "`json:\"roleId\" bson:\"_id\" msgpack:\"roleId\"`" + `
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
	assertField(t, spec, "Base", "base", "base", "base", "Base", true)
	assertEmbeddedField(t, spec, "Base", true)
	assertField(t, spec, "RoleID", "roleId", "_id", "roleId", "int64", true)
	assertField(t, spec, "Name", "name", "name", "name", "string", true)
	assertField(t, spec, "OwnerID", "ownerID", "owner_id", "ownerID", "int64", true)
	assertField(t, spec, "privateNote", "privateNote", "privateNote", "privateNote", "string", false)
	assertField(t, spec, "Items", "items", "items", "items", "[]string", true)
	assertNoField(t, spec, "Ignored")

	assertMethod(t, spec, "Reset", true)
	assertMethod(t, spec, "PublicValue", true)
	assertMethod(t, spec, "hidden", false)
	if spec.Storage.Mode != "fusion" || spec.Redis.IDField != "RoleID" || spec.Mongo.Collection != "roles" {
		t.Fatalf("metadata = %#v", spec)
	}
	if spec.Fields["Items"].Redis.SubKey != "items" || !spec.Fields["Items"].Mongo.Indexes[0].Unique {
		t.Fatalf("field metadata = %#v", spec.Fields["Items"])
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
	assertField(t, spec, "UserID", "userID", "userID", "userID", "string", true)
}

func TestASTScanRejectsIgnoredJSONTagWithOptions(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/role.go": `package data

type Role struct {
	Internal string ` + "`json:\"-,omitempty\"`" + `
}
`,
	})

	_, err := astscan.ScanDir(filepath.Join(root, "data"), astscan.Options{})
	if err == nil {
		t.Fatal("ScanDir returned nil error, want ignored json tag options rejection")
	}
	if got := err.Error(); !strings.Contains(got, `json:"-" must not declare options`) {
		t.Fatalf("ScanDir error = %v, want ignored json tag options rejection", err)
	}
}

func assertField(t *testing.T, spec meta.TypeSpec, name, jsonName, bsonName, msgpackName, typ string, exported bool) {
	t.Helper()
	for _, field := range spec.Type.Fields {
		if field.Name == name {
			if field.JSONName != jsonName || field.BSONName != bsonName || field.MsgpackName != msgpackName || field.Type != typ || field.Exported != exported {
				t.Fatalf("field %s = %#v", name, field)
			}
			return
		}
	}
	t.Fatalf("field %s not found in %#v", name, spec.Type.Fields)
}

func assertEmbeddedField(t *testing.T, spec meta.TypeSpec, name string, embedded bool) {
	t.Helper()
	for _, field := range spec.Type.Fields {
		if field.Name == name {
			if field.Embedded != embedded {
				t.Fatalf("field %s embedded = %v, want %v", name, field.Embedded, embedded)
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
