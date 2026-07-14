package tests

import (
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ngnl47/tx/anno"
	"github.com/ngnl47/tx/astscan"
)

type genDataTag struct{}
type genEnumTag struct{}
type customExportTag struct{}
type ignoreClientTag struct{}
type commentTag struct{}
type enumTag struct{}

func TestASTScanUsesCallerProvidedAnnotationDefinitions(t *testing.T) {
	set, err := astscan.ScanDir(filepath.Join(repoRoot(t), "data", "data"), astscan.Options{
		Annotations: dataAnnotationDefinitions(),
	})
	if err != nil {
		t.Fatalf("ScanDir returned error: %v", err)
	}

	typeDirectives := set.TypeAnnotations("github.com/ngnl47/tx/data/data.GDRole")
	if !hasDirective(typeDirectives, "customExport", "true") {
		t.Fatalf("GDRole type directives = %#v", typeDirectives)
	}
	if !hasDirective(typeDirectives, "comment", "角色") {
		t.Fatalf("GDRole type directives = %#v", typeDirectives)
	}

	fieldDirectives := set.FieldAnnotations("github.com/ngnl47/tx/data/data.GDRole", "items")
	if !hasFieldDirective(fieldDirectives, "comment", "拥有物品") {
		t.Fatalf("GDRole.Items field directives = %#v", fieldDirectives)
	}
	if !hasDirectiveParam(fieldDirectives, "redis", "subKey", "items") {
		t.Fatalf("GDRole.Items field directives = %#v", fieldDirectives)
	}
}

func TestASTScanRejectsUndefinedAnnotation(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/item.go": `package data

// @custom:` + "`owner:\"roleId\"`" + `
type Item struct {
	RoleID int64 ` + "`alias:\"roleId\"`" + `
}
`,
	})

	_, err := astscan.ScanDir(filepath.Join(root, "data"), astscan.Options{
		Annotations: anno.Definitions{
			anno.TypeTag[genDataTag]("other"),
		},
	})
	if !errors.Is(err, anno.ErrUnknownTag) {
		t.Fatalf("expected ErrUnknownTag, got %v", err)
	}
}

func TestAnnoParseTypeTagLineSupportsStructTagSyntax(t *testing.T) {
	directive, err := anno.ParseTypeTagLineWithDefinitions(
		`// @custom:`+"`owner:\"roleId\" ttl:\"600\"`",
		anno.Definitions{anno.TypeTag[struct {
			Owner string
			TTL   int
		}]("custom")},
	)
	if err != nil {
		t.Fatalf("ParseTypeTagLineWithDefinitions returned error: %v", err)
	}
	if directive.Params["owner"] != "roleId" || directive.Params["ttl"] != "600" {
		t.Fatalf("directive = %#v", directive)
	}
}

func dataAnnotationDefinitions() anno.Definitions {
	defs := anno.BuiltinDefinitions()
	defs = append(defs,
		anno.TypeTag[genDataTag]("genData"),
		anno.TypeTag[genEnumTag]("genEnum"),
		anno.TypeTag[customExportTag]("customExport"),
		anno.FieldTag[customExportTag]("customExport"),
		anno.TypeTag[ignoreClientTag]("ignoreClient"),
		anno.FieldTag[ignoreClientTag]("ignoreClient"),
		anno.TypeTag[commentTag]("comment"),
		anno.FieldTag[commentTag]("comment"),
		anno.FieldTag[enumTag]("enum"),
	)
	return defs
}

func hasDirective(directives []anno.Directive, name, value string) bool {
	for _, directive := range directives {
		if directive.Name == name && directive.Params["value"] == value {
			return true
		}
	}
	return false
}

func hasDirectiveParam(directives []anno.Directive, name, key, value string) bool {
	for _, directive := range directives {
		if directive.Name == name && directive.Params[key] == value {
			return true
		}
	}
	return false
}

func hasFieldDirective(directives []anno.Directive, name, value string) bool {
	for _, directive := range directives {
		if directive.Name == name && (directive.Params["value"] == value || directive.Flags[value]) {
			return true
		}
	}
	return false
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(filepath.Dir(file))
}
