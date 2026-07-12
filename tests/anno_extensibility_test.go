package tests

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/ngnl47/gobricks/anno"
	"github.com/ngnl47/gobricks/astscan"
)

type customTypeTag struct {
	Owner string
	TTL   int
}

type customFieldTag struct {
	Name string
}

func TestASTScanUsesCallerProvidedAnnotationDefinitions(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/item.go": `package data

// @custom:` + "`owner:\"roleId\" ttl:\"600\"`" + `
type Item struct {
	RoleID int64 ` + "`alias:\"roleId\" customField:\"name:owner\"`" + `
}
`,
	})

	set, err := astscan.ScanDir(filepath.Join(root, "data"), astscan.Options{
		Annotations: anno.Definitions{
			anno.TypeTag[customTypeTag]("custom"),
			anno.FieldTag[customFieldTag]("customField"),
		},
	})
	if err != nil {
		t.Fatalf("ScanDir returned error: %v", err)
	}

	typeDirectives := set.TypeAnnotations("example.com/project/data.Item")
	if len(typeDirectives) != 1 {
		t.Fatalf("type directives = %#v", typeDirectives)
	}
	if typeDirectives[0].Name != "custom" || typeDirectives[0].Params["ttl"] != "600" {
		t.Fatalf("type directive = %#v", typeDirectives[0])
	}
	fieldDirectives := set.FieldAnnotations("example.com/project/data.Item", "roleId")
	if len(fieldDirectives) != 1 || fieldDirectives[0].Name != "customField" {
		t.Fatalf("field directives = %#v", fieldDirectives)
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
			anno.TypeTag[customTypeTag]("other"),
		},
	})
	if !errors.Is(err, anno.ErrUnknownTag) {
		t.Fatalf("expected ErrUnknownTag, got %v", err)
	}
}

func TestAnnoParseTypeTagLineSupportsStructTagSyntax(t *testing.T) {
	directive, err := anno.ParseTypeTagLineWithDefinitions(
		`// @custom:`+"`owner:\"roleId\" ttl:\"600\"`",
		anno.Definitions{anno.TypeTag[customTypeTag]("custom")},
	)
	if err != nil {
		t.Fatalf("ParseTypeTagLineWithDefinitions returned error: %v", err)
	}
	if directive.Params["owner"] != "roleId" || directive.Params["ttl"] != "600" {
		t.Fatalf("directive = %#v", directive)
	}
}
