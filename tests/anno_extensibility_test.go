package tests

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/ngnl47/tx/anno"
	"github.com/ngnl47/tx/astscan"
)

type genDataTag struct{}

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
