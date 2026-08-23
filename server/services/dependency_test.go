package services_test

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestStandardServicesDoNotImportSiblingImplementations(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	root := filepath.Dir(filename)
	files, err := filepath.Glob(filepath.Join(root, "*", "*.go"))
	if err != nil {
		t.Fatalf("glob service sources: %v", err)
	}
	for _, filename := range files {
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		fileset := token.NewFileSet()
		parsed, err := parser.ParseFile(fileset, filename, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import %s: %v", spec.Path.Value, err)
			}
			if strings.HasPrefix(path, "github.com/ngnl47/tx/server/services/") {
				t.Errorf("%s imports sibling service implementation %q", fileset.Position(spec.Pos()), path)
			}
		}
	}
}
