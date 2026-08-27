package bee_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPublicAPIDoesNotExposeStatefulConcepts(t *testing.T) {
	forbidden := []string{
		"Transaction",
		"Procedure",
		"StateRuntime",
		"Checkpoint",
		"LogicalSlot",
	}
	for _, name := range exportedNames(t) {
		for _, fragment := range forbidden {
			if strings.Contains(name, fragment) {
				t.Fatalf("Bee exported symbol %s contains stateful concept %s", name, fragment)
			}
		}
	}
}

func exportedNames(t *testing.T) []string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate Bee package")
	}
	directory := filepath.Dir(currentFile)
	files, err := filepath.Glob(filepath.Join(directory, "*.go"))
	if err != nil {
		t.Fatalf("glob Bee source: %v", err)
	}

	set := token.NewFileSet()
	names := make([]string, 0, 32)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(set, file, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", file, parseErr)
		}
		for _, declaration := range parsed.Decls {
			switch value := declaration.(type) {
			case *ast.GenDecl:
				for _, specification := range value.Specs {
					switch specification := specification.(type) {
					case *ast.TypeSpec:
						if specification.Name.IsExported() {
							names = append(names, specification.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range specification.Names {
							if name.IsExported() {
								names = append(names, name.Name)
							}
						}
					}
				}
			case *ast.FuncDecl:
				if value.Name.IsExported() {
					names = append(names, value.Name.Name)
				}
			}
		}
	}
	return names
}
