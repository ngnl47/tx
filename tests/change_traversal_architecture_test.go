package tests

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMaterializedDeltaSetAPIIsRemoved(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(file))
	checks := map[string][]string{
		filepath.Join(root, "change", "path.go"): {
			"type DeltaEntry struct",
			"type DeltaSet []DeltaEntry",
			"func (p DeltaPath) PrefixOf(",
		},
		filepath.Join(root, "change", "source.go"): {
			"type DeltaCollector struct",
		},
		filepath.Join(root, "storage", "redis", "document.go"): {
			"func BuildDocumentPatchesWithDeltas(",
		},
		filepath.Join(root, "storage", "mongo", "document.go"): {
			"func BuildUpdateDocumentWithDeltas(",
		},
	}
	for name, forbidden := range checks {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile(%s) returned error: %v", name, err)
		}
		for _, declaration := range forbidden {
			if strings.Contains(string(body), declaration) {
				t.Errorf("%s still contains obsolete materialized API %q", name, declaration)
			}
		}
	}
}

func TestChangeTraversalAPIUsesVisitorNaming(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(file))
	required := map[string][]string{
		filepath.Join(root, "change", "source.go"): {
			"type Visitor interface",
			"VisitChange(Path, Op) error",
			"type Source interface",
			"HasChanges() bool",
			"VisitChanges(Visitor) error",
		},
		filepath.Join(root, "change", "path.go"): {
			"type Path struct",
			"type Op uint8",
		},
		filepath.Join(root, "change", "types.go"): {
			"type DocumentPath []string",
		},
		filepath.Join(root, "storage", "changes.go"): {
			"type ChangeApplier interface",
			"ApplyChangesWithOptions(",
			"type ChangeStore interface",
			"change.ValueResolver",
		},
	}
	for name, declarations := range required {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile(%s) returned error: %v", name, err)
		}
		for _, declaration := range declarations {
			if !strings.Contains(string(body), declaration) {
				t.Errorf("%s does not contain required API %q", name, declaration)
			}
		}
	}

	forbidden := []string{
		"DeltaSource",
		"DeltaProjector",
		"DeltaInspectable",
		"DeltaVisitor",
		"DeltaPath",
		"DeltaOp",
		"DeltaValueResolver",
		"DeltaStore",
		"DeltaApplier",
		"ApplyDeltasWithOptions",
		"HasDeltas",
		"ProjectDeltas",
		"ClearDeltas",
		"CollapseDeltas",
		"type Inspector interface",
		"ValueResolver() change.ValueResolver",
	}
	for _, relative := range []string{"change", "storage", filepath.Join("modules", "txn")} {
		err := filepath.WalkDir(filepath.Join(root, relative), func(name string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(name) != ".go" {
				return nil
			}
			body, readErr := os.ReadFile(name)
			if readErr != nil {
				return readErr
			}
			for _, symbol := range forbidden {
				if strings.Contains(string(body), symbol) {
					t.Errorf("%s still contains obsolete name %q", name, symbol)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("WalkDir(%s) returned error: %v", relative, err)
		}
	}
}
