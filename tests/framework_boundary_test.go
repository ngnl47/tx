package tests

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCorePackagesDoNotDependOnFrameworkFacade(t *testing.T) {
	patterns := []string{
		"./change",
		"./component",
		"./internal/framework",
		"./lifecycle",
		"./meta",
		"./modules/...",
		"./protocol/...",
		"./storage/...",
		"./wal/...",
	}
	for _, pattern := range patterns {
		for _, imported := range packageImports(t, pattern) {
			if imported == "github.com/ngnl47/tx/server" || strings.HasPrefix(imported, "github.com/ngnl47/tx/server/") ||
				imported == "github.com/ngnl47/tx/bee" || strings.HasPrefix(imported, "github.com/ngnl47/tx/bee/") ||
				imported == "github.com/ngnl47/tx/ant" || strings.HasPrefix(imported, "github.com/ngnl47/tx/ant/") {
				t.Fatalf("Core package %s depends on framework facade %s", pattern, imported)
			}
		}
	}
}

func TestBeeDoesNotDependOnAnt(t *testing.T) {
	for _, imported := range packageImports(t, "./bee/...") {
		if imported == "github.com/ngnl47/tx/ant" || strings.HasPrefix(imported, "github.com/ngnl47/tx/ant/") {
			t.Fatalf("Bee depends on Ant package %s", imported)
		}
		if imported == "github.com/ngnl47/tx/server" || strings.HasPrefix(imported, "github.com/ngnl47/tx/server/") {
			t.Fatalf("Bee depends on removed Server package %s", imported)
		}
	}
}

func TestAntDoesNotDependOnBee(t *testing.T) {
	for _, imported := range packageImports(t, "./ant/...") {
		if imported == "github.com/ngnl47/tx/bee" || strings.HasPrefix(imported, "github.com/ngnl47/tx/bee/") {
			t.Fatalf("Ant depends on Bee package %s", imported)
		}
		if imported == "github.com/ngnl47/tx/server" || strings.HasPrefix(imported, "github.com/ngnl47/tx/server/") {
			t.Fatalf("Ant depends on removed Server package %s", imported)
		}
	}
}

func TestLegacyServerPackageIsRemoved(t *testing.T) {
	for _, current := range packageList(t, "./...") {
		if current == "github.com/ngnl47/tx/server" || strings.HasPrefix(current, "github.com/ngnl47/tx/server/") {
			t.Fatalf("removed Server package still exists: %s", current)
		}
	}
}

func packageImports(t *testing.T, pattern string) []string {
	t.Helper()
	command := exec.Command("go", "list", "-deps", pattern)
	command.Dir = filepath.Clean("..")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pattern, err, output)
	}
	return strings.Fields(string(output))
}

func packageList(t *testing.T, pattern string) []string {
	t.Helper()
	command := exec.Command("go", "list", pattern)
	command.Dir = filepath.Clean("..")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pattern, err, output)
	}
	return strings.Fields(string(output))
}
