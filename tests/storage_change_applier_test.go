package tests

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestRedisAdapterImplementsDeltaApplier(t *testing.T) {
	var _ storage.DeltaApplier = (*txredis.Adapter)(nil)
}

func TestRedisUsesStorageApplyOptionsDirectly(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(file))
	for _, name := range []string{
		filepath.Join(root, "storage", "redis", "adapter.go"),
		filepath.Join(root, "storage", "redis", "cluster.go"),
	} {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile(%s) returned error: %v", name, err)
		}
		text := string(body)
		for _, forbidden := range []string{
			"type ApplyOptions = storage.ApplyOptions",
			"options ApplyOptions",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s contains Redis-local ApplyOptions surface %q", name, forbidden)
			}
		}
	}
}
