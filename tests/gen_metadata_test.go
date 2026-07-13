package tests

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ngnl47/gobricks/gen"
)

func TestGenLoadTagsAndScanRendersStableMetadataSnapshot(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/role.go": `package data

// @storage:` + "`mode:\"fusion\" delaySaveTime:\"600s\" delayDelTime:\"24h\"`" + `
// @redis:` + "`routeType:\"role\" routeBy:\"serverId\" idField:\"roleId\" key:\"role\" omitempty:\"true\"`" + `
// @mongodb:` + "`routeType:\"role\" routeBy:\"serverId\" idField:\"roleId\" collection:\"roles\" enableSharding:\"true\"`" + `
type Role struct {
	RoleID int64 ` + "`alias:\"roleId\"`" + `
	ServerID int64 ` + "`alias:\"serverId\"`" + `
	Items []string ` + "`json:\"items\" redis:\"subKey:items\"`" + `
}
`,
	})

	model, err := gen.LoadTagsAndScan(filepath.Join(root, "data"))
	if err != nil {
		t.Fatalf("LoadTagsAndScan returned error: %v", err)
	}
	assertMetadataSnapshot(t, model)
}

func assertMetadataSnapshot(t *testing.T, model *gen.Model) {
	t.Helper()
	snapshot, err := gen.RenderMetadata(model)
	if err != nil {
		t.Fatalf("RenderMetadata returned error: %v", err)
	}
	for _, want := range []string{
		`"typeName": "example.com/project/data.Role"`,
		`"mode": "fusion"`,
		`"key": "role"`,
		`"collection": "roles"`,
		`"subKey": "items"`,
	} {
		if !strings.Contains(snapshot, want) {
			t.Fatalf("snapshot missing %s:\n%s", want, snapshot)
		}
	}
}
