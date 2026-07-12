package tests

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngnl47/gobricks/schema"
)

func TestSchemaLoadBytesFromJSON(t *testing.T) {
	doc, err := schema.LoadBytes([]byte(`{
		"version": 1,
		"types": {
			"example.com/project/data.Role": {
				"storage": {
					"mode": "fusion",
					"delaySave": "600s",
					"delayDelete": "24h"
				},
				"redis": {
					"route": {"type": "role", "by": "roleId"},
					"id": "roleId",
					"key": "role",
					"omitEmpty": true,
					"storeType": "string"
				},
				"mongo": {
					"route": {"type": "role", "by": "roleId"},
					"id": "roleId",
					"collection": "roles",
					"sharding": {"enabled": true}
				},
				"fields": {
					"items": {"redis": {"subKey": "items"}},
					"roleId": {"mongo": {"indexes": [{"unique": true}]}}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("LoadBytes returned error: %v", err)
	}

	role := doc.Types["example.com/project/data.Role"]
	if doc.Version != 1 {
		t.Fatalf("version = %d", doc.Version)
	}
	if role.Storage.Mode != schema.StorageModeFusion {
		t.Fatalf("storage mode = %q", role.Storage.Mode)
	}
	if role.Storage.DelaySave.Duration != 10*time.Minute {
		t.Fatalf("delay save = %s", role.Storage.DelaySave.Duration)
	}
	if role.Redis.Route.By != "roleId" {
		t.Fatalf("redis route by = %q", role.Redis.Route.By)
	}
	if !role.Mongo.Sharding.Enabled {
		t.Fatal("mongo sharding should be enabled")
	}
	if !role.Fields["roleId"].Mongo.Indexes[0].Unique {
		t.Fatal("roleId mongo index should be unique")
	}
}

func TestSchemaLoadBytesFromYAML(t *testing.T) {
	doc, err := schema.LoadBytes([]byte(`
version: 1
types:
  example.com/project/data.Role:
    storage:
      mode: fusion
      delaySave: 600s
      delayDelete: 24h
    redis:
      route:
        type: role
        by: roleId
      id: roleId
      key: role
      omitEmpty: true
      storeType: string
    mongo:
      route:
        type: role
        by: roleId
      id: roleId
      collection: roles
      sharding:
        enabled: true
    fields:
      items:
        redis:
          subKey: items
      roleId:
        mongo:
          indexes:
            - unique: true
`))
	if err != nil {
		t.Fatalf("LoadBytes returned error: %v", err)
	}

	role := doc.Types["example.com/project/data.Role"]
	if role.Redis.Key != "role" {
		t.Fatalf("redis key = %q", role.Redis.Key)
	}
	if got := role.Storage.DelayDelete.Duration; got != 24*time.Hour {
		t.Fatalf("delay delete = %s", got)
	}
}

func TestSchemaLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.schema.json")
	err := os.WriteFile(path, []byte(`{"version":1,"types":{}}`), 0o600)
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	doc, err := schema.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}
	if doc.Version != 1 {
		t.Fatalf("version = %d", doc.Version)
	}
}
