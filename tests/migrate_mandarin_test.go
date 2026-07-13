package tests

import (
	"testing"
	"time"

	"github.com/ngnl47/gobricks/meta"
	"github.com/ngnl47/gobricks/migrate"
)

func TestMigrateMandarinLegacyTags(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/role.go": `package data

// @mongodb:"routeType:role,routeBy:roleId,idField:roleId,collection:roles,enableSharding"
// @redis:"routeType:role,routeBy:roleId,idField:roleId,key:role,omitempty"
// @storage:"mode:redisDelayMongoDB,delaySaveTime:600,delayDelTime:86400"
type GDRole struct {
	RoleID int64 ` + "`alias:\"roleId\" mongodb:\"index,unique\"`" + `
	Items []string ` + "`json:\"items\" redis:\"subKey:items\"`" + `
}
`,
	})

	specs, report, err := migrate.MandarinDir(root + "/data")
	if err != nil {
		t.Fatalf("MandarinDir returned error: %v", err)
	}

	spec := findMigratedSpec(t, specs, "example.com/project/data.GDRole")
	if report.MigratedTypes != 1 {
		t.Fatalf("migrated types = %d", report.MigratedTypes)
	}
	if spec.Storage.Mode != meta.StorageModeFusion {
		t.Fatalf("storage mode = %q", spec.Storage.Mode)
	}
	if spec.Storage.DelaySave.Duration != 10*time.Minute {
		t.Fatalf("delay save = %s", spec.Storage.DelaySave.Duration)
	}
	if spec.Storage.DelayDelete.Duration != 24*time.Hour {
		t.Fatalf("delay delete = %s", spec.Storage.DelayDelete.Duration)
	}
	if spec.Redis.Route.Type != "role" || spec.Redis.Route.By != "roleId" {
		t.Fatalf("redis route = %#v", spec.Redis.Route)
	}
	if spec.Redis.IDField != "roleId" || spec.Redis.Key != "role" || !spec.Redis.OmitEmpty {
		t.Fatalf("redis spec = %#v", spec.Redis)
	}
	if spec.Mongo.Route.Type != "role" || spec.Mongo.Route.By != "roleId" {
		t.Fatalf("mongo route = %#v", spec.Mongo.Route)
	}
	if spec.Mongo.IDField != "roleId" || spec.Mongo.Collection != "roles" || !spec.Mongo.Sharding.Enabled {
		t.Fatalf("mongo spec = %#v", spec.Mongo)
	}
	if spec.Fields["items"].Redis.SubKey != "items" {
		t.Fatalf("items redis field = %#v", spec.Fields["items"].Redis)
	}
	if !spec.Fields["roleId"].Mongo.Indexes[0].Unique {
		t.Fatalf("roleId mongo field = %#v", spec.Fields["roleId"].Mongo)
	}
	if len(report.LegacyAliases) == 0 {
		t.Fatal("expected legacy alias report entries")
	}
}

func TestMigrateMandarinReportsMissingFields(t *testing.T) {
	root := writeTempModule(t, map[string]string{
		"data/bad.go": `package data

// @redis:"routeBy:missing,idField:id,key:bad"
// @storage:"mode:redis"
type Bad struct {
	ID int64 ` + "`alias:\"id\"`" + `
}
`,
	})

	_, report, err := migrate.MandarinDir(root + "/data")
	if err != nil {
		t.Fatalf("MandarinDir returned error: %v", err)
	}
	if len(report.MissingFields) != 1 {
		t.Fatalf("missing fields = %#v", report.MissingFields)
	}
	if report.MissingFields[0].Field != "missing" {
		t.Fatalf("missing field = %#v", report.MissingFields[0])
	}
}

func findMigratedSpec(t *testing.T, specs []meta.TypeSpec, fullName string) meta.TypeSpec {
	t.Helper()
	for _, spec := range specs {
		if spec.Type.Ref.FullName == fullName {
			return spec
		}
	}
	t.Fatalf("missing migrated spec %s in %#v", fullName, specs)
	return meta.TypeSpec{}
}
