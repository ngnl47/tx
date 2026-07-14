package tests

import (
	"testing"

	"github.com/ngnl47/tx/storage"
)

func TestMongoFilterIncludesShardByWhenShardingEnabled(t *testing.T) {
	cfg := storage.MongoConfig{
		ID:         "roleId",
		Collection: "roles",
		Route:      storage.RouteConfig{By: "serverId"},
		Sharding:   storage.ShardingConfig{Enabled: true},
	}

	filter, err := cfg.Filter(map[string]any{"roleId": int64(42), "serverId": int64(7)})
	if err != nil {
		t.Fatalf("Filter returned error: %v", err)
	}
	if filter["roleId"] != int64(42) || filter["serverId"] != int64(7) {
		t.Fatalf("filter = %#v", filter)
	}
}

func TestMongoFilterOmitsShardByWhenIDMatchesShardBy(t *testing.T) {
	cfg := storage.MongoConfig{
		ID:         "roleId",
		Collection: "roles",
		Route:      storage.RouteConfig{By: "roleId"},
		Sharding:   storage.ShardingConfig{Enabled: true},
	}

	filter, err := cfg.Filter(map[string]any{"roleId": int64(42)})
	if err != nil {
		t.Fatalf("Filter returned error: %v", err)
	}
	if len(filter) != 1 || filter["roleId"] != int64(42) {
		t.Fatalf("filter = %#v", filter)
	}
}
