package tests

import (
	"testing"

	"github.com/ngnl47/gobricks/storage"
)

func TestRedisKeyUsesShardByAsHashTagWhenDifferentFromID(t *testing.T) {
	cfg := storage.RedisConfig{
		ID:    "roleId",
		Key:   "role",
		Route: storage.RouteConfig{By: "serverId"},
	}

	key, err := cfg.KeyBuilder("evo").Build(map[string]string{
		"roleId":   "42",
		"serverId": "7",
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if key != "evo:role:{7}:42" {
		t.Fatalf("key = %q", key)
	}
}

func TestRedisKeyUsesIDAsHashTagWhenIDMatchesShardBy(t *testing.T) {
	cfg := storage.RedisConfig{
		ID:    "roleId",
		Key:   "role",
		Route: storage.RouteConfig{By: "roleId"},
	}

	key, err := cfg.KeyBuilder("evo").Build(map[string]string{"roleId": "42"})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if key != "evo:role:{42}" {
		t.Fatalf("key = %q", key)
	}
}
