package tests

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/storage"
)

func TestRouteTableRoutesByRangeAndModulo(t *testing.T) {
	table := storage.NewRouteTable(map[string][]storage.RouteRule{
		"role": {
			{Min: 1, Max: 9, Shards: []string{"role-a", "role-b"}},
			{Min: 10, Shards: []string{"role-c", "role-d", "role-e"}},
		},
	})

	tests := []struct {
		name     string
		shardKey any
		want     string
	}{
		{name: "first range first shard", shardKey: int64(2), want: "role-a"},
		{name: "first range second shard", shardKey: int64(3), want: "role-b"},
		{name: "open ended range", shardKey: int64(11), want: "role-e"},
		{name: "numeric string", shardKey: "12", want: "role-c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := table.Route(context.Background(), "role", tt.shardKey)
			if err != nil {
				t.Fatalf("Route returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Route = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRouteTableImplementsRedisAndMongoRouter(t *testing.T) {
	table := storage.NewRouteTable(map[string][]storage.RouteRule{
		"global": {
			{Shards: []string{"main"}},
		},
	})

	redisShard, err := table.RouteRedis(context.Background(), "global", "string-id")
	if err != nil {
		t.Fatalf("RouteRedis returned error: %v", err)
	}
	if redisShard != "main" {
		t.Fatalf("RouteRedis = %q, want main", redisShard)
	}

	mongoShard, err := table.RouteMongo(context.Background(), "global", "string-id")
	if err != nil {
		t.Fatalf("RouteMongo returned error: %v", err)
	}
	if mongoShard != "main" {
		t.Fatalf("RouteMongo = %q, want main", mongoShard)
	}
}

func TestRouteTableReturnsErrorsForInvalidConfig(t *testing.T) {
	tests := []struct {
		name     string
		table    *storage.RouteTable
		route    string
		shardKey any
	}{
		{name: "nil table", table: nil, route: "role", shardKey: int64(1)},
		{name: "missing route", table: storage.NewRouteTable(nil), route: "role", shardKey: int64(1)},
		{name: "empty shards", table: storage.NewRouteTable(map[string][]storage.RouteRule{
			"role": {{Min: 1}},
		}), route: "role", shardKey: int64(1)},
		{name: "non numeric multi shard", table: storage.NewRouteTable(map[string][]storage.RouteRule{
			"role": {{Min: 1, Shards: []string{"a", "b"}}},
		}), route: "role", shardKey: "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := tt.table.Route(context.Background(), tt.route, tt.shardKey); err == nil {
				t.Fatalf("Route = %q, want error", got)
			}
		})
	}
}
