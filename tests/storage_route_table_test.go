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
		{name: "numeric-looking string uses hash", shardKey: "12", want: "role-d"},
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

func TestRouteTableImplementsMongoRouter(t *testing.T) {
	table := storage.NewRouteTable(map[string][]storage.RouteRule{
		"global": {
			{Shards: []string{"main"}},
		},
	})

	mongoShard, err := table.RouteMongo(context.Background(), "global", "string-id")
	if err != nil {
		t.Fatalf("RouteMongo returned error: %v", err)
	}
	if mongoShard != "main" {
		t.Fatalf("RouteMongo = %q, want main", mongoShard)
	}
}

func TestRouteTableCopiesShardSlices(t *testing.T) {
	shards := []string{"role-a", "role-b"}
	table := storage.NewRouteTable(map[string][]storage.RouteRule{
		"role": {{Min: 1, Shards: shards}},
	})

	shards[0] = "mutated"

	got, err := table.Route(context.Background(), "role", int64(2))
	if err != nil {
		t.Fatalf("Route returned error: %v", err)
	}
	if got != "role-a" {
		t.Fatalf("Route = %q, want copied shard role-a", got)
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
		{name: "unsupported shard key type", table: storage.NewRouteTable(map[string][]storage.RouteRule{
			"role": {{Min: 1, Shards: []string{"a", "b"}}},
		}), route: "role", shardKey: struct{ ID int }{ID: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := tt.table.Route(context.Background(), tt.route, tt.shardKey); err == nil {
				t.Fatalf("Route = %q, want error", got)
			}
		})
	}
}
