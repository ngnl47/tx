package storage

import (
	"context"
	"hash/fnv"
	"math"
	"testing"
)

func TestRouteTableRoutesStringKeyWithStableFNV1a(t *testing.T) {
	shards := []string{"role-a", "role-b", "role-c"}
	table := NewRouteTable(map[string][]RouteRule{
		"role": {{Shards: shards}},
	})

	for _, key := range []string{"role-1001", "1001", "open-id-47"} {
		got, err := table.Route(context.Background(), "role", key)
		if err != nil {
			t.Fatalf("Route(%q) error = %v", key, err)
		}
		want := shards[referenceStringRouteHash(key)%uint32(len(shards))]
		if got != want {
			t.Fatalf("Route(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestRouteTableAppliesRangeToStringHash(t *testing.T) {
	key := "role-1001"
	hash := int64(referenceStringRouteHash(key))
	table := NewRouteTable(map[string][]RouteRule{
		"role": {
			{Min: 1, Max: hash - 1, Shards: []string{"before"}},
			{Min: hash, Max: math.MaxInt64, Shards: []string{"matched"}},
		},
	})

	got, err := table.Route(context.Background(), "role", key)
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	if got != "matched" {
		t.Fatalf("Route() = %q, want matched", got)
	}
}

func TestRouteTableRejectsUnsupportedKeyOnSingleShard(t *testing.T) {
	table := NewRouteTable(map[string][]RouteRule{
		"global": {{Shards: []string{"main"}}},
	})

	for _, key := range []any{nil, struct{ ID int }{ID: 1}} {
		if _, err := table.Route(context.Background(), "global", key); err == nil {
			t.Fatalf("Route(%T) error = nil, want unsupported key error", key)
		}
	}
}

func BenchmarkRouteTableStringKey(b *testing.B) {
	table := NewRouteTable(map[string][]RouteRule{
		"role": {{Shards: []string{"role-a", "role-b", "role-c", "role-d"}}},
	})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := table.Route(ctx, "role", "open-id-470001")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func referenceStringRouteHash(value string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	sum := h.Sum32()
	if sum == 0 {
		return 1
	}
	return sum
}
