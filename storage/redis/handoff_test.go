package redis

import "testing"

var benchmarkHandoffKey string

func TestHandoffKeyUsesSamePhysicalSlotAsDocument(t *testing.T) {
	for _, prefix := range []string{
		"role:{1001}",
		"role:{7}:1001",
		"world:{zone-47}:bucket-3",
	} {
		documentSlot := redisClusterSlot(prefix)
		handoffKey := redisHandoffKey(documentSlot)
		if got := redisClusterSlot(handoffKey); got != documentSlot {
			t.Fatalf("handoff key %q slot = %d, want document slot %d", handoffKey, got, documentSlot)
		}
	}
}

func TestRedisClusterSlotMatchesPublishedExample(t *testing.T) {
	if got := redisClusterSlot("{user1000}.following"); got != 3443 {
		t.Fatalf("slot = %d, want 3443", got)
	}
	if got := redisClusterSlot("{user1000}.followers"); got != 3443 {
		t.Fatalf("shared hash tag slot = %d, want 3443", got)
	}
}

func TestRedisHandoffKeySteadyStateDoesNotAllocate(t *testing.T) {
	redisHandoffKey(0)
	allocations := testing.AllocsPerRun(1000, func() {
		benchmarkHandoffKey = redisHandoffKey(3443)
	})
	if allocations != 0 {
		t.Fatalf("redisHandoffKey allocations = %v, want 0 after table initialization", allocations)
	}
}

func BenchmarkRedisHandoffKey(b *testing.B) {
	redisHandoffKey(0)
	prefixes := [...]string{
		"role:{470001}",
		"legion:{9223372036854775807}",
		"world:{zone-1001}:bucket:255",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; b.Loop(); index++ {
		prefix := prefixes[index%len(prefixes)]
		benchmarkHandoffKey = redisHandoffKey(redisClusterSlot(prefix))
	}
}

func TestRedisHandoffKeysCoverEveryPhysicalSlot(t *testing.T) {
	for slot := range uint16(redisClusterSlotCount) {
		key := redisHandoffKey(slot)
		if key == "" {
			t.Fatalf("slot %d has empty handoff key", slot)
		}
		if got := redisClusterSlot(key); got != slot {
			t.Fatalf("handoff key %q maps to slot %d, want %d", key, got, slot)
		}
	}
}
