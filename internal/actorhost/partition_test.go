package actorhost

import "testing"

// 固定向量保护持久归属键，不能随哈希、编码或实例数变化而悄悄漂移。
func TestPartitionSlotV1(t *testing.T) {
	for _, tc := range []struct {
		id   Identity
		want string
	}{
		{Identity{"role", "1"}, "actor-v1.cm9sZQ.p212"},
		{Identity{"role", "01"}, "actor-v1.cm9sZQ.p245"},
		{Identity{"role", " 1"}, "actor-v1.cm9sZQ.p649"},
		{Identity{"role", "角色/一"}, "actor-v1.cm9sZQ.p277"},
		{Identity{"room", "1"}, "actor-v1.cm9vbQ.p212"},
		{Identity{"role", "8"}, "actor-v1.cm9sZQ.p100"},
		{Identity{"role", "26"}, "actor-v1.cm9sZQ.p100"},
	} {
		if got := PartitionSlot(tc.id); got != tc.want {
			t.Errorf("%+v: %q, want %q", tc.id, got, tc.want)
		}
	}
}
