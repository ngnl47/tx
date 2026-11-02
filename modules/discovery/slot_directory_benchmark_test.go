package discovery

import (
	"fmt"
	"testing"
)

// benchmarkSlotAssignments 模拟稳定槽位，不把逐角色数量当成目录大小。
func benchmarkSlotAssignments(count int) []SlotAssignment {
	values := make([]SlotAssignment, count)
	for i := range values {
		values[i] = SlotAssignment{
			SlotID:            fmt.Sprintf("actor-v1.role%d.p%d", i/1024, i%1024),
			RuntimeInstanceID: "game-1234-567890", Generation: 1,
		}
	}
	return values
}

// BenchmarkSlotDirectorySnapshot 单独记录启动和断 Watch 重建所需的完整快照成本。
func BenchmarkSlotDirectorySnapshot(b *testing.B) {
	for _, count := range []int{1024, 8192, 65536} {
		b.Run(fmt.Sprintf("slots=%d", count), func(b *testing.B) {
			values := benchmarkSlotAssignments(count)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				directory := NewSlotDirectory()
				if err := directory.ReplaceSnapshot(values, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSlotDirectoryWatchUpdate 测量已解码变更到可查询目录的更新；不含网络和 JSON 解码。
func BenchmarkSlotDirectoryWatchUpdate(b *testing.B) {
	for _, count := range []int{1024, 8192, 65536} {
		for _, batch := range []int{1, 32, 256} {
			b.Run(fmt.Sprintf("slots=%d/batch=%d", count, batch), func(b *testing.B) {
				values := benchmarkSlotAssignments(count)
				directory := NewSlotDirectory()
				if err := directory.ReplaceSnapshot(values, 1); err != nil {
					b.Fatal(err)
				}
				revision, offset := int64(1), 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					revision++
					changes := make([]slotChange, 0, batch)
					for i := range batch {
						value := &values[(offset+i)%count]
						value.Generation++
						changes = append(changes, slotChange{assignment: *value, revision: revision})
					}
					if err := directory.applyChanges(changes, revision); err != nil {
						b.Fatal(err)
					}
					offset = (offset + batch) % count
				}
			})
		}
	}
}
