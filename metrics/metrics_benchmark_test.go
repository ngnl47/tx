package metrics

import (
	"testing"
	"time"
)

func BenchmarkRecordUplinkAndDownlink(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		RecordAgentUplink(true, time.Millisecond)
		RecordAgentDownlink(1, time.Millisecond, nil)
	}
}
