package wal

import "testing"

var benchmarkHash [32]byte

func BenchmarkNewRecord(b *testing.B) {
	stream := Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	payload := make([]byte, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		record, err := NewRecord(stream, 1, 1, payload)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkHash = record.PayloadHash
	}
}
