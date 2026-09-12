//go:build unix

package actortrial

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestComparisonCountsOnlyCompletedRPC(t *testing.T) {
	var calls atomic.Int64
	stats := runComparison(comparisonLoad{Requests: 12, Concurrency: 3}, func(context.Context, int) error {
		if calls.Add(1)%3 == 0 {
			return errors.New("预期拒绝")
		}
		return nil
	})
	if calls.Load() != 12 || stats.Completed != 8 || stats.Errors["预期拒绝"] != 4 || stats.Attempted != 12 {
		t.Fatalf("完整 RPC 计数错误: %+v calls=%d", stats, calls.Load())
	}
	if stats.P99Ns < stats.P50Ns || stats.Seconds <= 0 {
		t.Fatal(stats)
	}
}
