//go:build unix

package actortrial

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestComparisonLayersRealNATS(t *testing.T) {
	url := natsURL(t)
	for _, backend := range []string{"nats-native", "nats-envelope", "nats-reply-unique", "nats-reply-fixed", "nats"} {
		t.Run(backend, func(t *testing.T) {
			config := comparisonConfig{Backend: backend, Mode: "server", Namespace: nats.NewInbox(), URL: url}
			server, err := newComparisonPeer(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.close(); err != nil {
					t.Error(err)
				}
			})
			config.Mode, config.Target = "client", server.endpoint
			client, err := newComparisonPeer(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.close(); err != nil {
					t.Error(err)
				}
			})
			stats := runComparison(comparisonLoad{Requests: 128, Concurrency: 16}, func(ctx context.Context, index int) error {
				return client.call(ctx, index, []byte(fmt.Sprintf("请求-%d", index)))
			})
			if stats.Completed != 128 || len(stats.Errors) != 0 {
				t.Fatalf("分层对照未完成并发响应校验: %+v", stats)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			cancel()
			if err := client.call(ctx, 0, []byte("取消")); !errors.Is(err, context.Canceled) {
				t.Fatalf("取消返回 %v", err)
			}
			config.Target += "-missing"
			missing, err := newComparisonPeer(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := missing.close(); err != nil {
					t.Error(err)
				}
			})
			ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := missing.call(ctx, 0, []byte("无接收者")); !errors.Is(err, nats.ErrNoResponders) {
				t.Fatalf("无接收者返回 %v", err)
			}
		})
	}
}

func TestComparisonCountsOnlyCompletedRPC(t *testing.T) {
	var calls atomic.Int64
	start := time.Now().Add(20 * time.Millisecond).UnixNano()
	stats := runComparison(comparisonLoad{Requests: 12, Concurrency: 3, StartAt: start}, func(context.Context, int) error {
		if time.Now().UnixNano() < start {
			t.Error("请求先于共同计划起点执行")
		}
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
	if stats.StartedAt < start || stats.FinishedAt <= stats.StartedAt || len(stats.Samples) != stats.Completed || len(stats.ScheduledSamples) != 0 {
		t.Fatalf("聚合窗口或成功样本错误: %+v", stats)
	}
}

func TestComparisonArrivalSamplesExcludeFailedRPC(t *testing.T) {
	stats := runComparison(comparisonLoad{Rate: 100, Duration: 100 * time.Millisecond, Concurrency: 1}, func(_ context.Context, index int) error {
		if index%2 == 0 {
			return errors.New("预期拒绝")
		}
		return nil
	})
	if stats.Offered != 10 || stats.Attempted+stats.Dropped != stats.Offered || stats.Completed+stats.Errors["预期拒绝"] != stats.Attempted {
		t.Fatalf("到达率计数不守恒: %+v", stats)
	}
	if stats.Completed == 0 || len(stats.Samples) != stats.Completed || len(stats.ScheduledSamples) != stats.Completed {
		t.Fatalf("聚合样本混入失败或未发送请求: %+v", stats)
	}
}
