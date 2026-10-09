//go:build txbench

package runtime

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	storageconfig "github.com/ngnl47/tx/storage/config"
	redis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/event"
)

func TestIOStatsPreservesDriverResultsAndBoundsCategories(t *testing.T) {
	runtime := New(context.Background())
	failure := errors.New("backend failure")
	hook := runtime.io.redisHook()
	call := hook.ProcessHook(func(context.Context, redis.Cmder) error { return failure })
	if err := call(context.Background(), redis.NewStringCmd(context.Background(), "get", "private-key")); err != failure {
		t.Fatal("驱动错误被改变")
	}
	missing := hook.ProcessHook(func(context.Context, redis.Cmder) error { return redis.Nil })
	_ = missing(context.Background(), redis.NewStringCmd(context.Background(), "get", "missing"))
	for range 100 {
		_ = hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })(context.Background(), redis.NewCmd(context.Background(), "custom-command", "secret"))
	}
	monitor := runtime.io.mongoMonitor()
	monitor.Succeeded(context.Background(), &event.CommandSucceededEvent{CommandFinishedEvent: event.CommandFinishedEvent{CommandName: "update", Duration: 3 * time.Millisecond}})
	monitor.Failed(context.Background(), &event.CommandFailedEvent{CommandFinishedEvent: event.CommandFinishedEvent{CommandName: "update", Duration: 5 * time.Millisecond}})
	pipeline := hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return failure })
	if err := pipeline(context.Background(), []redis.Cmder{redis.NewCmd(context.Background(), "get", "a"), redis.NewCmd(context.Background(), "get", "b")}); err != failure {
		t.Fatal("批处理错误被改变")
	}
	stats := runtime.IOStats()
	if len(stats) != 4 {
		t.Fatalf("类别数量 = %d", len(stats))
	}
	for _, stat := range stats {
		switch stat.Backend + "/" + stat.Operation {
		case "redis/get":
			if stat.Calls != 2 || stat.Errors != 1 {
				t.Fatalf("get = %+v", stat)
			}
		case "redis/other":
			if stat.Calls != 100 || stat.Errors != 0 {
				t.Fatalf("other = %+v", stat)
			}
		case "mongo/update":
			if stat.Calls != 2 || stat.Errors != 1 || stat.TotalDuration != 8*time.Millisecond || stat.MaxDuration != 5*time.Millisecond {
				t.Fatalf("mongo = %+v", stat)
			}
		case "redis/pipeline":
			if stat.Calls != 1 || stat.Commands != 2 || stat.Errors != 1 {
				t.Fatalf("pipeline = %+v", stat)
			}
		default:
			t.Fatalf("非固定统计标签 = %+v", stat)
		}
	}
}

func TestIOStatsRuntimeRealBackends(t *testing.T) {
	redisURI, mongoURI := os.Getenv("TX_IO_REDIS_URI"), os.Getenv("TX_IO_MONGO_URI")
	if redisURI == "" || mongoURI == "" {
		t.Skip("set TX_IO_REDIS_URI and TX_IO_MONGO_URI for real backend verification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime := New(ctx)
	config := func(endpoint string) storageconfig.RouteTableConfig {
		return storageconfig.RouteTableConfig{Routes: map[string]storageconfig.RouteTarget{"test": {Rules: []storageconfig.RouteRule{{Min: 1, Shards: []string{endpoint}}}}}}
	}
	if err := runtime.StartRedis(ctx, storageconfig.NewRedisConfig(map[string]string{"test": redisURI})); err != nil {
		t.Fatal(err)
	}
	defer runtime.CloseRedis()
	if err := runtime.StartMongo(ctx, config(mongoURI)); err != nil {
		t.Fatal(err)
	}
	defer runtime.CloseMongo(context.Background())
	// 只读一个不存在的测试键，不创建业务数据或扫描数据库。
	if err := runtime.redisClients[0].Get(ctx, "tx:test:io-stats:absent").Err(); err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, stat := range runtime.IOStats() {
		seen[stat.Backend+"/"+stat.Operation] = true
		if stat.Calls == 0 || stat.Commands == 0 {
			t.Fatalf("真实后端统计 = %+v", stat)
		}
		if (stat.Operation == "ping" || stat.Operation == "get") && stat.Errors != 0 {
			t.Fatalf("读取或探测失败 = %+v", stat)
		}
	}
	for _, key := range []string{"redis/ping", "redis/get", "mongo/ping"} {
		if !seen[key] {
			t.Fatalf("未采集 %s", key)
		}
	}
}

func BenchmarkIOStatsRecord(b *testing.B) {
	var stats ioStats
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stats.record(1, 1, time.Microsecond, false)
		}
	})
}
