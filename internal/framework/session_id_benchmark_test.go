package framework_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ngnl47/tx/bee"
	txetcd "github.com/ngnl47/tx/etcd"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/sessionid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 仅显式指定 TX_ETCD_ENDPOINT 和 -bench 时写入隔离前缀；不压缩或改动共享 etcd 配置。
// 例如：go test ./internal/framework -run '^$' -bench '^BenchmarkSessionIDEtcdPath$' -benchtime=3s -count=3。
func BenchmarkSessionIDEtcdPath(b *testing.B) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		b.Skip("set TX_ETCD_ENDPOINT to run the etcd benchmark")
	}
	ctx, cancel := context.WithTimeout(b.Context(), 3*time.Minute)
	defer cancel()
	root := "/tx/tests/session-id-benchmark-" + uuid.NewString()
	raw, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		defer raw.Close()
		if _, err := raw.Delete(cleanupCtx, root+"/", clientv3.WithPrefix()); err != nil {
			b.Error(err)
		}
		remaining, err := raw.Get(cleanupCtx, root+"/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			b.Error(err)
		} else if remaining.Count != 0 {
			b.Errorf("基准前缀残留 %d 个键: %s", remaining.Count, root)
		}
		after, err := raw.Status(cleanupCtx, endpoint)
		if err != nil {
			b.Error(err)
		} else {
			b.Logf("etcd-after db_bytes=%d db_in_use_bytes=%d revision=%d root=%s", after.DbSize, after.DbSizeInUse, after.Header.Revision, root)
		}
	})
	before, err := raw.Status(ctx, endpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("etcd-before db_bytes=%d db_in_use_bytes=%d revision=%d root=%s", before.DbSize, before.DbSizeInUse, before.Header.Revision, root)
	client, err := txetcd.NewWithClient(raw, false)
	if err != nil {
		b.Fatal(err)
	}
	var serviceContext bee.Context
	server, err := bee.New(bee.Config{
		AppID: "session-id-benchmark",
		Discovery: bee.DiscoveryConfig{Etcd: discovery.EtcdConfig{
			Endpoints: []string{endpoint}, Prefix: root + "/discovery", LeaseTTL: 5 * time.Second, DialTimeout: time.Second,
		}},
	}).Services(bee.ServiceRegistration{
		Spec: bee.ServiceSpec{Name: "login", Requires: []bee.CModID{bee.CModSessionID}},
		Factory: func(runtime bee.Context) (bee.Service, error) {
			serviceContext = runtime
			return sessionIDBenchmarkService{}, nil
		},
	}).Build()
	if err != nil {
		b.Fatal(err)
	}
	if err := server.Start(ctx, bee.RunOptions{Services: []string{"login"}, InstanceEndpoint: "127.0.0.1:19001"}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(stopCtx); err != nil {
			b.Error(err)
		}
	})
	for _, path := range []struct {
		name     string
		allocate func(context.Context) (int64, error)
	}{
		{"PutRevision", func(ctx context.Context) (int64, error) {
			return client.PutRevision(ctx, root+"/put-revision", []byte("1"), 0)
		}},
		{"CModSessionID", func(ctx context.Context) (int64, error) {
			return serviceContext.CModSessionID().AllocateSessionID(ctx)
		}},
	} {
		for _, concurrency := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("%s/concurrency=%d", path.name, concurrency), func(b *testing.B) {
				benchmarkSessionIDPath(b, ctx, concurrency, path.allocate)
			})
		}
	}
}

type sessionIDBenchmarkService struct{}

func (sessionIDBenchmarkService) Init(context.Context) error { return nil }

func benchmarkSessionIDPath(b *testing.B, ctx context.Context, concurrency int, allocate func(context.Context) (int64, error)) {
	for range 32 {
		if _, err := allocate(ctx); err != nil {
			b.Fatal(err)
		}
	}
	// 每个 worker 只写自己的采样位置；排序和去重在计时结束后执行。
	type sample struct {
		id      int64
		elapsed time.Duration
		err     error
	}
	samples := make([]sample, b.N)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	for worker := range concurrency {
		go func() {
			defer workers.Done()
			for index := worker; index < b.N; index += concurrency {
				start := time.Now()
				id, err := allocate(ctx)
				samples[index] = sample{id: id, elapsed: time.Since(start), err: err}
			}
		}()
	}
	workers.Wait()
	elapsed := time.Since(started)
	b.StopTimer()
	latencies := make([]time.Duration, 0, b.N)
	seen := make(map[int64]struct{}, b.N)
	var failures, duplicates, invalid int
	var firstErr error
	for _, sample := range samples {
		if sample.err != nil {
			failures++
			if firstErr == nil {
				firstErr = sample.err
			}
			continue
		}
		latencies = append(latencies, sample.elapsed)
		if sample.id <= 0 || sample.id > sessionid.MaxSessionID {
			invalid++
		}
		if _, exists := seen[sample.id]; exists {
			duplicates++
		}
		seen[sample.id] = struct{}{}
	}
	slices.Sort(latencies)
	b.ReportMetric(float64(len(latencies))/elapsed.Seconds(), "success/s")
	b.ReportMetric(float64(failures), "errors")
	b.ReportMetric(float64(duplicates), "duplicates")
	b.ReportMetric(float64(invalid), "invalid")
	if len(latencies) > 0 {
		b.ReportMetric(float64(latencies[(len(latencies)*50-1)/100])/float64(time.Microsecond), "p50-us")
		b.ReportMetric(float64(latencies[(len(latencies)*99-1)/100])/float64(time.Microsecond), "p99-us")
	}
	if failures != 0 || duplicates != 0 || invalid != 0 {
		b.Fatalf("errors=%d duplicates=%d invalid=%d first_error=%v", failures, duplicates, invalid, firstErr)
	}
}
