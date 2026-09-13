//go:build unix

package actortrial

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/protocol"
)

// 此文件仅编入测试二进制。外部编排器负责隔离进程、依赖、采样顺序和资源清理。
type comparisonConfig struct{ Backend, Mode, Namespace, URL, Etcd, Target, Profile string }
type comparisonLoad struct {
	Action                                         string
	Payload, Entities, Concurrency, Requests, Rate int
	Duration                                       time.Duration
	Profile                                        string
	StartAt                                        int64
}
type comparisonResources struct {
	CPUSeconds                              float64
	TotalAlloc, Mallocs, HeapAlloc, HeapSys uint64
	Goroutines                              int
}

func comparisonSnapshot() comparisonResources {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return comparisonResources{float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6, mem.TotalAlloc, mem.Mallocs, mem.HeapAlloc, mem.HeapSys, runtime.NumGoroutine()}
}

type comparisonStats struct {
	Completed, Attempted, Offered, Dropped int
	Seconds                                float64
	P50Ns, P99Ns, ScheduledP99Ns           int64
	Errors                                 map[string]int
	Before, After                          comparisonResources
	StartedAt, FinishedAt                  int64
	Samples, ScheduledSamples              []int64 `json:"-"`
}

// 分位数仅含通过结果校验的请求。精确样本记录属于驱动成本，分配与 CPU 也计入报告。
func runComparison(load comparisonLoad, call func(context.Context, int) error) comparisonStats {
	type sample struct {
		latency, scheduled []int64
		errors             map[string]int
	}
	samples := make([]sample, load.Concurrency)
	for i := range samples {
		samples[i] = sample{make([]int64, 0, 4096), make([]int64, 0, 4096), map[string]int{}}
	}
	type arrival struct {
		index int
		at    time.Time
	}
	jobs := make(chan arrival, load.Concurrency)
	ready := make(chan struct{})
	var start time.Time
	var group sync.WaitGroup
	for worker := range samples {
		group.Go(func() {
			<-ready
			one := func(index int, scheduled time.Time) {
				begin := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := call(ctx, index)
				cancel()
				end := time.Now()
				if err != nil {
					samples[worker].errors[err.Error()]++
					return
				}
				samples[worker].latency = append(samples[worker].latency, end.Sub(begin).Nanoseconds())
				if !scheduled.IsZero() {
					samples[worker].scheduled = append(samples[worker].scheduled, end.Sub(scheduled).Nanoseconds())
				}
			}
			if load.Rate > 0 {
				for job := range jobs {
					one(job.index, job.at)
				}
				return
			}
			for i := worker; (load.Requests > 0 && i < load.Requests) || (load.Requests == 0 && time.Since(start) < load.Duration); i += load.Concurrency {
				one(i, time.Time{})
			}
		})
	}
	if wait := time.Until(time.Unix(0, load.StartAt)); wait > 0 {
		time.Sleep(wait)
	}
	stats := comparisonStats{Errors: map[string]int{}, Before: comparisonSnapshot()}
	start = time.Now()
	stats.StartedAt = start.UnixNano()
	close(ready)
	if load.Rate > 0 {
		stats.Offered = int(load.Duration.Seconds() * float64(load.Rate))
		for i := 0; i < stats.Offered; i++ {
			at := start.Add(time.Duration(float64(time.Second) * float64(i) / float64(load.Rate)))
			if wait := time.Until(at); wait > 0 {
				time.Sleep(wait)
			}
			select {
			case jobs <- arrival{i, at}:
			default:
				stats.Dropped++
			}
		}
		close(jobs)
	}
	group.Wait()
	finish := time.Now()
	stats.Seconds = finish.Sub(start).Seconds()
	stats.FinishedAt = finish.UnixNano()
	stats.After = comparisonSnapshot()
	var all, scheduled []int64
	for _, sample := range samples {
		all = append(all, sample.latency...)
		scheduled = append(scheduled, sample.scheduled...)
		for err, count := range sample.errors {
			stats.Errors[err] += count
			stats.Attempted += count
		}
	}
	stats.Completed = len(all)
	stats.Attempted += stats.Completed
	if load.Rate == 0 {
		stats.Offered = stats.Attempted
	}
	slices.Sort(all)
	slices.Sort(scheduled)
	stats.Samples, stats.ScheduledSamples = all, scheduled
	if len(all) > 0 {
		stats.P50Ns = all[(len(all)*50+99)/100-1]
		stats.P99Ns = all[(len(all)*99+99)/100-1]
	}
	if len(scheduled) > 0 {
		stats.ScheduledP99Ns = scheduled[(len(scheduled)*99+99)/100-1]
	}
	return stats
}

type comparisonEcho struct{}

func (comparisonEcho) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	request, err := txactor.DeliveryFromProto(message)
	if err != nil {
		panic(err)
	}
	result := comparisonReply(request)
	if err = txactor.Respond(ctx, request, result, nil); err != nil {
		panic(err)
	}
}
func comparisonReply(request txactor.Delivery) txactor.Result {
	return txactor.Result{MessageType: "comparison.done", CommandID: request.CommandID + 1, Payload: append([]byte(nil), request.Payload...)}
}

type comparisonPeer struct {
	endpoint string
	call     func(context.Context, int, []byte) error
	close    func() error
}

func newComparisonPeer(config comparisonConfig) (*comparisonPeer, error) {
	if config.Backend == "nats-native" || config.Backend == "nats-envelope" || config.Backend == "nats-reply-unique" || config.Backend == "nats-reply-fixed" {
		return newComparisonLayer(config)
	}
	if config.Backend == "proto" {
		root := lifecycle.NewWaitGroupContext(context.Background())
		var kinds []txactor.Kind
		if config.Mode != "client" {
			kinds = []txactor.Kind{txactor.MustFixedKind("comparison", func() protoactor.Actor { return comparisonEcho{} })}
		}
		r, err := txactor.New(txactor.Config{ClusterName: config.Namespace, Host: "127.0.0.1", LogLevel: txactor.LogLevelError, MailboxCapacity: 1024, MaxPendingCalls: 4096, Etcd: txactor.EtcdConfig{Endpoints: []string{config.Etcd}, Prefix: "/tx/comparison/" + config.Namespace, DialTimeout: time.Second}}, kinds...)
		if err != nil {
			return nil, err
		}
		if err = r.Start(root); err != nil {
			_ = r.Shutdown()
			root.Cancel()
			root.Wait()
			return nil, err
		}
		endpoint, err := r.Endpoint()
		if err != nil {
			return nil, err
		}
		target := config.Target
		if config.Mode != "client" {
			target = endpoint
		}
		pid, err := txactor.FixedKindPID(target, "comparison")
		if err != nil {
			return nil, err
		}
		return &comparisonPeer{endpoint: endpoint, call: func(ctx context.Context, id int, payload []byte) error {
			result, err := r.Call(ctx, pid, txactor.Delivery{MessageType: "comparison.call", CommandID: 47, RouteKey: protocol.Uint64RouteKey(uint64(id + 1)), Payload: payload})
			if err == nil && (result.MessageType != "comparison.done" || result.CommandID != 48 || !bytes.Equal(payload, result.Payload)) {
				err = errors.New("RPC 响应不匹配")
			}
			return err
		}, close: func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.Drain(ctx); err != nil {
				return err
			}
			err := r.Shutdown()
			root.Cancel()
			root.Wait()
			return err
		}}, nil
	}
	if config.Backend != "nats" {
		return nil, errors.New("未知对照后端")
	}
	r := New(1024, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, value any) (any, error) {
			request, err := txactor.DeliveryFromProto(value.(*actorpb.Delivery))
			if err != nil {
				return nil, err
			}
			result := comparisonReply(request)
			return &actorpb.Result{MessageType: result.MessageType, CommandId: result.CommandID, Payload: append([]byte(nil), result.Payload...)}, nil
		}}, nil
	})
	instance := config.Namespace + "-" + config.Mode
	p, err := Connect(config.URL, config.Namespace, instance, r, 4096)
	if err != nil {
		return nil, err
	}
	target := config.Target
	if config.Mode != "client" {
		target = instance
	}
	peer := comparisonTransportPeer(p, target)
	peer.close = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return p.Drain(ctx, nil)
	}
	return peer, nil
}

func comparisonTransportPeer(p *Transport, target string) *comparisonPeer {
	return &comparisonPeer{endpoint: p.instance, call: func(ctx context.Context, id int, payload []byte) error {
		result, err := p.Call(ctx, Target{target, Identity{"comparison", strconv.Itoa(id + 1)}}, &actorpb.Delivery{MessageType: "comparison.call", CommandId: 47, RouteKeyKind: actorpb.RouteKeyKind_ROUTE_KEY_UINT64, RouteKeyNumeric: uint64(id + 1), Payload: payload, RequestId: 1})
		// 对齐正式 Future.Await 返回给调用方的副本隔离；原型内部仍走自身协议。
		if err == nil {
			copied := append([]byte(nil), result.Payload...)
			if result.MessageType != "comparison.done" || result.CommandId != 48 || !bytes.Equal(payload, copied) {
				err = errors.New("RPC 响应不匹配")
			}
		}
		return err
	}}
}

// TestRPCComparisonProcess 通过标准输入接收采样命令；环境未显式设置时不启动任何外部依赖。
func TestRPCComparisonProcess(t *testing.T) {
	raw := os.Getenv("TX_RPC_COMPARISON")
	if raw == "" {
		t.Skip("设置 TX_RPC_COMPARISON 启动独立对照进程")
	}
	var config comparisonConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	// 目标端独立诊断进程从启动采到退出；调用端的 Profile 仍只包围负载窗口。
	if config.Profile != "" {
		file, err := os.Create(config.Profile)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err = pprof.StartCPUProfile(file); err != nil {
			t.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	peer, err := newComparisonPeer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := peer.close(); err != nil {
			t.Error(err)
		}
	}()
	output := json.NewEncoder(os.Stdout)
	emit := func(value any) {
		if err := output.Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	emit(map[string]any{"Ready": peer.endpoint, "Resources": comparisonSnapshot()})
	input := json.NewDecoder(os.Stdin)
	var last comparisonStats
	for {
		var load comparisonLoad
		if err = input.Decode(&load); err == io.EOF {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		switch load.Action {
		case "snapshot":
			emit(comparisonSnapshot())
			continue
		case "gc":
			last = comparisonStats{}
			runtime.GC()
			emit(comparisonSnapshot())
			continue
		case "samples":
			// 聚合器在全部资源快照之后取样，避免 JSON 导出成本混入 RPC 测量。
			emit(map[string]any{"Samples": last.Samples, "ScheduledSamples": last.ScheduledSamples})
			last = comparisonStats{}
			continue
		case "stop":
			emit(map[string]bool{"Stopped": true})
			return
		case "warm", "run":
		default:
			t.Fatal("未知采样命令", load.Action)
		}
		if load.Entities < 1 || load.Payload < 1 || load.Concurrency < 1 || (load.Requests < 1 && load.Duration <= 0) {
			t.Fatal("无效负载", load)
		}
		payload := bytes.Repeat([]byte{0x7a}, load.Payload)
		call := func(ctx context.Context, index int) error { return peer.call(ctx, index%load.Entities, payload) }
		if load.Profile != "" {
			file, err := os.Create(load.Profile)
			if err != nil {
				t.Fatal(err)
			}
			if err = pprof.StartCPUProfile(file); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			last = runComparison(load, call)
			pprof.StopCPUProfile()
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			last = runComparison(load, call)
		}
		emit(last)
	}
}
