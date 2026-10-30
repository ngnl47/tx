package txn

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// BenchmarkSubmitBurst 固定发起短时突发，不等待前一个任务完成，也不重投拒绝项。
// 推荐 -benchtime=1x -count=3；一次操作是一秒发压窗口及其尾部排空。
func BenchmarkSubmitBurst(b *testing.B) {
	for _, workload := range []string{"cpu", "io", "hotlock"} {
		for _, concurrent := range []int{16, 64, 256} {
			for _, pending := range []int{256, 1024, 4096} {
				b.Run(fmt.Sprintf("%s/C%d/P%d", workload, concurrent, pending), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						result := runSubmitBurst(b, workload, concurrent, pending)
						data, err := json.Marshal(result)
						if err != nil {
							b.Fatal(err)
						}
						b.Logf("RESULT %s", data)
					}
				})
			}
		}
	}
}

type submitBurstProcedure struct {
	workload                                        string
	locks                                           Lockeys
	planned, submitted, entered, acquired, finished time.Time
	future                                          *Future
	err                                             error
	checksum                                        [32]byte
}

func (p *submitBurstProcedure) Locks(context.Context) (Lockeys, error) {
	p.entered = time.Now()
	return p.locks, nil
}

func (p *submitBurstProcedure) Process(tx *Tx) error {
	p.acquired = time.Now()
	tx.OnFinish(func() { p.finished = time.Now() })
	if p.workload == "cpu" {
		for i := 0; i < 128; i++ {
			p.checksum = sha256.Sum256(p.checksum[:])
		}
		return nil
	}
	// 模拟持锁期间的外部等待；不代表 Redis/Mongo 的实际耗时。
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-tx.Context().Done():
		return tx.Context().Err()
	}
}

type burstLatency struct {
	P50 float64 `json:"p50_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

type submitBurstResult struct {
	Workload           string       `json:"workload"`
	Concurrent         int          `json:"concurrent"`
	Pending            int          `json:"pending"`
	Attempts           int          `json:"attempts"`
	Accepted           int          `json:"accepted"`
	Rejected           int          `json:"rejected"`
	Completed          int          `json:"completed"`
	Failed             int          `json:"failed"`
	InjectionSeconds   float64      `json:"injection_seconds"`
	TotalSeconds       float64      `json:"total_seconds"`
	CompletedPerSecond float64      `json:"completed_per_second"`
	GeneratorLag       burstLatency `json:"generator_lag"`
	SubmitToEntry      burstLatency `json:"submit_to_entry"`
	LockWait           burstLatency `json:"lock_wait"`
	TransactionFinish  burstLatency `json:"transaction_finish"`
}

func runSubmitBurst(b *testing.B, workload string, concurrent, pending int) submitBurstResult {
	b.Helper()
	const pulses, batch, producers = 20, 512, 8
	const interval = 50 * time.Millisecond
	runtime := NewRuntime()
	if err := runtime.ConfigureAsync(AsyncConfig{MaxConcurrent: concurrent, MaxPending: pending, Timeout: 10 * time.Second}); err != nil {
		b.Fatal(err)
	}
	requests := make([]submitBurstProcedure, pulses*batch)
	for i := range requests {
		key := i + 1
		if workload == "hotlock" && i%5 == 0 {
			key = 0
		}
		requests[i].workload = workload
		requests[i].locks = Lockeys{NewLockey("submit-burst", 1, key)}
	}
	jobs := make(chan *submitBurstProcedure, batch)
	var workers sync.WaitGroup
	for i := 0; i < producers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for p := range jobs {
				p.submitted = time.Now()
				p.future, p.err = runtime.Submit(context.Background(), p)
			}
		}()
	}
	start := time.Now()
	for pulse := 0; pulse < pulses; pulse++ {
		planned := start.Add(time.Duration(pulse) * interval)
		time.Sleep(time.Until(planned))
		for i := pulse * batch; i < (pulse+1)*batch; i++ {
			requests[i].planned = planned
			jobs <- &requests[i]
		}
	}
	close(jobs)
	workers.Wait()
	// 末次突发位于 950ms，完整注入窗口至少为一秒；发生器超时仍如实计时。
	time.Sleep(time.Until(start.Add(pulses * interval)))
	injectionEnd := time.Now()
	result := submitBurstResult{Workload: workload, Concurrent: concurrent, Pending: pending, Attempts: len(requests), InjectionSeconds: injectionEnd.Sub(start).Seconds()}
	for i := range requests {
		p := &requests[i]
		if errors.Is(p.err, ErrAsyncFull) {
			result.Rejected++
			continue
		}
		if p.err != nil {
			b.Fatal(p.err)
		}
		result.Accepted++
		if err := p.future.Await(context.Background()); err != nil {
			result.Failed++
		} else {
			result.Completed++
		}
	}
	if err := runtime.Drain(context.Background()); err != nil {
		b.Fatal(err)
	}
	result.TotalSeconds = time.Since(start).Seconds()
	result.CompletedPerSecond = float64(result.Completed) / result.TotalSeconds
	var lag, entry, lock, finish []time.Duration
	for i := range requests {
		p := &requests[i]
		lag = append(lag, p.submitted.Sub(p.planned))
		if !p.entered.IsZero() {
			entry = append(entry, p.entered.Sub(p.submitted))
		}
		if !p.acquired.IsZero() {
			lock = append(lock, p.acquired.Sub(p.entered))
		}
		if !p.finished.IsZero() {
			finish = append(finish, p.finished.Sub(p.submitted))
		}
	}
	result.GeneratorLag = summarizeBurstLatency(lag)
	result.SubmitToEntry = summarizeBurstLatency(entry)
	result.LockWait = summarizeBurstLatency(lock)
	result.TransactionFinish = summarizeBurstLatency(finish)
	return result
}

func summarizeBurstLatency(values []time.Duration) burstLatency {
	if len(values) == 0 {
		return burstLatency{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	milliseconds := func(index int) float64 { return float64(values[index]) / float64(time.Millisecond) }
	return burstLatency{P50: milliseconds((len(values) - 1) / 2), P99: milliseconds((len(values)*99+99)/100 - 1), Max: milliseconds(len(values) - 1)}
}
