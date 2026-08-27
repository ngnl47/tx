// Package checkpoint 提供 WAL 持久化检查点的按流合并调度。
package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/wal"
)

var (
	ErrInvalidRequest = errors.New("wal checkpoint: invalid request")
	ErrNotStarted     = errors.New("wal checkpoint: scheduler is not started")
	ErrAlreadyStarted = errors.New("wal checkpoint: scheduler is already started")
)

const (
	defaultWorkers         = 1
	defaultRetryInitial    = 100 * time.Millisecond
	defaultRetryMultiplier = 2
	defaultRetryMax        = time.Minute
)

// Request 只描述一条 WAL stream 需要推进到的最大序号，不携带业务正文。
type Request struct {
	// Stream 指定需要推进持久化检查点的 WAL 状态流。
	Stream wal.Stream
	// ThroughSeq 是本轮 checkpoint 成功后保证已经覆盖的最大连续序号。
	ThroughSeq uint64
}

// Validate 校验调度身份和目标位置。
func (request Request) Validate() error {
	if err := request.Stream.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if request.ThroughSeq == 0 || request.ThroughSeq > math.MaxInt64 {
		return fmt.Errorf("%w: through sequence must be between 1 and %d", ErrInvalidRequest, int64(math.MaxInt64))
	}
	return nil
}

// Handler 消费一条已经按 stream 收敛的持久化检查点请求。
// 实现负责读取 WAL、更新业务 checkpoint，并通过 wal.Store.Checkpoint 清理已覆盖日志。
type Handler interface {
	Checkpoint(context.Context, Request) error
}

// HandlerFunc 允许使用函数实现 Handler。
type HandlerFunc func(context.Context, Request) error

// Checkpoint 实现 Handler。
func (handler HandlerFunc) Checkpoint(ctx context.Context, request Request) error {
	return handler(ctx, request)
}

// Config 描述同流串行、跨流并行和失败退避策略。
type Config struct {
	// Workers 是可并行处理不同 WAL stream 的 worker 数；同一 stream 始终串行。
	Workers int
	// RetryInitial 是 checkpoint 首次失败后的重试等待时间。
	RetryInitial time.Duration
	// RetryMultiplier 是连续失败时的指数退避倍率。
	RetryMultiplier float64
	// RetryMax 是单次重试等待的上限；达到上限后仍继续重试。
	RetryMax time.Duration
}

type streamID struct {
	namespace  string
	streamKey  string
	generation uint64
}

type streamState struct {
	request Request
	running bool
	attempt int
	retryAt time.Time
}

type job struct {
	id      streamID
	request Request
}

type result struct {
	job job
	err error
}

// Scheduler 只保存每条 stream 的最大 ThroughSeq。它不会复制 WAL 正文。
type Scheduler struct {
	handler Handler
	config  Config

	mu      sync.Mutex
	states  map[streamID]*streamState
	wake    chan struct{}
	idle    chan struct{}
	started bool
	stopped bool
}

// New 创建 WAL 持久化检查点调度器。
func New(handler Handler, config Config) (*Scheduler, error) {
	if isNil(handler) {
		return nil, errors.New("wal checkpoint: handler is not configured")
	}
	config = effectiveConfig(config)
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	idle := make(chan struct{})
	close(idle)
	return &Scheduler{
		handler: handler,
		config:  config,
		states:  make(map[streamID]*streamState),
		wake:    make(chan struct{}, 1),
		idle:    idle,
	}, nil
}

// Start 把调度器纳入进程根生命周期。一个实例只能启动一次。
func (scheduler *Scheduler) Start(tasks lifecycle.Tasks) error {
	if scheduler == nil || tasks == nil {
		return ErrNotStarted
	}
	scheduler.mu.Lock()
	if scheduler.started {
		scheduler.mu.Unlock()
		return ErrAlreadyStarted
	}
	scheduler.started = true
	scheduler.stopped = false
	scheduler.mu.Unlock()
	if err := tasks.Go(scheduler.run); err != nil {
		scheduler.mu.Lock()
		scheduler.started = false
		scheduler.stopped = true
		scheduler.mu.Unlock()
		return err
	}
	return nil
}

// Submit 合并一条检查点请求。同一 stream 只保留最大的 ThroughSeq。
func (scheduler *Scheduler) Submit(request Request) error {
	if scheduler == nil {
		return ErrNotStarted
	}
	if err := request.Validate(); err != nil {
		return err
	}
	id := identify(request.Stream)
	scheduler.mu.Lock()
	if !scheduler.started || scheduler.stopped {
		scheduler.mu.Unlock()
		return ErrNotStarted
	}
	state := scheduler.states[id]
	if state == nil {
		if len(scheduler.states) == 0 {
			scheduler.idle = make(chan struct{})
		}
		scheduler.states[id] = &streamState{request: request}
	} else if request.ThroughSeq > state.request.ThroughSeq {
		state.request.ThroughSeq = request.ThroughSeq
	}
	scheduler.mu.Unlock()
	scheduler.signal()
	return nil
}

// Pending 返回尚未完成持久化检查点的 stream 数量。
func (scheduler *Scheduler) Pending() int {
	if scheduler == nil {
		return 0
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	return len(scheduler.states)
}

// WaitIdle 等待当前已提交的全部 stream 完成持久化检查点。
func (scheduler *Scheduler) WaitIdle(ctx context.Context) error {
	if scheduler == nil {
		return ErrNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		scheduler.mu.Lock()
		if len(scheduler.states) == 0 {
			scheduler.mu.Unlock()
			return nil
		}
		idle := scheduler.idle
		scheduler.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-idle:
		}
	}
}

func (scheduler *Scheduler) run(ctx context.Context) {
	jobs := make(chan job)
	results := make(chan result, scheduler.config.Workers)
	var workers sync.WaitGroup
	for index := 0; index < scheduler.config.Workers; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case current, ok := <-jobs:
					if !ok {
						return
					}
					err := scheduler.handler.Checkpoint(ctx, current.request)
					select {
					case results <- result{job: current, err: err}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	defer func() {
		close(jobs)
		workers.Wait()
		scheduler.mu.Lock()
		scheduler.stopped = true
		scheduler.mu.Unlock()
	}()

	active := 0
	for {
		for active < scheduler.config.Workers {
			current, ok := scheduler.takeReady(time.Now())
			if !ok {
				break
			}
			select {
			case jobs <- current:
				active++
			case <-ctx.Done():
				return
			}
		}

		retry, hasRetry := scheduler.nextRetry(time.Now())
		var timer *time.Timer
		var timerC <-chan time.Time
		if hasRetry {
			if retry < 0 {
				retry = 0
			}
			timer = time.NewTimer(retry)
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-scheduler.wake:
		case completed := <-results:
			active--
			scheduler.complete(completed, time.Now())
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (scheduler *Scheduler) takeReady(now time.Time) (job, bool) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	for id, state := range scheduler.states {
		if state.running || (!state.retryAt.IsZero() && state.retryAt.After(now)) {
			continue
		}
		state.running = true
		return job{id: id, request: state.request}, true
	}
	return job{}, false
}

func (scheduler *Scheduler) nextRetry(now time.Time) (time.Duration, bool) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	var earliest time.Time
	for _, state := range scheduler.states {
		if state.running || state.retryAt.IsZero() {
			continue
		}
		if earliest.IsZero() || state.retryAt.Before(earliest) {
			earliest = state.retryAt
		}
	}
	if earliest.IsZero() {
		return 0, false
	}
	return earliest.Sub(now), true
}

func (scheduler *Scheduler) complete(completed result, now time.Time) {
	scheduler.mu.Lock()
	state := scheduler.states[completed.job.id]
	if state == nil {
		scheduler.mu.Unlock()
		return
	}
	state.running = false
	if completed.err == nil {
		state.attempt = 0
		state.retryAt = time.Time{}
		if state.request.ThroughSeq <= completed.job.request.ThroughSeq {
			delete(scheduler.states, completed.job.id)
			if len(scheduler.states) == 0 {
				close(scheduler.idle)
			}
		}
		scheduler.mu.Unlock()
		scheduler.signal()
		return
	}
	state.attempt++
	state.retryAt = now.Add(scheduler.retryDelay(state.attempt))
	request := state.request
	attempt := state.attempt
	retryAt := state.retryAt
	scheduler.mu.Unlock()
	txlog.Error("WAL checkpoint failed, retry scheduled",
		"namespace", request.Stream.Namespace,
		"streamKey", request.Stream.StreamKey,
		"generation", request.Stream.Generation,
		"throughSeq", request.ThroughSeq,
		"attempt", attempt,
		"retryAt", retryAt,
		"err", completed.err,
	)
	scheduler.signal()
}

func (scheduler *Scheduler) retryDelay(attempt int) time.Duration {
	delay := scheduler.config.RetryInitial
	if attempt <= 1 {
		return delay
	}
	multiplier := math.Pow(scheduler.config.RetryMultiplier, float64(attempt-1))
	if multiplier >= float64(scheduler.config.RetryMax)/float64(delay) {
		return scheduler.config.RetryMax
	}
	return time.Duration(float64(delay) * multiplier)
}

func (scheduler *Scheduler) signal() {
	select {
	case scheduler.wake <- struct{}{}:
	default:
	}
}

func identify(stream wal.Stream) streamID {
	return streamID{namespace: stream.Namespace, streamKey: stream.StreamKey, generation: stream.Generation}
}

func effectiveConfig(config Config) Config {
	if config.Workers == 0 {
		config.Workers = defaultWorkers
	}
	if config.RetryInitial == 0 {
		config.RetryInitial = defaultRetryInitial
	}
	if config.RetryMultiplier == 0 {
		config.RetryMultiplier = defaultRetryMultiplier
	}
	if config.RetryMax == 0 {
		config.RetryMax = defaultRetryMax
	}
	return config
}

func validateConfig(config Config) error {
	if config.Workers <= 0 {
		return errors.New("wal checkpoint: workers must be greater than 0")
	}
	if config.RetryInitial <= 0 || config.RetryMax <= 0 {
		return errors.New("wal checkpoint: retry durations must be greater than 0")
	}
	if config.RetryMultiplier <= 1 {
		return errors.New("wal checkpoint: retry multiplier must be greater than 1")
	}
	if config.RetryMax < config.RetryInitial {
		return errors.New("wal checkpoint: retry max must not be less than retry initial")
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
