package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/event"
)

// IOStat 是进程存储运行时的累计驱动统计，不包含端点、key、命令参数或业务身份。
// Calls 是完成调用数；Commands 对 Redis pipeline 为批内命令数，其他调用为 1。
// TotalDuration/MaxDuration 对 pipeline 记录整批耗时，不摊算单条命令延迟。
type IOStat struct {
	Backend       string        `json:"backend"`
	Operation     string        `json:"operation"`
	Calls         uint64        `json:"calls"`
	Commands      uint64        `json:"commands"`
	Errors        uint64        `json:"errors"`
	TotalDuration time.Duration `json:"totalDurationNs"`
	MaxDuration   time.Duration `json:"maxDurationNs"`
}

// 类别集合固定，未知命令汇入 other，防止参数或扩展命令产生无界标签。
var ioOperations = [...]string{"ping", "get", "mget", "read", "write", "eval", "evalsha", "pipeline", "other", "ping", "find", "getMore", "insert", "update", "delete", "findAndModify", "other"}

type ioStats struct {
	mu     sync.Mutex
	values [len(ioOperations)]IOStat
}

// IOStats 返回已完成调用的累计快照；阶段调用数、命令数和累计耗时可用两次快照作差。
// 最大耗时是运行期累计最大值，不能通过相减获得阶段最大值。
func (s *Runtime) IOStats() []IOStat {
	s.io.mu.Lock()
	defer s.io.mu.Unlock()
	var result []IOStat
	for _, value := range s.io.values {
		if value.Calls > 0 {
			result = append(result, value)
		}
	}
	return result
}

func (s *ioStats) record(index int, commands int, elapsed time.Duration, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value := &s.values[index]
	value.Backend = "redis"
	if index >= 9 {
		value.Backend = "mongo"
	}
	value.Operation = ioOperations[index]
	value.Calls++
	value.Commands += uint64(commands)
	if failed {
		value.Errors++
	}
	value.TotalDuration += elapsed
	value.MaxDuration = max(value.MaxDuration, elapsed)
}

func redisOperation(name string) int {
	switch name {
	case "ping":
		return 0
	case "get":
		return 1
	case "mget":
		return 2
	case "hget", "hmget", "hgetall", "exists", "ttl", "pttl", "zrange", "zrangebyscore":
		return 3
	case "set", "mset", "hset", "del", "unlink", "expire", "pexpire", "incr", "incrby", "zadd", "zrem":
		return 4
	case "eval":
		return 5
	case "evalsha":
		return 6
	default:
		return 8
	}
}

type redisIOHook struct{ stats *ioStats }

func (s *ioStats) redisHook() redis.Hook                          { return redisIOHook{stats: s} }
func (h redisIOHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h redisIOHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		started := time.Now()
		err := next(ctx, cmd)
		h.stats.record(redisOperation(cmd.Name()), 1, time.Since(started), err != nil && !errors.Is(err, redis.Nil))
		return err
	}
}
func (h redisIOHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		started := time.Now()
		err := next(ctx, cmds)
		h.stats.record(7, len(cmds), time.Since(started), err != nil && !errors.Is(err, redis.Nil))
		return err
	}
}

func (s *ioStats) mongoMonitor() *event.CommandMonitor {
	record := func(value event.CommandFinishedEvent, failed bool) {
		index := len(ioOperations) - 1
		for i := 9; i < len(ioOperations)-1; i++ {
			if value.CommandName == ioOperations[i] {
				index = i
				break
			}
		}
		s.record(index, 1, value.Duration, failed)
	}
	return &event.CommandMonitor{
		Succeeded: func(_ context.Context, value *event.CommandSucceededEvent) { record(value.CommandFinishedEvent, false) },
		Failed:    func(_ context.Context, value *event.CommandFailedEvent) { record(value.CommandFinishedEvent, true) },
	}
}
