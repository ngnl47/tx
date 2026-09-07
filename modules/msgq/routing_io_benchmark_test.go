package msgq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

// BenchmarkRoutingRedisIO 对比两种现行寻址边界；注入延迟不是 Redis 自身耗时。
func BenchmarkRoutingRedisIO(b *testing.B) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		b.Skip("set TX_REDIS_ENDPOINT for real Redis verification")
	}
	for _, delay := range []time.Duration{0, 5 * time.Millisecond} {
		b.Run(delay.String(), func(b *testing.B) {
			ctx := context.Background()
			raw := redis.NewClient(&redis.Options{Addr: endpoint})
			defer raw.Close()
			hook := &delayedRedisCounter{delay: delay}
			raw.AddHook(hook)
			prefix := fmt.Sprintf("tx:test:routing-io:%d", time.Now().UnixNano())
			defer raw.Del(ctx, prefix+":role:{47}", prefix+":session:v2:{11}")
			store, err := NewRedisSessionStore(goredis.NewClient(raw), SessionRedisOptions{Prefix: prefix, TTL: time.Hour})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := store.ClaimRole(ctx, RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "first"}); err != nil {
				b.Fatal(err)
			}
			if _, err := store.Create(ctx, SessionBinding{RoleID: 47, SessionID: 11, ConnectionID: 1, InstanceID: "msgq-a"}); err != nil {
				b.Fatal(err)
			}
			directory := discovery.NewServiceDirectory()
			if err := directory.Replace([]discovery.Instance{{ID: "msgq-a", Endpoint: "localhost:9001", Services: []string{"msgq"}, Status: discovery.StatusRunning}}); err != nil {
				b.Fatal(err)
			}
			roles, err := NewRoleRouter(directory, store.FindRole, 128)
			if err != nil {
				b.Fatal(err)
			}
			sessions, err := NewSessionRouter(directory, store)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := roles.Resolve(ctx, 47); err != nil {
				b.Fatal(err)
			}
			for _, name := range []string{"role-warm", "session"} {
				b.Run(name, func(b *testing.B) {
					before := hook.count.Load()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if name == "role-warm" {
							_, err = roles.Resolve(ctx, 47)
						} else {
							_, err = sessions.ResolveSession(ctx, 11)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(hook.count.Load()-before)/float64(b.N), "redis-cmd/op")
				})
			}
		})
	}
}

type delayedRedisCounter struct {
	redisCommandCounter
	delay time.Duration
}

func (c *delayedRedisCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	counted := c.redisCommandCounter.ProcessHook(next)
	return func(ctx context.Context, cmd redis.Cmder) error {
		if c.delay > 0 {
			timer := time.NewTimer(c.delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
		return counted(ctx, cmd)
	}
}
