package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	storageconfig "github.com/ngnl47/tx/storage/config"
	redis "github.com/redis/go-redis/v9"
)

// 验收业务原生命令、共享连接及运行时关闭的公共契约，只清理本次创建的 Key。
func TestRedisClientNativeCommands(t *testing.T) {
	uri := os.Getenv("TX_REDIS_URI")
	if uri == "" {
		t.Skip("设置 TX_REDIS_URI 后执行真实 Redis 命令验收")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	runtime := New(ctx)
	if err := runtime.StartRedis(ctx, storageconfig.NewRedisConfig(map[string]string{"main": uri, "rank": uri})); err != nil {
		t.Fatal(err)
	}
	defer runtime.CloseRedis()
	client, err := runtime.RedisClient("rank")
	if err != nil {
		t.Fatal(err)
	}
	main, err := runtime.RedisClient("main")
	if err != nil || main != client {
		t.Fatalf("同 URI 应复用客户端: %v", err)
	}
	if _, err := runtime.RedisClient("missing"); err == nil {
		t.Fatal("未知名称应返回错误")
	}
	key := fmt.Sprintf("tx-native-client:%d", time.Now().UnixNano())
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// 正常路径在关闭前清理；失败路径也不留下测试数据。
		if err := client.Del(cleanup, key).Err(); err != nil && !errors.Is(err, redis.ErrClosed) {
			t.Error(err)
		}
	}()
	if err := client.HGet(ctx, key, "name").Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("redis.Nil 未保留: %v", err)
	}
	var name *redis.StringCmd
	_, err = client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key, "name", "玩家")
		name = pipe.HGet(ctx, key, "name")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if value, err := name.Result(); err != nil || value != "玩家" {
		t.Fatalf("pipeline: %q, %v", value, err)
	}
	result, err := client.Eval(ctx, `return {1, "ok"}`, nil).Result()
	if err != nil || !reflect.DeepEqual(result, []interface{}{int64(1), "ok"}) {
		t.Fatalf("Lua 数组: %#v, %v", result, err)
	}
	if err := client.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.CloseRedis(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(ctx).Err(); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("借用接口应随运行时关闭: %v", err)
	}
}
