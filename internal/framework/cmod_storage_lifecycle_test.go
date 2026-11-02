package framework

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/component"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	storageconfig "github.com/ngnl47/tx/storage/config"
	redis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestBuiltInStorageCModsWaitForFusionWorkerBeforeRollbackConnections(t *testing.T) {
	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
	redisURI := newStorageRedisFixture(t, workerEntered, releaseWorker)
	mongoURI := newStorageMongoFixture(t)
	root := lifecycle.NewWaitGroupContext(context.Background())
	config := Config{
		Storage: StorageConfig{
			Redis: storageconfig.NewRedisConfig(map[string]string{"fusion": redisURI}),
			MongoDB: storageconfig.NewRouteTableConfig(map[string]storageconfig.RouteTarget{
				"fusion": {Rules: []storageconfig.RouteRule{{Shards: []string{mongoURI}}}},
			}),
		},
		Runtime: RuntimeConfig{
			Lifecycle: LifecycleConfig{DrainTimeout: 100 * time.Millisecond},
			DelayTask: DelayTaskConfig{Prefix: "rollback", Buckets: 1},
			Health:    HealthConfig{Interval: time.Hour, Timeout: time.Second},
		},
	}
	types := meta.NewRegistry()
	if err := types.RegisterType(meta.TypeSpec{
		Type: meta.Type{
			Ref:    meta.TypeRef{FullName: "RollbackRecord"},
			Fields: []meta.Field{{Name: "ID", Type: "int64", BSONName: "id", JSONName: "id", Exported: true}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeFusion},
		Redis:   &meta.RedisSpec{Key: "rollback", IDField: "ID", Route: meta.RouteSpec{Type: "fusion", By: "ID"}},
		Mongo:   &meta.MongoSpec{Collection: "records", IDField: "ID", Route: meta.RouteSpec{Type: "fusion", By: "ID"}},
	}); err != nil {
		t.Fatal(err)
	}
	server := &Server{config: config, types: types, runtime: &runtime{config: config, lifecycle: root}}
	failure := errors.New("后续模组启动失败")
	var redisClient redis.Cmdable
	var mongoClient *mongo.Client
	host := newCModHost(t.Context(), newCModContext(server), cmodGraph{ordered: []CMod{
		server.redisCMod(), server.mongoCMod(), server.fusionCMod(),
		&builtInCMod{spec: CModSpec{ID: "failure", Requires: []CModID{CModFusionStorage}}, start: func(CModContext) error {
			select {
			case <-workerEntered:
			case <-time.After(time.Second):
				return errors.New("Fusion 未领取既有 Redis 回写任务")
			}
			var err error
			redisClient, err = server.runtime.storage.RedisClient("fusion")
			if err != nil {
				return err
			}
			database, err := server.runtime.storage.MongoDatabase(t.Context(), "fusion", int64(7))
			if err != nil {
				return err
			}
			mongoClient = database.Client()
			return failure
		}},
	}})
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = host.stop(ctx)
		if err := root.CancelAndWait(ctx); err != nil {
			t.Errorf("清理后台任务: %v", err)
		}
	})

	err := host.start()
	if !errors.Is(err, failure) {
		t.Fatalf("启动错误 = %v，期望后续模组失败", err)
	}
	if !errors.Is(err, component.ErrStopDeferred) {
		t.Fatalf("回滚未等待仍在执行的 Fusion worker: %v", err)
	}
	if root.Err() != nil {
		t.Fatal("局部回滚取消了进程根生命周期")
	}
	if err := redisClient.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("worker 退出前关闭了 Redis: %v", err)
	}
	if err := mongoClient.Ping(t.Context(), nil); err != nil {
		t.Fatalf("worker 退出前关闭了 Mongo: %v", err)
	}

	release()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := host.stop(ctx); err != nil {
		t.Fatalf("worker 退出后重试回滚: %v", err)
	}
	if err := redisClient.Ping(ctx).Err(); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("回滚后 Redis 错误 = %v，期望连接已关闭", err)
	}
	if err := mongoClient.Ping(ctx, nil); !errors.Is(err, mongo.ErrClientDisconnected) {
		t.Fatalf("回滚后 Mongo 错误 = %v，期望连接已关闭", err)
	}
	// 根节点仍存活；全部内建模组停止后，发布器与健康探测也必须已经退出。
	tasksExited := make(chan struct{})
	go func() { root.Wait(); close(tasksExited) }()
	select {
	case <-tasksExited:
	case <-ctx.Done():
		t.Fatal("内建存储任务仍挂在进程根生命周期，未随所属 CMod 退出")
	}
}

// 使用真实 Redis 客户端、任务解码和 Fusion handler，只替换数据库 I/O。
func newStorageRedisFixture(t *testing.T, workerEntered chan<- struct{}, releaseWorker <-chan struct{}) string {
	t.Helper()
	lease, err := json.Marshal([]map[string]any{{
		"body":  `{"type":"RollbackRecord","id":{"kind":"int64","value":"7"},"action":"save","dueAt":"2020-01-01T00:00:00Z"}`,
		"token": "existing-lease", "attempts": 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var claimed atomic.Bool
	var entered sync.Once
	address := newStorageTCPFixture(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		for {
			command, err := readStorageRedisCommand(reader)
			if err != nil {
				return
			}
			response := "+OK\r\n"
			switch strings.ToLower(command[0]) {
			case "hello":
				response = "-ERR unknown command 'hello'\r\n"
			case "ping":
				response = "+PONG\r\n"
			case "client":
			case "eval":
				if len(command) > 3 && command[3] == "rollback:{7}@lock" {
					entered.Do(func() { close(workerEntered) })
					<-releaseWorker
					response = "-ERR fixture write interrupted\r\n"
				} else if len(command) > 3 && command[3] == "rollback:{0}:due" && claimed.CompareAndSwap(false, true) {
					response = fmt.Sprintf("$%d\r\n%s\r\n", len(lease), lease)
				} else {
					response = "$2\r\n[]\r\n"
				}
			default:
				response = "-ERR unsupported fixture command\r\n"
			}
			if _, err := io.WriteString(conn, response); err != nil {
				return
			}
		}
	})
	return "redis://" + address + "?read_timeout=5s"
}

func readStorageRedisCommand(reader *bufio.Reader) ([]string, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	var count int
	if _, err := fmt.Sscanf(header, "*%d", &count); err != nil || count <= 0 {
		return nil, fmt.Errorf("invalid Redis fixture command: %q", header)
	}
	command := make([]string, count)
	for i := range command {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		var size int
		if _, err := fmt.Sscanf(header, "$%d", &size); err != nil || size < 0 {
			return nil, fmt.Errorf("invalid Redis fixture argument: %q", header)
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		command[i] = string(data[:size])
	}
	return command, nil
}
