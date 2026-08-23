package server

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txstorage "github.com/ngnl47/tx/storage"
	storageconfig "github.com/ngnl47/tx/storage/config"
	txfusion "github.com/ngnl47/tx/storage/fusion"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txmongodriver "github.com/ngnl47/tx/storage/mongo/mongodriver"
	txredis "github.com/ngnl47/tx/storage/redis"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	txtaskredis "github.com/ngnl47/tx/storage/task/redis"
	goredis "github.com/redis/go-redis/v9"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultDelayTaskRouteType = "fusion"

var ErrStorageUnavailable = errors.New("server: storage unavailable")

type storageOptions struct {
	DelayTaskRouteType       string
	DelayTaskPrefix          string
	DelayTaskBuckets         int
	DelayTaskLease           time.Duration
	SchedulerBatchSize       int
	SchedulerPoll            time.Duration
	SchedulerRetryInitial    time.Duration
	SchedulerRetryMultiplier float64
	SchedulerRetryMax        time.Duration
	SchedulerRetryJitter     float64
	HealthProbeInterval      time.Duration
	HealthProbeTimeout       time.Duration
}

type Storage struct {
	ctx          context.Context
	redis        *txredis.Cluster
	mongo        *txmongo.Cluster
	fusion       txfusion.Strategy
	delayBackend *txtaskredis.Backend
	delayQueue   *txfusion.TaskQueue
	delayHandler txfusion.DelayTaskHandler
	delayWorker  *txfusion.Scheduler

	workerOnce     sync.Once
	workerStartErr error
	closeOnce      sync.Once
	closeErr       error
	redisClients   []*goredis.Client
	mongoClients   []*mongodriver.Client
	healthMu       sync.RWMutex
	redisHealthErr error
	mongoHealthErr error
	redisHealthGen uint64
	mongoHealthGen uint64
}

func newStorage(ctx context.Context, cfg StorageConfig, opts storageOptions, types txfusion.TypeRegistry) (*Storage, error) {
	if types == nil {
		return nil, fmt.Errorf("server: generated type registry is required")
	}
	redisCluster, redisClients, redisShards, err := buildRedisCluster(ctx, cfg.Redis)
	if err != nil {
		return nil, err
	}
	mongoCluster, mongoClients, err := buildMongoCluster(ctx, cfg.MongoDB)
	if err != nil {
		closeRedisClients(redisClients)
		return nil, err
	}
	delayBackend, delayQueue, err := buildDelayQueue(ctx, cfg.Redis, redisShards, opts)
	if err != nil {
		closeRedisClients(redisClients)
		disconnectMongoClients(ctx, mongoClients)
		return nil, err
	}

	store := &Storage{
		ctx:          ctx,
		redis:        redisCluster,
		mongo:        mongoCluster,
		delayBackend: delayBackend,
		delayQueue:   delayQueue,
		redisClients: redisClients,
		mongoClients: mongoClients,
	}
	store.fusion = txfusion.Strategy{
		RedisApplier:  redisCluster,
		RedisLoader:   redisCluster,
		RedisDeleter:  redisCluster,
		RedisReplacer: redisCluster,
		RedisLocker:   redisCluster,
		MongoLoader:   mongoCluster,
		MongoDeleter:  mongoCluster,
		MongoQueue:    delayQueue,
	}
	if err := store.fusion.Validate(); err != nil {
		closeRedisClients(redisClients)
		disconnectMongoClients(ctx, mongoClients)
		return nil, fmt.Errorf("server: validate fusion storage: %w", err)
	}
	delayHandler := txfusion.NewDelayTaskHandlerWithOptions(types, redisCluster, mongoCluster, txfusion.DelayTaskHandlerOptions{
		RedisRevisionReader: redisCluster,
		RedisRevisionDelete: redisCluster,
		RedisLocker:         redisCluster,
		DelayQueue:          delayQueue,
	})
	store.delayHandler = healthAwareDelayHandler{storage: store, next: delayHandler}
	store.delayWorker = txfusion.NewSchedulerWithOptions(delayBackend, txfusion.SchedulerOptions{
		BatchSize:         opts.SchedulerBatchSize,
		PollInterval:      opts.SchedulerPoll,
		RetryInitialDelay: opts.SchedulerRetryInitial,
		RetryMultiplier:   opts.SchedulerRetryMultiplier,
		RetryMaxDelay:     opts.SchedulerRetryMax,
		RetryJitter:       opts.SchedulerRetryJitter,
		Handler:           store.delayHandler,
	})
	return store, nil
}

func (s *Storage) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *Storage) close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		closeRedisClients(s.redisClients)
		s.closeErr = disconnectMongoClients(ctx, s.mongoClients)
	})
	return s.closeErr
}

func (s *Storage) RedisReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeRedis, s.redis)
}

func (s *Storage) MongoReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeMongo, s.mongo)
}

func (s *Storage) FusionReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeFusion, s.fusion)
}

// RedisScriptClientFor 返回 routeType/shardKey 对应的既有 Redis 客户端。
// 仅限会话绑定等项目基础设施复用；GD 数据读写仍通过 Storage 的类型化入口完成。
func (s *Storage) RedisScriptClientFor(ctx context.Context, routeType string, shardKey any) (txredis.ScriptClient, error) {
	if s == nil || s.redis == nil {
		return nil, errStorageNotConfigured
	}
	return s.redis.ScriptClientFor(ctx, routeType, shardKey)
}

// RequireWritable 确认 Fusion 的 Redis 与 Mongo 依赖均可接受新的业务写入。
// 它只读取后台健康探针维护的状态，不会在业务热路径额外执行 Ping。
func (s *Storage) RequireWritable() error {
	return s.requireStorageMode(meta.StorageModeFusion)
}

// probe 检查当前项目装配的全部 Redis 与 Mongo 端点。只有全部恢复后才重新开放
// 新业务写入，避免 Fusion 在单边依赖异常时继续接受状态事务。
func (s *Storage) probe(ctx context.Context) error {
	if s == nil {
		return errStorageNotConfigured
	}
	redisGeneration := s.backendHealthGeneration(txstorage.BackendRedis)
	var redisErr error
	if len(s.redisClients) == 0 {
		redisErr = errors.New("server: redis clients are not configured")
	} else {
		for _, client := range s.redisClients {
			if err := client.Ping(ctx).Err(); err != nil {
				redisErr = fmt.Errorf("server: ping redis: %w", err)
				break
			}
		}
	}
	if redisErr != nil {
		if errors.Is(redisErr, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}
		s.markBackendUnavailable(txstorage.BackendRedis, redisErr)
	} else {
		s.recoverBackendHealth(txstorage.BackendRedis, redisGeneration)
	}

	mongoGeneration := s.backendHealthGeneration(txstorage.BackendMongo)
	var mongoErr error
	if len(s.mongoClients) == 0 {
		mongoErr = errors.New("server: mongo clients are not configured")
	} else {
		for _, client := range s.mongoClients {
			if err := client.Ping(ctx, nil); err != nil {
				mongoErr = fmt.Errorf("server: ping mongo: %w", err)
				break
			}
		}
	}
	if mongoErr != nil {
		if errors.Is(mongoErr, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}
		s.markBackendUnavailable(txstorage.BackendMongo, mongoErr)
	} else {
		s.recoverBackendHealth(txstorage.BackendMongo, mongoGeneration)
	}
	return errors.Join(redisErr, mongoErr)
}

// ChangeWriter 返回项目业务与 txn.Runtime 共用的 mode-aware 写入口。入口按
// TypeSpec.Storage.Mode 检查对应健康状态并选择 Redis、Mongo 或 Fusion writer。
func (s *Storage) ChangeWriter() txstorage.ChangeApplier {
	if s == nil {
		return storageChangeWriter{}
	}
	return storageChangeWriter{
		storage: s,
		redis:   s.redis,
		mongo:   s.mongo,
		fusion:  s.fusion,
	}
}

type storageChangeWriter struct {
	storage *Storage
	redis   txstorage.ChangeApplier
	mongo   txstorage.ChangeApplier
	fusion  txstorage.ChangeApplier
}

func (writer storageChangeWriter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options txstorage.ApplyOptions) error {
	if writer.storage == nil {
		return errStorageNotConfigured
	}
	mode := spec.Storage.Mode
	if err := writer.storage.requireStorageMode(mode); err != nil {
		return err
	}
	var applier txstorage.ChangeApplier
	switch mode {
	case meta.StorageModeRedis:
		applier = writer.redis
	case meta.StorageModeMongo:
		applier = writer.mongo
	case meta.StorageModeFusion:
		applier = writer.fusion
	default:
		return fmt.Errorf("server: unsupported storage mode %q", mode)
	}
	if applier == nil {
		return fmt.Errorf("server: %s writer is not configured", mode)
	}
	if err := applier.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options); err != nil {
		writer.storage.observeStorageError(err)
		return err
	}
	return nil
}

func buildRedisCluster(ctx context.Context, cfg storageconfig.RouteTableConfig) (*txredis.Cluster, []*goredis.Client, map[string]txgoredis.Client, error) {
	routeTable, err := cfg.RouteTable()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("server: build redis route table: %w", err)
	}
	shards := map[string]txredis.ScriptClient{}
	runtimeClients := map[string]txgoredis.Client{}
	clients := make([]*goredis.Client, 0, len(cfg.Endpoints()))
	for _, endpoint := range sortedEndpointValues(cfg.Endpoints()) {
		options, err := goredis.ParseURL(endpoint)
		if err != nil {
			closeRedisClients(clients)
			return nil, nil, nil, fmt.Errorf("server: parse redis endpoint %s: %w", endpoint, err)
		}
		client := goredis.NewClient(options)
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			closeRedisClients(clients)
			return nil, nil, nil, fmt.Errorf("server: ping redis endpoint %s: %w", endpoint, err)
		}
		runtimeClient := txgoredis.NewClient(client)
		clients = append(clients, client)
		runtimeClients[endpoint] = runtimeClient
		shards[endpoint] = runtimeClient
	}
	cluster, err := txredis.NewCluster(routeTable, shards)
	if err != nil {
		closeRedisClients(clients)
		return nil, nil, nil, fmt.Errorf("server: build redis cluster: %w", err)
	}
	return cluster, clients, runtimeClients, nil
}

func buildMongoCluster(ctx context.Context, cfg storageconfig.RouteTableConfig) (*txmongo.Cluster, []*mongodriver.Client, error) {
	routeTable, err := cfg.RouteTable()
	if err != nil {
		return nil, nil, fmt.Errorf("server: build mongo route table: %w", err)
	}
	shards := map[string]txmongo.Client{}
	clients := make([]*mongodriver.Client, 0, len(cfg.Endpoints()))
	for _, endpoint := range sortedEndpointValues(cfg.Endpoints()) {
		client, err := mongodriver.Connect(ctx, options.Client().ApplyURI(endpoint))
		if err != nil {
			disconnectMongoClients(ctx, clients)
			return nil, nil, fmt.Errorf("server: connect mongo endpoint %s: %w", endpoint, err)
		}
		if err := client.Ping(ctx, nil); err != nil {
			_ = client.Disconnect(ctx)
			disconnectMongoClients(ctx, clients)
			return nil, nil, fmt.Errorf("server: ping mongo endpoint %s: %w", endpoint, err)
		}
		dbName, err := mongoDatabaseName(endpoint)
		if err != nil {
			_ = client.Disconnect(ctx)
			disconnectMongoClients(ctx, clients)
			return nil, nil, err
		}
		clients = append(clients, client)
		shards[endpoint] = txmongodriver.NewDatabase(client.Database(dbName))
	}
	cluster, err := txmongo.NewCluster(routeTable, shards)
	if err != nil {
		disconnectMongoClients(ctx, clients)
		return nil, nil, fmt.Errorf("server: build mongo cluster: %w", err)
	}
	return cluster, clients, nil
}

func buildDelayQueue(ctx context.Context, cfg storageconfig.RouteTableConfig, clients map[string]txgoredis.Client, opts storageOptions) (*txtaskredis.Backend, *txfusion.TaskQueue, error) {
	routeType := opts.DelayTaskRouteType
	if routeType == "" {
		routeType = defaultDelayTaskRouteType
	}
	routeTable, err := cfg.RouteTable()
	if err != nil {
		return nil, nil, fmt.Errorf("server: build delay queue route table: %w", err)
	}
	endpoint, err := routeTable.RouteRedis(ctx, routeType, int64(1))
	if err != nil {
		return nil, nil, fmt.Errorf("server: route delay queue redis endpoint %q: %w", routeType, err)
	}
	client, ok := clients[endpoint]
	if !ok {
		return nil, nil, fmt.Errorf("server: delay queue redis endpoint %q is not configured", endpoint)
	}
	backend := txtaskredis.NewBackend(client, txtaskredis.Options{
		Prefix:       opts.DelayTaskPrefix,
		BucketCount:  opts.DelayTaskBuckets,
		LeaseTimeout: opts.DelayTaskLease,
	})
	return backend, txfusion.NewTaskQueue(backend), nil
}

func mongoDatabaseName(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("server: parse mongo endpoint %s: %w", endpoint, err)
	}
	name := strings.Trim(parsed.Path, "/")
	if name == "" {
		return "", fmt.Errorf("server: mongo endpoint %s must include database name", endpoint)
	}
	return name, nil
}

func sortedEndpointValues(endpoints map[string]string) []string {
	values := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		values = append(values, endpoint)
	}
	sort.Strings(values)
	return values
}

func closeRedisClients(clients []*goredis.Client) {
	for _, client := range clients {
		_ = client.Close()
	}
}

func disconnectMongoClients(ctx context.Context, clients []*mongodriver.Client) error {
	var firstErr error
	for _, client := range clients {
		if err := client.Disconnect(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
