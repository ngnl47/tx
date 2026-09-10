package runtime

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

// Config 描述 Fusion、延迟任务队列和调度器参数。
type Config struct {
	FusionDirtyTTL           time.Duration
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
}

type Runtime struct {
	io              ioStats
	ctx             context.Context
	redis           *txredis.Cluster
	mongo           *txmongo.Cluster
	fusion          txfusion.Strategy
	delayBackend    *txtaskredis.Backend
	delayHandler    txfusion.DelayTaskHandler
	delayWorker     *txfusion.Scheduler
	delayDispatcher *txfusion.DelayTaskDispatcher

	redisShards    map[string]txgoredis.Client
	mongoIndexers  map[string]txmongo.IndexManager
	redisClients   []*goredis.Client
	mongoClients   []*mongodriver.Client
	redisCloseOnce sync.Once
	mongoCloseOnce sync.Once
	redisCloseErr  error
	mongoCloseErr  error
	healthMu       sync.RWMutex
	redisHealthErr error
	mongoHealthErr error
	redisHealthGen uint64
	mongoHealthGen uint64
}

// New 创建尚未连接外部后端的存储运行时。
func New(ctx context.Context) *Runtime {
	return &Runtime{ctx: ctx}
}

// StartRedis 建立 Redis 路由和连接。
func (s *Runtime) StartRedis(ctx context.Context, cfg storageconfig.RouteTableConfig) error {
	if s == nil {
		return ErrNotConfigured
	}
	redisCluster, redisClients, redisShards, err := buildRedisCluster(ctx, cfg, &s.io)
	if err != nil {
		return err
	}
	s.redis = redisCluster
	s.redisClients = redisClients
	s.redisShards = redisShards
	return nil
}

// StartMongo 建立 MongoDB 路由和连接。
func (s *Runtime) StartMongo(ctx context.Context, cfg storageconfig.RouteTableConfig) error {
	if s == nil {
		return ErrNotConfigured
	}
	mongoCluster, mongoClients, mongoIndexers, err := buildMongoCluster(ctx, cfg, &s.io)
	if err != nil {
		return err
	}
	s.mongo = mongoCluster
	s.mongoClients = mongoClients
	s.mongoIndexers = mongoIndexers
	return nil
}

// InitializeMongoIndexes 按生成类型注册表创建缺失索引。
func (s *Runtime) InitializeMongoIndexes(ctx context.Context, cfg storageconfig.RouteTableConfig, types txfusion.TypeRegistry) error {
	if s == nil {
		return ErrNotConfigured
	}
	if types == nil {
		return nil
	}
	for _, spec := range types.TypeSpecs() {
		if spec.Mongo == nil || spec.Storage.Mode == meta.StorageModeRedis {
			continue
		}
		target, ok := cfg.Routes[spec.Mongo.Route.Type]
		if !ok {
			continue
		}
		indexes, err := txmongo.BuildIndexModels(spec)
		if err != nil {
			return fmt.Errorf("storage runtime: build Mongo indexes for %s: %w", spec.Type.Ref.FullName, err)
		}
		if len(indexes) == 0 {
			continue
		}
		visited := make(map[string]struct{})
		for _, rule := range target.Rules {
			for _, endpoint := range rule.Shards {
				if _, ok := visited[endpoint]; ok {
					continue
				}
				visited[endpoint] = struct{}{}
				indexer, ok := s.mongoIndexers[endpoint]
				if !ok || indexer == nil {
					return fmt.Errorf("storage runtime: Mongo index endpoint %q for %s is not configured", endpoint, spec.Type.Ref.FullName)
				}
				if err := indexer.EnsureIndexes(ctx, spec.Mongo.Collection, indexes); err != nil {
					return fmt.Errorf("storage runtime: ensure Mongo indexes for %s on %s: %w", spec.Type.Ref.FullName, endpoint, err)
				}
			}
		}
	}
	return nil
}

// StartFusion 组装 Redis 热数据、Mongo 冷数据和延迟任务队列。
func (s *Runtime) StartFusion(ctx context.Context, redisConfig storageconfig.RouteTableConfig, opts Config, types txfusion.TypeRegistry) error {
	if s == nil {
		return ErrNotConfigured
	}
	if types == nil {
		return fmt.Errorf("storage runtime: generated type registry is required")
	}
	if s.redis == nil || s.mongo == nil {
		return fmt.Errorf("storage runtime: Redis and Mongo storage must start before Fusion")
	}
	delayBackend, delayedSaveQueue, err := buildDelayedSaveQueue(ctx, redisConfig, s.redisShards, opts)
	if err != nil {
		return err
	}
	s.delayBackend = delayBackend
	s.delayDispatcher = txfusion.NewDelayTaskDispatcher(delayedSaveQueue)
	s.fusion = txfusion.Strategy{
		RedisApplier:    s.redis,
		RedisLoader:     s.redis,
		RedisBackfiller: s.redis,
		RedisLocker:     s.redis,
		MongoLoader:     s.mongo,
		DelaySubmitter:  s.delayDispatcher,
		DirtyTTL:        opts.FusionDirtyTTL,
	}
	if err := s.fusion.Validate(); err != nil {
		return fmt.Errorf("storage runtime: validate fusion storage: %w", err)
	}
	delayHandler := txfusion.NewDelayTaskHandlerWithOptions(types, s.redis, s.mongo, txfusion.DelayTaskHandlerOptions{
		RedisRevisionReader: s.redis,
		RedisRevisionTTL:    s.redis,
		RedisLocker:         s.redis,
		DelayedSaveQueue:    delayedSaveQueue,
	})
	s.delayHandler = healthAwareDelayHandler{storage: s, next: delayHandler}
	return nil
}

// PrepareDelayScheduler 创建延迟任务调度器。
func (s *Runtime) PrepareDelayScheduler(opts Config) error {
	if s == nil {
		return ErrNotConfigured
	}
	if s.delayBackend == nil || s.delayHandler == nil {
		return ErrWorkerNotConfigured
	}
	s.delayWorker = txfusion.NewSchedulerWithOptions(s.delayBackend, txfusion.SchedulerOptions{
		BatchSize:         opts.SchedulerBatchSize,
		PollInterval:      opts.SchedulerPoll,
		RetryInitialDelay: opts.SchedulerRetryInitial,
		RetryMultiplier:   opts.SchedulerRetryMultiplier,
		RetryMaxDelay:     opts.SchedulerRetryMax,
		RetryJitter:       opts.SchedulerRetryJitter,
		Handler:           s.delayHandler,
	})
	return nil
}

func (s *Runtime) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// CloseRedis 关闭运行时持有的 Redis 客户端。
func (s *Runtime) CloseRedis() error {
	if s == nil {
		return nil
	}
	s.redisCloseOnce.Do(func() { closeRedisClients(s.redisClients) })
	return s.redisCloseErr
}

// CloseMongo 关闭运行时持有的 MongoDB 客户端。
func (s *Runtime) CloseMongo(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mongoCloseOnce.Do(func() { s.mongoCloseErr = disconnectMongoClients(ctx, s.mongoClients) })
	return s.mongoCloseErr
}

func (s *Runtime) RedisReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeRedis, s.redis)
}

func (s *Runtime) MongoReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeMongo, s.mongo)
}

// MongoBackfiller 返回带 Mongo 健康闸门的条件回填器。
func (s *Runtime) MongoBackfiller() txstorage.DocumentBackfiller {
	if s == nil || s.mongo == nil {
		return nil
	}
	return guardedMongoBackfiller{storage: s, next: s.mongo}
}

// MongoCounters 返回带 Mongo 健康闸门的原子计数存储。
func (s *Runtime) MongoCounters() txmongo.CounterStore {
	if s == nil || s.mongo == nil {
		return nil
	}
	return guardedMongoCounters{storage: s, next: s.mongo}
}

func (s *Runtime) FusionReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeFusion, s.fusion)
}

// RedisScriptClientFor 返回 routeType/shardKey 对应的既有 Redis 客户端。
// 仅限会话绑定等项目基础设施复用；GD 数据读写仍通过 Storage 的类型化入口完成。
func (s *Runtime) RedisScriptClientFor(ctx context.Context, routeType string, shardKey any) (txredis.ScriptClient, error) {
	if s == nil || s.redis == nil {
		return nil, ErrNotConfigured
	}
	return s.redis.ScriptClientFor(ctx, routeType, shardKey)
}

// ProbeBackend 检查指定 backend 的全部端点，并在无新失败时恢复健康闸门。
func (s *Runtime) ProbeBackend(ctx context.Context, backend txstorage.Backend) error {
	if s == nil {
		return ErrNotConfigured
	}
	switch backend {
	case txstorage.BackendRedis:
		return s.probeRedis(ctx)
	case txstorage.BackendMongo:
		return s.probeMongo(ctx)
	default:
		return fmt.Errorf("storage runtime: unsupported health backend %q", backend)
	}
}

func (s *Runtime) probeRedis(ctx context.Context) error {
	redisGeneration := s.backendHealthGeneration(txstorage.BackendRedis)
	var redisErr error
	if len(s.redisClients) == 0 {
		redisErr = errors.New("storage runtime: redis clients are not configured")
	} else {
		for _, client := range s.redisClients {
			if err := client.Ping(ctx).Err(); err != nil {
				redisErr = fmt.Errorf("storage runtime: ping redis: %w", err)
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
	return redisErr
}

func (s *Runtime) probeMongo(ctx context.Context) error {
	mongoGeneration := s.backendHealthGeneration(txstorage.BackendMongo)
	var mongoErr error
	if len(s.mongoClients) == 0 {
		mongoErr = errors.New("storage runtime: mongo clients are not configured")
	} else {
		for _, client := range s.mongoClients {
			if err := client.Ping(ctx, nil); err != nil {
				mongoErr = fmt.Errorf("storage runtime: ping mongo: %w", err)
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
	return mongoErr
}

// ChangeWriter 返回项目业务与 txn.Runtime 共用的 mode-aware 写入口。入口按
// TypeSpec.Storage.Mode 检查对应健康状态并选择 Redis、Mongo 或 Fusion writer。
func (s *Runtime) ChangeWriter() txstorage.ChangeApplier {
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
	storage *Runtime
	redis   txstorage.ChangeApplier
	mongo   txstorage.ChangeApplier
	fusion  txstorage.ChangeApplier
}

func (writer storageChangeWriter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options txstorage.ApplyOptions) error {
	if writer.storage == nil {
		return ErrNotConfigured
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
		return fmt.Errorf("storage runtime: unsupported storage mode %q", mode)
	}
	if applier == nil {
		return fmt.Errorf("storage runtime: %s writer is not configured", mode)
	}
	var err error
	if mode == meta.StorageModeMongo && !options.Upsert {
		versioned, ok := applier.(txmongo.VersionedChangeApplier)
		if !ok {
			return fmt.Errorf("storage runtime: mongo versioned updates are not configured")
		}
		err = versioned.ApplyVersionedChanges(ctx, spec, key, source, resolver, values)
	} else {
		err = applier.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options)
	}
	if err != nil {
		writer.storage.observeStorageError(err)
		return err
	}
	return nil
}

func buildRedisCluster(ctx context.Context, cfg storageconfig.RouteTableConfig, stats *ioStats) (*txredis.Cluster, []*goredis.Client, map[string]txgoredis.Client, error) {
	routeTable, err := cfg.RouteTable()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("storage runtime: build redis route table: %w", err)
	}
	shards := map[string]txredis.ScriptClient{}
	runtimeClients := map[string]txgoredis.Client{}
	clients := make([]*goredis.Client, 0, len(cfg.Endpoints()))
	for _, endpoint := range sortedEndpointValues(cfg.Endpoints()) {
		options, err := goredis.ParseURL(endpoint)
		if err != nil {
			closeRedisClients(clients)
			return nil, nil, nil, fmt.Errorf("storage runtime: parse redis endpoint %s: %w", endpoint, err)
		}
		client := goredis.NewClient(options)
		stats.attachRedis(client)
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			closeRedisClients(clients)
			return nil, nil, nil, fmt.Errorf("storage runtime: ping redis endpoint %s: %w", endpoint, err)
		}
		runtimeClient := txgoredis.NewClient(client)
		clients = append(clients, client)
		runtimeClients[endpoint] = runtimeClient
		shards[endpoint] = runtimeClient
	}
	cluster, err := txredis.NewCluster(routeTable, shards)
	if err != nil {
		closeRedisClients(clients)
		return nil, nil, nil, fmt.Errorf("storage runtime: build redis cluster: %w", err)
	}
	return cluster, clients, runtimeClients, nil
}

func buildMongoCluster(ctx context.Context, cfg storageconfig.RouteTableConfig, stats *ioStats) (*txmongo.Cluster, []*mongodriver.Client, map[string]txmongo.IndexManager, error) {
	routeTable, err := cfg.RouteTable()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("storage runtime: build mongo route table: %w", err)
	}
	shards := map[string]txmongo.Client{}
	indexers := map[string]txmongo.IndexManager{}
	clients := make([]*mongodriver.Client, 0, len(cfg.Endpoints()))
	for _, endpoint := range sortedEndpointValues(cfg.Endpoints()) {
		client, err := mongodriver.Connect(ctx, options.Client().ApplyURI(endpoint).SetMonitor(stats.mongoMonitor()))
		if err != nil {
			disconnectMongoClients(ctx, clients)
			return nil, nil, nil, fmt.Errorf("storage runtime: connect mongo endpoint %s: %w", endpoint, err)
		}
		if err := client.Ping(ctx, nil); err != nil {
			_ = client.Disconnect(ctx)
			disconnectMongoClients(ctx, clients)
			return nil, nil, nil, fmt.Errorf("storage runtime: ping mongo endpoint %s: %w", endpoint, err)
		}
		dbName, err := mongoDatabaseName(endpoint)
		if err != nil {
			_ = client.Disconnect(ctx)
			disconnectMongoClients(ctx, clients)
			return nil, nil, nil, err
		}
		clients = append(clients, client)
		database := client.Database(dbName)
		runtimeDatabase := txmongodriver.NewDatabase(database)
		shards[endpoint] = runtimeDatabase
		indexers[endpoint] = runtimeDatabase
	}
	cluster, err := txmongo.NewCluster(routeTable, shards)
	if err != nil {
		disconnectMongoClients(ctx, clients)
		return nil, nil, nil, fmt.Errorf("storage runtime: build mongo cluster: %w", err)
	}
	return cluster, clients, indexers, nil
}

func buildDelayedSaveQueue(ctx context.Context, cfg storageconfig.RouteTableConfig, clients map[string]txgoredis.Client, opts Config) (*txtaskredis.Backend, *txfusion.TaskQueue, error) {
	routeType := opts.DelayTaskRouteType
	if routeType == "" {
		routeType = defaultDelayTaskRouteType
	}
	routeTable, err := cfg.RouteTable()
	if err != nil {
		return nil, nil, fmt.Errorf("storage runtime: build delayed save queue route table: %w", err)
	}
	endpoint, err := routeTable.RouteRedis(ctx, routeType, int64(1))
	if err != nil {
		return nil, nil, fmt.Errorf("storage runtime: route delayed save queue redis endpoint %q: %w", routeType, err)
	}
	client, ok := clients[endpoint]
	if !ok {
		return nil, nil, fmt.Errorf("storage runtime: delayed save queue redis endpoint %q is not configured", endpoint)
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
		return "", fmt.Errorf("storage runtime: parse mongo endpoint %s: %w", endpoint, err)
	}
	name := strings.Trim(parsed.Path, "/")
	if name == "" {
		return "", fmt.Errorf("storage runtime: mongo endpoint %s must include database name", endpoint)
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
