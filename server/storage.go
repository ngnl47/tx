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
	"github.com/ngnl47/tx/wal"
	walmongo "github.com/ngnl47/tx/wal/mongo"
	waldriver "github.com/ngnl47/tx/wal/mongo/mongodriver"
	goredis "github.com/redis/go-redis/v9"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultDelayTaskRouteType = "fusion"

var ErrStorageUnavailable = errors.New("server: storage unavailable")

type storageOptions struct {
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

type storageRuntime struct {
	ctx             context.Context
	redis           *txredis.Cluster
	mongo           *txmongo.Cluster
	wal             wal.Store
	fusion          txfusion.Strategy
	delayBackend    *txtaskredis.Backend
	delayQueue      *txfusion.TaskQueue
	delayHandler    txfusion.DelayTaskHandler
	delayWorker     *txfusion.Scheduler
	delayDispatcher *txfusion.DelayTaskDispatcher

	redisShards    map[string]txgoredis.Client
	mongoWALShards map[string]walmongo.Client
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

func newStorageRuntime(ctx context.Context) *storageRuntime {
	return &storageRuntime{ctx: ctx}
}

func (s *storageRuntime) startRedis(ctx context.Context, cfg storageconfig.RouteTableConfig) error {
	if s == nil {
		return errStorageNotConfigured
	}
	redisCluster, redisClients, redisShards, err := buildRedisCluster(ctx, cfg)
	if err != nil {
		return err
	}
	s.redis = redisCluster
	s.redisClients = redisClients
	s.redisShards = redisShards
	return nil
}

func (s *storageRuntime) startMongo(ctx context.Context, cfg storageconfig.RouteTableConfig) error {
	if s == nil {
		return errStorageNotConfigured
	}
	mongoCluster, mongoClients, mongoWALShards, mongoIndexers, err := buildMongoCluster(ctx, cfg)
	if err != nil {
		return err
	}
	s.mongo = mongoCluster
	s.mongoClients = mongoClients
	s.mongoWALShards = mongoWALShards
	s.mongoIndexers = mongoIndexers
	return nil
}

func (s *storageRuntime) initializeMongoIndexes(ctx context.Context, cfg storageconfig.RouteTableConfig, types txfusion.TypeRegistry) error {
	if s == nil {
		return errStorageNotConfigured
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
			return fmt.Errorf("server: build Mongo indexes for %s: %w", spec.Type.Ref.FullName, err)
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
					return fmt.Errorf("server: Mongo index endpoint %q for %s is not configured", endpoint, spec.Type.Ref.FullName)
				}
				if err := indexer.EnsureIndexes(ctx, spec.Mongo.Collection, indexes); err != nil {
					return fmt.Errorf("server: ensure Mongo indexes for %s on %s: %w", spec.Type.Ref.FullName, endpoint, err)
				}
			}
		}
	}
	return nil
}

func (s *storageRuntime) startFusion(ctx context.Context, redisConfig storageconfig.RouteTableConfig, opts storageOptions, types txfusion.TypeRegistry) error {
	if s == nil {
		return errStorageNotConfigured
	}
	if types == nil {
		return fmt.Errorf("server: generated type registry is required")
	}
	if s.redis == nil || s.mongo == nil {
		return fmt.Errorf("server: Redis and Mongo storage must start before Fusion")
	}
	delayBackend, delayQueue, err := buildDelayQueue(ctx, redisConfig, s.redisShards, opts)
	if err != nil {
		return err
	}
	s.delayBackend = delayBackend
	s.delayQueue = delayQueue
	s.delayDispatcher = txfusion.NewDelayTaskDispatcher(delayQueue)
	s.fusion = txfusion.Strategy{
		RedisApplier:   s.redis,
		RedisLoader:    s.redis,
		RedisReplacer:  s.redis,
		RedisLocker:    s.redis,
		MongoLoader:    s.mongo,
		DelaySubmitter: s.delayDispatcher,
		DirtyTTL:       opts.FusionDirtyTTL,
	}
	if err := s.fusion.Validate(); err != nil {
		return fmt.Errorf("server: validate fusion storage: %w", err)
	}
	delayHandler := txfusion.NewDelayTaskHandlerWithOptions(types, s.redis, s.mongo, txfusion.DelayTaskHandlerOptions{
		RedisRevisionReader: s.redis,
		RedisRevisionTTL:    s.redis,
		RedisLocker:         s.redis,
		DelayQueue:          delayQueue,
	})
	s.delayHandler = healthAwareDelayHandler{storage: s, next: delayHandler}
	return nil
}

func (s *storageRuntime) prepareDelayScheduler(opts storageOptions) error {
	if s == nil {
		return errStorageNotConfigured
	}
	if s.delayBackend == nil || s.delayHandler == nil {
		return errWorkerNotConfigured
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

func (s *storageRuntime) startWAL(ctx context.Context, cfg WALConfig, mongoConfig storageconfig.RouteTableConfig) error {
	if s == nil || s.mongo == nil {
		return errStorageNotConfigured
	}
	store, err := buildWAL(ctx, cfg, mongoConfig, s.mongoWALShards)
	if err != nil {
		return err
	}
	if store == nil {
		return fmt.Errorf("server: WAL storage is not configured")
	}
	s.wal = healthAwareWAL{storage: s, next: store}
	return nil
}

func (s *storageRuntime) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *storageRuntime) closeRedis() error {
	if s == nil {
		return nil
	}
	s.redisCloseOnce.Do(func() { closeRedisClients(s.redisClients) })
	return s.redisCloseErr
}

func (s *storageRuntime) closeMongo(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mongoCloseOnce.Do(func() { s.mongoCloseErr = disconnectMongoClients(ctx, s.mongoClients) })
	return s.mongoCloseErr
}

func (s *storageRuntime) RedisReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeRedis, s.redis)
}

func (s *storageRuntime) MongoReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeMongo, s.mongo)
}

func (s *storageRuntime) FusionReader() txstorage.DocumentLoader {
	if s == nil {
		return nil
	}
	return s.guardReader(meta.StorageModeFusion, s.fusion)
}

func (s *storageRuntime) WAL() wal.Store {
	if s == nil {
		return nil
	}
	return s.wal
}

// RedisScriptClientFor 返回 routeType/shardKey 对应的既有 Redis 客户端。
// 仅限会话绑定等项目基础设施复用；GD 数据读写仍通过 Storage 的类型化入口完成。
func (s *storageRuntime) RedisScriptClientFor(ctx context.Context, routeType string, shardKey any) (txredis.ScriptClient, error) {
	if s == nil || s.redis == nil {
		return nil, errStorageNotConfigured
	}
	return s.redis.ScriptClientFor(ctx, routeType, shardKey)
}

// RequireWritable 确认 Fusion 的 Redis 与 Mongo 依赖均可接受新的业务写入。
// 它只读取后台健康探针维护的状态，不会在业务热路径额外执行 Ping。
func (s *storageRuntime) RequireWritable() error {
	return s.requireStorageMode(meta.StorageModeFusion)
}

// probeBackend 检查指定 backend 的全部端点。只有本轮探针没有观察到新的失败时，
// 才重新开放对应健康闸门。
func (s *storageRuntime) probeBackend(ctx context.Context, backend txstorage.Backend) error {
	if s == nil {
		return errStorageNotConfigured
	}
	switch backend {
	case txstorage.BackendRedis:
		return s.probeRedis(ctx)
	case txstorage.BackendMongo:
		return s.probeMongo(ctx)
	default:
		return fmt.Errorf("server: unsupported health backend %q", backend)
	}
}

func (s *storageRuntime) probeRedis(ctx context.Context) error {
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
	return redisErr
}

func (s *storageRuntime) probeMongo(ctx context.Context) error {
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
	return mongoErr
}

// ChangeWriter 返回项目业务与 txn.Runtime 共用的 mode-aware 写入口。入口按
// TypeSpec.Storage.Mode 检查对应健康状态并选择 Redis、Mongo 或 Fusion writer。
func (s *storageRuntime) ChangeWriter() txstorage.ChangeApplier {
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
	storage *storageRuntime
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

func buildMongoCluster(ctx context.Context, cfg storageconfig.RouteTableConfig) (*txmongo.Cluster, []*mongodriver.Client, map[string]walmongo.Client, map[string]txmongo.IndexManager, error) {
	routeTable, err := cfg.RouteTable()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("server: build mongo route table: %w", err)
	}
	shards := map[string]txmongo.Client{}
	walShards := map[string]walmongo.Client{}
	indexers := map[string]txmongo.IndexManager{}
	clients := make([]*mongodriver.Client, 0, len(cfg.Endpoints()))
	for _, endpoint := range sortedEndpointValues(cfg.Endpoints()) {
		client, err := mongodriver.Connect(ctx, options.Client().ApplyURI(endpoint))
		if err != nil {
			disconnectMongoClients(ctx, clients)
			return nil, nil, nil, nil, fmt.Errorf("server: connect mongo endpoint %s: %w", endpoint, err)
		}
		if err := client.Ping(ctx, nil); err != nil {
			_ = client.Disconnect(ctx)
			disconnectMongoClients(ctx, clients)
			return nil, nil, nil, nil, fmt.Errorf("server: ping mongo endpoint %s: %w", endpoint, err)
		}
		dbName, err := mongoDatabaseName(endpoint)
		if err != nil {
			_ = client.Disconnect(ctx)
			disconnectMongoClients(ctx, clients)
			return nil, nil, nil, nil, err
		}
		clients = append(clients, client)
		database := client.Database(dbName)
		runtimeDatabase := txmongodriver.NewDatabase(database)
		shards[endpoint] = runtimeDatabase
		walShards[endpoint] = waldriver.NewDatabase(database)
		indexers[endpoint] = runtimeDatabase
	}
	cluster, err := txmongo.NewCluster(routeTable, shards)
	if err != nil {
		disconnectMongoClients(ctx, clients)
		return nil, nil, nil, nil, fmt.Errorf("server: build mongo cluster: %w", err)
	}
	return cluster, clients, walShards, indexers, nil
}

func buildWAL(ctx context.Context, cfg WALConfig, routes storageconfig.RouteTableConfig, shards map[string]walmongo.Client) (wal.Store, error) {
	target, ok := routes.Routes[cfg.Mongo.RouteType]
	if !ok {
		return nil, fmt.Errorf("server: WAL Mongo route %q is not configured", cfg.Mongo.RouteType)
	}
	walShards := make(map[string]walmongo.Client)
	for _, rule := range target.Rules {
		for _, endpoint := range rule.Shards {
			client, exists := shards[endpoint]
			if !exists {
				return nil, fmt.Errorf("server: WAL Mongo endpoint %q is not configured", endpoint)
			}
			walShards[endpoint] = client
		}
	}
	routeTable, err := routes.RouteTable()
	if err != nil {
		return nil, fmt.Errorf("server: build WAL route table: %w", err)
	}
	cluster, err := walmongo.NewCluster(ctx, routeTable, walShards, walmongo.Options{
		RouteType:  cfg.Mongo.RouteType,
		Collection: cfg.Mongo.Collection,
	})
	if err != nil {
		return nil, fmt.Errorf("server: build Mongo WAL: %w", err)
	}
	store, err := wal.NewStore(cluster)
	if err != nil {
		return nil, fmt.Errorf("server: build WAL store: %w", err)
	}
	return store, nil
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
