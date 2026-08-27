// Package mongo 实现按 tx Mongo 路由表分片的 WAL 存储。
package mongo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/ngnl47/tx/wal"
)

var ErrDuplicatePosition = errors.New("wal/mongo: duplicate stream position")

// Router 按 routeType 和 shardKey 选择 Mongo endpoint。
type Router interface {
	Route(context.Context, string, any) (string, error)
}

// Client 是单个 Mongo database 需要提供的最小 WAL 操作集合。
type Client interface {
	EnsureIndexes(context.Context, string) error
	Insert(context.Context, string, wal.Record) error
	Load(context.Context, string, wal.Stream, uint64) (wal.Record, bool, error)
	Read(context.Context, string, wal.Stream, uint64, int) ([]wal.Record, error)
	Trim(context.Context, string, wal.Stream, uint64) error
}

// Options 定义 WAL 在现有 Mongo 路由表中的位置。
type Options struct {
	// RouteType 是现有 Mongo 路由表中专门用于 WAL 的稳定路由类型。
	RouteType string
	// Collection 是每个目标 Mongo database 中保存 WAL 记录的集合名。
	Collection string
}

// Cluster 将 WAL 操作路由到项目已经装配的 Mongo database。
type Cluster struct {
	router     Router
	shards     map[string]Client
	routeType  string
	collection string
}

var _ wal.Backend = (*Cluster)(nil)

// NewCluster 创建 WAL 集群，并在接收业务写入前确认每个分片的唯一索引。
func NewCluster(ctx context.Context, router Router, shards map[string]Client, options Options) (*Cluster, error) {
	if isNil(router) {
		return nil, fmt.Errorf("wal/mongo: router is not configured")
	}
	if strings.TrimSpace(options.RouteType) == "" || options.RouteType != strings.TrimSpace(options.RouteType) {
		return nil, fmt.Errorf("wal/mongo: route type is empty or contains surrounding whitespace")
	}
	if strings.TrimSpace(options.Collection) == "" || options.Collection != strings.TrimSpace(options.Collection) {
		return nil, fmt.Errorf("wal/mongo: collection is empty or contains surrounding whitespace")
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("wal/mongo: shards are not configured")
	}
	cluster := &Cluster{
		router:     router,
		shards:     make(map[string]Client, len(shards)),
		routeType:  options.RouteType,
		collection: options.Collection,
	}
	for name, client := range shards {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("wal/mongo: shard name is empty")
		}
		if isNil(client) {
			return nil, fmt.Errorf("wal/mongo: shard %q client is not configured", name)
		}
		if err := client.EnsureIndexes(ctx, options.Collection); err != nil {
			return nil, fmt.Errorf("wal/mongo: ensure indexes on shard %q: %w", name, err)
		}
		cluster.shards[name] = client
	}
	return cluster, nil
}

// Append 同步追加记录。同一流位置写入相同内容视为幂等成功，不同内容视为冲突。
func (cluster *Cluster) Append(ctx context.Context, record wal.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	client, err := cluster.clientFor(ctx, record.Stream)
	if err != nil {
		return err
	}
	err = client.Insert(ctx, cluster.collection, record)
	if !errors.Is(err, ErrDuplicatePosition) {
		return err
	}
	existing, exists, loadErr := client.Load(ctx, cluster.collection, record.Stream, record.Seq)
	if loadErr != nil {
		return loadErr
	}
	if !exists {
		return fmt.Errorf("wal/mongo: duplicate position %d is not readable", record.Seq)
	}
	if sameRecordContent(existing, record) {
		return nil
	}
	return fmt.Errorf("%w: namespace=%q streamKey=%q generation=%d sequence=%d", wal.ErrConflict, record.Stream.Namespace, record.Stream.StreamKey, record.Stream.Generation, record.Seq)
}

// Read 从 afterSeq 后读取一批严格连续的记录。
func (cluster *Cluster) Read(ctx context.Context, stream wal.Stream, afterSeq uint64, limit int) ([]wal.Record, error) {
	if err := stream.Validate(); err != nil {
		return nil, err
	}
	if afterSeq > math.MaxInt64 {
		return nil, fmt.Errorf("%w: sequence %d exceeds the supported range", wal.ErrInvalidRecord, afterSeq)
	}
	if limit <= 0 || limit > wal.MaxReadBatch {
		return nil, fmt.Errorf("%w: read limit must be between 1 and %d", wal.ErrInvalidRecord, wal.MaxReadBatch)
	}
	client, err := cluster.clientFor(ctx, stream)
	if err != nil {
		return nil, err
	}
	records, err := client.Read(ctx, cluster.collection, stream, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	if err := wal.ValidateSequence(stream, afterSeq, records); err != nil {
		return nil, err
	}
	return records, nil
}

// Trim 删除 checkpoint 已经覆盖的日志位置。
func (cluster *Cluster) Trim(ctx context.Context, stream wal.Stream, throughSeq uint64) error {
	if err := stream.Validate(); err != nil {
		return err
	}
	if throughSeq == 0 || throughSeq > math.MaxInt64 {
		return fmt.Errorf("%w: trim sequence must be between 1 and %d", wal.ErrInvalidRecord, int64(math.MaxInt64))
	}
	client, err := cluster.clientFor(ctx, stream)
	if err != nil {
		return err
	}
	return client.Trim(ctx, cluster.collection, stream, throughSeq)
}

func (cluster *Cluster) clientFor(ctx context.Context, stream wal.Stream) (Client, error) {
	if cluster == nil || isNil(cluster.router) {
		return nil, fmt.Errorf("wal/mongo: cluster is not configured")
	}
	endpoint, err := cluster.router.Route(ctx, cluster.routeType, streamShardKey(stream))
	if err != nil {
		return nil, err
	}
	client, ok := cluster.shards[endpoint]
	if !ok {
		return nil, fmt.Errorf("wal/mongo: shard %q is not configured", endpoint)
	}
	return client, nil
}

func streamShardKey(stream wal.Stream) int64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	add := func(value string) {
		length := uint64(len(value))
		for shift := 0; shift < 64; shift += 8 {
			hash ^= uint64(byte(length >> shift))
			hash *= prime
		}
		for index := 0; index < len(value); index++ {
			hash ^= uint64(value[index])
			hash *= prime
		}
	}
	add(stream.Namespace)
	add(stream.StreamKey)
	key := int64(hash & math.MaxInt64)
	if key == 0 {
		return 1
	}
	return key
}

func sameRecordContent(left, right wal.Record) bool {
	return left.Stream.Namespace == right.Stream.Namespace &&
		left.Stream.StreamKey == right.Stream.StreamKey &&
		left.Stream.Generation == right.Stream.Generation &&
		left.Seq == right.Seq &&
		left.PayloadVersion == right.PayloadVersion &&
		left.PayloadHash == right.PayloadHash &&
		bytes.Equal(left.Payload, right.Payload)
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
