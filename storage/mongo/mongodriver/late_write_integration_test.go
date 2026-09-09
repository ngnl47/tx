package mongodriver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/redis/goredis"
	"github.com/ngnl47/tx/storage/task"
	"github.com/ngnl47/tx/storage/task/memory"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// 对比同等持久化确认下的普通和版本条件写入；不包含 Redis 热写。
func BenchmarkFusionMongoVersionedReplace(b *testing.B) {
	address := os.Getenv("TX_MONGO_LATE_WRITE_ADDRESS")
	if address == "" {
		b.Skip("set TX_MONGO_LATE_WRITE_ADDRESS")
	}
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+address))
	if err != nil {
		b.Fatal(err)
	}
	defer client.Disconnect(ctx)
	journal := true
	db := client.Database(fmt.Sprintf("tx_version_bench_%d", time.Now().UnixNano()), options.Database().SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &journal}))
	defer db.Drop(ctx)
	adapter, err := txmongo.NewAdapter(NewDatabase(db))
	if err != nil {
		b.Fatal(err)
	}
	spec := meta.TypeSpec{
		DocumentFactory: func() any { return &lateWriteDocument{} },
		Type:            meta.Type{Ref: meta.TypeRef{FullName: "probe.Record"}, Fields: []meta.Field{{Name: "ID", JSONName: "id", BSONName: "_id", Type: "string"}, {Name: "Coins", JSONName: "coins", BSONName: "coins", Type: "int"}}},
		Mongo:           &meta.MongoSpec{IDField: "ID", Collection: "records"},
	}
	for _, mode := range []string{"replace", "versioned-replace", "incremental", "versioned-incremental"} {
		b.Run(mode, func(b *testing.B) {
			key := storage.RecordKey{ID: mode}
			doc := map[string]any{"id": key.ID, "coins": 0}
			if err := adapter.ReplaceDocument(ctx, spec, key, doc); err != nil {
				b.Fatal(err)
			}
			resolver := change.FieldNameResolverFunc(func(change.FieldID, change.FieldNameKind) string { return "coins" })
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doc["coins"] = i + 1
				if mode == "incremental" || mode == "versioned-incremental" {
					source := probeCoinChange(i + 1)
					var err error
					if mode == "incremental" {
						err = adapter.ApplyChangesWithOptions(ctx, spec, key, source, resolver, source, storage.ApplyOptions{})
					} else {
						err = adapter.ApplyVersionedChanges(ctx, spec, key, source, resolver, source)
					}
					if err != nil {
						b.Fatal(err)
					}
				} else if mode == "versioned-replace" {
					version, err := adapter.ReadDocumentVersion(ctx, spec, key)
					if err != nil {
						b.Fatal(err)
					}
					err = adapter.ReplaceDocumentAtVersion(ctx, spec, key, doc, version)
					if err != nil {
						b.Fatal(err)
					}
				} else if err := adapter.ReplaceDocument(ctx, spec, key, doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// 验证传输延迟下客户端超时不等于数据库取消；仅在显式指定本地测试地址时运行。
func TestMongoLateWriteAfterClientTimeout(t *testing.T) {
	address := os.Getenv("TX_MONGO_LATE_WRITE_ADDRESS")
	if address == "" {
		t.Skip("set TX_MONGO_LATE_WRITE_ADDRESS for isolated transport probe")
	}
	for _, mode := range []string{"direct", "incremental", "incremental-ack-loss", "fusion", "fusion-ack-loss", "fusion-create", "fusion-create-ack-loss"} {
		t.Run(mode, func(t *testing.T) { testLateMongoWrite(t, address, mode) })
	}
}

func testLateMongoWrite(t *testing.T, address, mode string) {
	ackLoss := mode == "fusion-ack-loss" || mode == "fusion-create-ack-loss" || mode == "incremental-ack-loss"
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	held, release, forwarded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	answered := make(chan struct{})
	var delayedOnce atomic.Bool
	marker := fmt.Sprintf("tx-delayed-update-probe-%d", time.Now().UnixNano())
	go func() {
		for {
			incoming, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer incoming.Close()
				outgoing, err := net.DialTimeout("tcp", address, time.Second)
				if err != nil {
					return
				}
				defer outgoing.Close()
				var delayedConnection atomic.Bool
				go func() {
					for {
						packet, err := readProbeFrame(outgoing)
						if err != nil {
							return
						}
						if delayedConnection.Load() {
							if ackLoss {
								close(held)
								select {
								case <-release:
								case <-ctx.Done():
									return
								}
							}
							close(answered)
							return
						}
						if _, err := incoming.Write(packet); err != nil {
							return
						}
					}
				}()
				for {
					packet, err := readProbeFrame(incoming)
					if err != nil {
						return
					}
					isWrite := bytes.Contains(packet, []byte("update\x00")) || bytes.Contains(packet, []byte("insert\x00"))
					delayed := isWrite && bytes.Contains(packet, []byte(marker)) && delayedOnce.CompareAndSwap(false, true)
					if delayed {
						delayedConnection.Store(true)
						if !ackLoss {
							close(held)
							select {
							case <-release:
							case <-ctx.Done():
								return
							}
						}
					}
					if _, err := outgoing.Write(packet); err != nil {
						return
					}
					if delayed {
						close(forwarded)
						select {
						case <-answered:
						case <-ctx.Done():
						}
						return
					}
				}
			}()
		}
	}()
	connect := func(endpoint string) *mongo.Client {
		client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+endpoint).SetDirect(true))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
		return client
	}
	client := connect(address)
	db := client.Database(fmt.Sprintf("tx_late_write_probe_%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		if err := db.Drop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if mode != "fusion-create" && mode != "fusion-create-ack-loss" {
		if _, err := db.Collection("records").InsertOne(ctx, bson.M{"_id": marker, "coins": 100, "untouched": "keep"}); err != nil {
			t.Fatal(err)
		}
	}
	proxy := connect(listener.Addr().String())
	if err := proxy.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	write := func(ctx context.Context, coins int) error {
		filter := txmongo.Filter{Field: "_id", Value: marker}
		values := bson.M{"coins": coins}

		result, err := NewDatabase(proxy.Database(db.Name())).UpdateOneDurable(ctx, "records", filter, map[string]any{"$set": values}, storage.ApplyOptions{})
		if err == nil && result.MatchedCount != 1 {
			return fmt.Errorf("write version conflict: matched=%d", result.MatchedCount)
		}
		return err
	}
	settled := func() {}
	if mode == "incremental" || mode == "incremental-ack-loss" {
		adapter, err := txmongo.NewAdapter(NewDatabase(proxy.Database(db.Name())))
		if err != nil {
			t.Fatal(err)
		}
		spec := meta.TypeSpec{Mongo: &meta.MongoSpec{IDField: "ID", Collection: "records"}, Type: meta.Type{Fields: []meta.Field{{Name: "ID", BSONName: "_id"}}}}
		resolver := change.FieldNameResolverFunc(func(change.FieldID, change.FieldNameKind) string { return "coins" })
		write = func(ctx context.Context, coins int) error {
			source := probeCoinChange(coins)
			return adapter.ApplyVersionedChanges(ctx, spec, storage.RecordKey{ID: marker}, source, resolver, source)
		}
	} else if mode != "direct" {
		write, settled = fusionLateWriteProbe(t, ctx, proxy.Database(db.Name()), marker)
	}
	result := make(chan error, 1)
	go func() {
		deadline, done := context.WithTimeout(ctx, 150*time.Millisecond)
		defer done()
		result <- write(deadline, 110)
	}()
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-result; err == nil {
		t.Fatal("delayed request must time out")
	} else {
		t.Logf("first write returned: %v", err)
	}

	if err := write(ctx, 120); err != nil {
		t.Fatal(err)
	}
	settled()
	close(release)
	select {
	case <-forwarded:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if mode != "direct" {
		// 等待旧连接的响应转发结束，而不是睡眠一段时间后假定旧请求已经处理。
		select {
		case <-answered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		var doc struct {
			Coins     int    `bson:"coins"`
			Untouched string `bson:"untouched"`
			Version   int64  `bson:"_tx_version"`
		}
		if err := db.Collection("records").FindOne(ctx, bson.M{"_id": marker}).Decode(&doc); err != nil {
			t.Fatal(err)
		}
		wantVersion := int64(1)
		if ackLoss {
			wantVersion = 2
		}
		if doc.Coins != 120 || doc.Version != wantVersion {
			t.Fatalf("conditional write lost: %+v", doc)
		}
		if (mode == "incremental" || mode == "incremental-ack-loss") && doc.Untouched != "keep" {
			t.Fatal("incremental write replaced an untouched field")
		}
		t.Logf("confirmed: coins=120 version=%d", doc.Version)
		return
	}
	for {
		var doc struct {
			Coins int `bson:"coins"`
		}
		if err := db.Collection("records").FindOne(ctx, bson.M{"_id": marker}).Decode(&doc); err != nil {
			t.Fatal(err)
		}
		if doc.Coins == 110 {
			settled()
			t.Log("confirmed: late timed-out write replaced 120 with 110")
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("late overwrite not observed")
		}
	}
}

type probeCoinChange int

func (probeCoinChange) HasChanges() bool { return true }
func (probeCoinChange) VisitChanges(visitor change.Visitor) error {
	return visitor.VisitChange(change.Path{Depth: 1, Nodes: [change.MaxPathDepth]change.PathNode{{Field: 1}}}, change.OpSet)
}
func (value probeCoinChange) ResolveValue(change.Path) (any, bool) { return int(value), true }

func readProbeFrame(reader io.Reader) ([]byte, error) {
	var header [16]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	length := int(binary.LittleEndian.Uint32(header[:]))
	if length < 16 || length > 48*1024*1024 {
		return nil, fmt.Errorf("invalid Mongo frame length: %d", length)
	}
	packet := make([]byte, length)
	copy(packet, header[:])
	_, err := io.ReadFull(reader, packet[16:])
	return packet, err
}

type lateWriteDocument struct {
	ID    string `json:"id" bson:"_id"`
	Coins int    `json:"coins" bson:"coins"`
}

// 复用真实 Handler 和调度器；任务队列使用内存实现，以精确控制重试时刻。
func fusionLateWriteProbe(t *testing.T, ctx context.Context, db *mongo.Database, marker string) (func(context.Context, int) error, func()) {
	t.Helper()
	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 15})
	t.Cleanup(func() { _ = raw.Close() })
	prefix := "late-probe:{" + marker + "}"
	t.Cleanup(func() {
		if err := raw.Del(context.Background(), prefix, prefix+"@v", prefix+"@lock").Err(); err != nil {
			t.Error(err)
		}
	})
	redisStore, err := txredis.NewAdapter(goredis.NewClient(raw))
	if err != nil {
		t.Fatal(err)
	}
	mongoStore, err := txmongo.NewAdapter(NewDatabase(db))
	if err != nil {
		t.Fatal(err)
	}
	spec := meta.TypeSpec{
		Type:            meta.Type{Ref: meta.TypeRef{FullName: "probe.Record"}, Fields: []meta.Field{{Name: "ID", JSONName: "id", BSONName: "_id", Type: "string"}, {Name: "Coins", JSONName: "coins", BSONName: "coins", Type: "int"}}},
		DocumentFactory: func() any { return &lateWriteDocument{} },
		Storage:         meta.StorageSpec{Mode: meta.StorageModeFusion, DelayDelete: meta.Duration{Duration: time.Minute}},
		Redis:           &meta.RedisSpec{IDField: "ID", Key: "late-probe"},
		Mongo:           &meta.MongoSpec{IDField: "ID", Collection: "records"},
	}
	registry := meta.NewRegistry()
	if err := registry.RegisterType(spec); err != nil {
		t.Fatal(err)
	}
	backend := memory.NewBackend()
	queue := fusion.NewTaskQueue(backend)
	handler := fusion.NewDelayTaskHandlerWithOptions(registry, redisStore, mongoStore, fusion.DelayTaskHandlerOptions{RedisRevisionReader: redisStore, RedisRevisionTTL: redisStore, RedisLocker: redisStore, DelayedSaveQueue: queue})
	var attemptContext context.Context
	var handlerErr error
	var elapsed time.Duration
	scheduler := fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{Handler: fusion.DelayTaskHandlerFunc(func(_ context.Context, item task.DelayTask) error {
		handlerErr = handler.HandleDelayTask(attemptContext, item)
		return handlerErr
	})})
	write := func(callContext context.Context, coins int) error {
		// 构造两个合法 Redis 热数据状态；本探针不重测上游 Procedure 增量生成。
		if err := raw.Set(ctx, prefix, fmt.Sprintf(`{"id":%q,"coins":%d}`, marker, coins), time.Minute).Err(); err != nil {
			return err
		}
		if err := raw.Set(ctx, prefix+"@v", fmt.Sprint(coins), time.Minute).Err(); err != nil {
			return err
		}
		if coins == 110 {
			if err := backend.Enqueue(ctx, task.DelayTask{Type: "probe.Record", ID: marker, Action: "save", DueAt: time.Now()}); err != nil {
				return err
			}
		}
		attemptContext = callContext
		elapsed += time.Minute
		count, err := scheduler.RunOnce(ctx, time.Now().Add(elapsed))
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected one delayed save, got %d", count)
		}
		return handlerErr
	}
	settled := func() {
		if count, err := scheduler.RunOnce(ctx, time.Now().Add(time.Hour)); err != nil || count != 0 {
			t.Fatalf("retry not settled: count=%d err=%v", count, err)
		}
	}
	return write, settled
}
