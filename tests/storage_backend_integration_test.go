package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txstorage "github.com/ngnl47/tx/storage"
	txfusion "github.com/ngnl47/tx/storage/fusion"
	txmongodriver "github.com/ngnl47/tx/storage/mongo/mongodriver"
	txredis "github.com/ngnl47/tx/storage/redis"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	"github.com/ngnl47/tx/storage/task"
	txtaskredis "github.com/ngnl47/tx/storage/task/redis"
	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// 这些测试只依赖存储 SPI，用固定变更源替代应用生成的 GDRole。
func storageIntegrationSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{FullName: "StorageIntegration"}, Fields: []meta.Field{
			{Name: "RoleId", JSONName: "roleId", BSONName: "roleId", Type: "int64", Exported: true},
			{Name: "Name", JSONName: "name", BSONName: "name", Type: "string", Exported: true},
			{Name: "Items", JSONName: "items", BSONName: "items", Type: "map[string]any", Exported: true},
			{Name: "RankInfo", JSONName: "rankInfo", BSONName: "rankInfo", Type: "map[string]any", Exported: true},
		}},
		DocumentFactory: func() any {
			return &struct {
				RoleId int64          `json:"roleId" bson:"roleId"`
				Name   string         `json:"name" bson:"name"`
				Items  map[string]any `json:"items" bson:"items"`
			}{}
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeFusion, DelaySave: meta.Duration{Duration: time.Minute}, DelayDelete: meta.Duration{Duration: time.Hour}},
		Redis:   &meta.RedisSpec{Key: "tx:test:storage", IDField: "RoleId"},
		Mongo:   &meta.MongoSpec{Collection: "storage_integration", IDField: "RoleId"},
		Fields:  map[string]meta.FieldSpec{"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}}},
	}
}

type storageIntegrationStore struct {
	testChangeStore
	values testValueResolver
}

func newStorageIntegrationStore(id int64) *storageIntegrationStore {
	s := &storageIntegrationStore{
		testChangeStore: testChangeStore{spec: storageIntegrationSpec(), key: txstorage.RecordKey{ID: id}},
		values:          testValueResolver{4: id, changeFieldName: "storage-integration", changeFieldItems: map[string]any{"1001": map[string]any{"quantity": int64(3)}}},
	}
	s.AllTouch()
	return s
}
func (s *storageIntegrationStore) AllTouch() {
	s.changes = recordedChanges{
		{Path: changePath(fieldNode(4)), Op: change.OpSet},
		{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet},
		{Path: changePath(fieldNode(changeFieldItems)), Op: change.OpReplace},
	}
}
func (s *storageIntegrationStore) SetName(name string) {
	s.values[changeFieldName] = name
	s.changes = append(s.changes, recordedChange{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet})
}
func (s *storageIntegrationStore) FieldNameResolver() change.FieldNameResolver {
	return testFieldNameResolver{
		change.FieldNameGo:   {1: "Name", 2: "Items", 4: "RoleId", 5: "RankInfo", 6: "Level"},
		change.FieldNameJSON: {1: "name", 2: "items", 4: "roleId", 5: "rankInfo", 6: "level"},
		change.FieldNameBSON: {1: "name", 2: "items", 4: "roleId", 5: "rankInfo", 6: "level"},
	}
}
func (s *storageIntegrationStore) ResolveValue(path change.Path) (any, bool) {
	return s.values.ResolveValue(path)
}
func applyStorageIntegrationStore(ctx context.Context, applier txstorage.ChangeApplier, spec meta.TypeSpec, key txstorage.RecordKey, store *storageIntegrationStore, options txstorage.ApplyOptions) error {
	return applier.ApplyChangesWithOptions(ctx, spec, key, store, store.FieldNameResolver(), store, options)
}

func storageIntegrationRedis(t *testing.T, ctx context.Context) *goredis.Client {
	t.Helper()
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_REDIS_ENDPOINT 运行真实 Redis 存储回归")
	}
	client := goredis.NewClient(&goredis.Options{Addr: endpoint})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func storageIntegrationMongo(t *testing.T, ctx context.Context) *mongodriver.Database {
	t.Helper()
	uri := os.Getenv("TX_MONGO_URI")
	if uri == "" {
		t.Skip("设置 TX_MONGO_URI 运行真实 Mongo 存储回归")
	}
	client, err := mongodriver.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	return client.Database("tx_storage_integration")
}

func integrationCleanupStorage(ctx context.Context, client *goredis.Client, db *mongodriver.Database, id int64) {
	prefix := integrationRedisRoleKey(id)
	_ = client.Del(ctx, prefix, prefix+":items", prefix+"@v", prefix+"@lock").Err()
	_, _ = db.Collection("storage_integration").DeleteOne(ctx, bson.M{"roleId": id})
}

type integrationRecordingDelayQueue struct{ ops []txfusion.DelayedSave }

type integrationImmediateDelaySubmitter struct {
	queue txfusion.DelayedSaveQueue
}

func (submitter integrationImmediateDelaySubmitter) Submit(spec meta.TypeSpec, key txstorage.RecordKey) {
	_ = submitter.queue.Enqueue(context.Background(), txfusion.DelayedSave{
		Type: spec.Type.Ref, Key: key, RunAfter: spec.Storage.DelaySave.Duration,
	})
}

func (q *integrationRecordingDelayQueue) Enqueue(_ context.Context, op txfusion.DelayedSave) error {
	q.ops = append(q.ops, op)
	return nil
}

type integrationRedisMutatingDocumentReplacer struct {
	delegate txstorage.VersionedDocumentReplacer
	after    func(context.Context) error
}

type integrationBlockingDocumentReplacer struct {
	delegate txstorage.VersionedDocumentReplacer
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (r *integrationBlockingDocumentReplacer) ReadDocumentVersion(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey) (txstorage.DocumentVersion, error) {
	return r.delegate.ReadDocumentVersion(ctx, spec, key)
}

func (r *integrationBlockingDocumentReplacer) ReplaceDocumentAtVersion(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, doc map[string]any, version txstorage.DocumentVersion) error {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return r.delegate.ReplaceDocumentAtVersion(ctx, spec, key, doc, version)
}

type integrationFirstLoadsBarrier struct {
	delegate txstorage.DocumentLoader
	ready    chan struct{}
	release  chan struct{}

	mu    sync.Mutex
	loads int
}

func (l *integrationFirstLoadsBarrier) LoadDocument(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, fields ...string) (map[string]any, bool, error) {
	doc, exists, err := l.delegate.LoadDocument(ctx, spec, key, fields...)
	if err != nil {
		return nil, false, err
	}
	l.mu.Lock()
	l.loads++
	shouldWait := l.loads <= 2
	if l.loads == 2 {
		close(l.ready)
	}
	l.mu.Unlock()
	if !shouldWait {
		return doc, exists, nil
	}
	select {
	case <-l.release:
		return doc, exists, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

type integrationCountingDocumentLoader struct {
	delegate txstorage.DocumentLoader

	mu    sync.Mutex
	loads int
}

type integrationFirstDocumentLoadBarrier struct {
	delegate txstorage.DocumentLoader
	ready    chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (l *integrationFirstDocumentLoadBarrier) LoadDocument(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, fields ...string) (map[string]any, bool, error) {
	doc, exists, err := l.delegate.LoadDocument(ctx, spec, key, fields...)
	if err != nil {
		return nil, false, err
	}
	wait := false
	l.once.Do(func() {
		wait = true
		close(l.ready)
	})
	if !wait {
		return doc, exists, nil
	}
	select {
	case <-l.release:
		return doc, exists, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

type integrationCountingBatchDocumentLoader struct {
	delegate interface {
		txstorage.DocumentLoader
		txstorage.BatchDocumentLoader
	}

	mu     sync.Mutex
	single int
	batch  int
}

// integrationFirstBatchLoadsBarrier 让前两次 Mongo 批量读取在拿到候选文档后暂停，
// 用于验证回填前的 Redis 二次读取不会让旧 Mongo 候选覆盖新的热数据。
type integrationFirstBatchLoadsBarrier struct {
	delegate interface {
		txstorage.DocumentLoader
		txstorage.BatchDocumentLoader
	}
	ready   chan struct{}
	release chan struct{}

	mu    sync.Mutex
	loads int
}

func (l *integrationFirstBatchLoadsBarrier) LoadDocument(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, fields ...string) (map[string]any, bool, error) {
	return l.delegate.LoadDocument(ctx, spec, key, fields...)
}

func (l *integrationFirstBatchLoadsBarrier) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []txstorage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	documents, exists, err := l.delegate.LoadDocuments(ctx, spec, keys, fields...)
	if err != nil {
		return nil, nil, err
	}
	l.mu.Lock()
	l.loads++
	shouldWait := l.loads <= 2
	if l.loads == 2 {
		close(l.ready)
	}
	l.mu.Unlock()
	if !shouldWait {
		return documents, exists, nil
	}
	select {
	case <-l.release:
		return documents, exists, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (l *integrationCountingBatchDocumentLoader) LoadDocument(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, fields ...string) (map[string]any, bool, error) {
	l.mu.Lock()
	l.single++
	l.mu.Unlock()
	return l.delegate.LoadDocument(ctx, spec, key, fields...)
}

func (l *integrationCountingBatchDocumentLoader) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []txstorage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	l.mu.Lock()
	l.batch++
	l.mu.Unlock()
	return l.delegate.LoadDocuments(ctx, spec, keys, fields...)
}

func (l *integrationCountingBatchDocumentLoader) Counts() (single int, batch int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.single, l.batch
}

func (l *integrationCountingDocumentLoader) LoadDocument(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, fields ...string) (map[string]any, bool, error) {
	l.mu.Lock()
	l.loads++
	l.mu.Unlock()
	return l.delegate.LoadDocument(ctx, spec, key, fields...)
}

func (l *integrationCountingDocumentLoader) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.loads
}

func (r integrationRedisMutatingDocumentReplacer) ReadDocumentVersion(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey) (txstorage.DocumentVersion, error) {
	return r.delegate.ReadDocumentVersion(ctx, spec, key)
}

func (r integrationRedisMutatingDocumentReplacer) ReplaceDocumentAtVersion(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, doc map[string]any, version txstorage.DocumentVersion) error {
	if err := r.delegate.ReplaceDocumentAtVersion(ctx, spec, key, doc, version); err != nil {
		return err
	}
	return r.after(ctx)
}

func integrationAssertRedisRoleLoad(t *testing.T, ctx context.Context, adapter *txredis.Adapter, key txstorage.RecordKey, roleID int64) {
	t.Helper()
	doc, exists, err := adapter.LoadDocument(ctx, storageIntegrationSpec(), key)
	if err != nil {
		t.Fatalf("load redis role document: %v", err)
	}
	if !exists {
		t.Fatalf("load redis role exists = false, want true")
	}
	if integrationRedisInt64(t, doc["roleId"]) != roleID {
		t.Fatalf("loaded redis roleId = %#v, want %d; doc=%#v", doc["roleId"], roleID, doc)
	}
	if doc["name"] != "storage-integration" {
		t.Fatalf("loaded redis name = %#v, want storage-integration; doc=%#v", doc["name"], doc)
	}
	items, ok := doc["items"].(map[string]any)
	if !ok {
		t.Fatalf("loaded redis items = %#v, want document; doc=%#v", doc["items"], doc)
	}
	item1001, ok := items["1001"].(map[string]any)
	if !ok {
		t.Fatalf("loaded redis items.1001 = %#v, want document", items["1001"])
	}
	if integrationRedisInt64(t, item1001["quantity"]) != int64(3) {
		t.Fatalf("loaded redis items.1001.quantity = %#v, want 3; doc=%#v", item1001["quantity"], doc)
	}

	if _, exists := item1001["quality"]; exists {
		t.Fatalf("loaded redis items.1001 still contains quality: %#v", doc)
	}
}

func integrationAssertRedisRoleTTLNear(t *testing.T, ctx context.Context, client *goredis.Client, roleID int64, expected time.Duration) {
	t.Helper()
	for _, key := range []string{
		integrationRedisRoleKey(roleID),
		integrationRedisRoleKey(roleID) + ":items",
		integrationRedisRoleKey(roleID) + "@v",
	} {
		exists, err := client.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("check redis key %s: %v", key, err)
		}
		if exists != 1 {
			t.Fatalf("redis key %s exists = %d, want 1", key, exists)
		}
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("read redis ttl %s: %v", key, err)
		}
		if ttl <= expected-time.Minute || ttl > expected {
			t.Fatalf("redis ttl %s = %s, want within (%s, %s]", key, ttl, expected-time.Minute, expected)
		}
	}
}

func integrationAssertRedisKeysShareTTL(t *testing.T, ctx context.Context, client *goredis.Client, keys ...string) {
	t.Helper()
	var mainTTL time.Duration
	for index, key := range keys {
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("read redis ttl %s: %v", key, err)
		}
		if ttl <= 0 {
			t.Fatalf("redis ttl %s = %s, want positive ttl", key, ttl)
		}
		if index == 0 {
			mainTTL = ttl
			continue
		}
		if delta := ttl - mainTTL; delta < -time.Second || delta > time.Second {
			t.Fatalf("redis ttl %s = %s, want same lifecycle as main document ttl %s", key, ttl, mainTTL)
		}
	}
}

func integrationAssertRedisKeysPTTLNear(t *testing.T, ctx context.Context, client *goredis.Client, expected time.Duration, keys ...string) {
	t.Helper()
	for _, key := range keys {
		ttl, err := client.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("read redis pttl %s: %v", key, err)
		}
		if ttl <= expected-400*time.Millisecond || ttl > expected {
			t.Fatalf("redis pttl %s = %s, want within (%s, %s]", key, ttl, expected-400*time.Millisecond, expected)
		}
	}
}

func integrationRedisJSONDocument(t *testing.T, ctx context.Context, client *goredis.Client, key string) map[string]any {
	t.Helper()
	body, err := client.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("read redis key %s: %v", key, err)
	}
	var doc map[string]any
	decoder := json.NewDecoder(bytes.NewReader([]byte(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("unmarshal redis key %s body %q: %v", key, body, err)
	}
	return doc
}

func integrationRedisInt64(t *testing.T, value any) int64 {
	t.Helper()
	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("redis value = %#v, want json.Number", value)
	}
	got, err := number.Int64()
	if err != nil {
		t.Fatalf("json.Number.Int64(%q) returned error: %v", number, err)
	}
	return got
}

func integrationCleanupRedisKeys(ctx context.Context, client *goredis.Client, pattern string) {
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}

func integrationRedisRoleKey(roleID int64) string {
	return fmt.Sprintf("tx:test:storage:{%d}", roleID)
}

func integrationSeedRedisRoleDocuments(t *testing.T, ctx context.Context, client *goredis.Client, roleID int64) {
	t.Helper()
	prefix := integrationRedisRoleKey(roleID)
	mainDocument := fmt.Sprintf(`{"roleId":%d,"name":"old","power":1,"headImg":"old"}`, roleID)
	if err := client.Set(ctx, prefix, mainDocument, 0).Err(); err != nil {
		t.Fatalf("seed redis main doc: %v", err)
	}
	if err := client.Set(ctx, prefix+":items", `{"items":{"1001":{"quantity":1,"quality":9}}}`, 0).Err(); err != nil {
		t.Fatalf("seed redis items doc: %v", err)
	}
}

func TestRedisVersionKeyFollowsDocumentTTL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)

	roleID := time.Now().UnixMicro()
	key := integrationRedisRoleKey(roleID)
	if err := redisClient.Del(ctx, key, key+":items", key+"@v").Err(); err != nil {
		t.Fatalf("clear redis role keys: %v", err)
	}
	t.Cleanup(func() {
		_ = redisClient.Del(context.Background(), key, key+":items", key+"@v").Err()
	})

	store := newStorageIntegrationStore(roleID)
	store.SetName("version-key-ttl")
	adapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	const ttl = 1500 * time.Millisecond
	if err := txstorage.CreateWithOptions(ctx, adapter, store, txstorage.ApplyOptions{TTL: ttl}); err != nil {
		t.Fatalf("create redis role: %v", err)
	}

	integrationAssertRedisKeysPTTLNear(t, ctx, redisClient, ttl, key, key+"@v")
}

func TestRedisConditionalTTLConvergencePreservesNewerHotDocument(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)

	roleID := time.Now().UnixMicro()
	prefix := integrationRedisRoleKey(roleID)
	if err := redisClient.Del(ctx, prefix, prefix+":items", prefix+"@v").Err(); err != nil {
		t.Fatalf("clear redis role keys: %v", err)
	}
	t.Cleanup(func() {
		_ = redisClient.Del(context.Background(), prefix, prefix+":items", prefix+"@v").Err()
	})

	adapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	spec := storageIntegrationSpec()
	recordKey := txstorage.RecordKey{ID: roleID}
	role := newStorageIntegrationStore(roleID)
	dirtyTTL := 7 * 24 * time.Hour
	if err := txstorage.CreateWithOptions(ctx, adapter, role, txstorage.ApplyOptions{TTL: dirtyTTL}); err != nil {
		t.Fatalf("create redis role: %v", err)
	}
	previousRevision, err := adapter.ReadRecordRevision(ctx, spec, recordKey)
	if err != nil {
		t.Fatalf("read initial redis revision: %v", err)
	}

	role.SetName("newer-hot-document")
	if err := txstorage.SaveWithOptions(ctx, adapter, role, txstorage.ApplyOptions{TTL: dirtyTTL}); err != nil {
		t.Fatalf("update redis role: %v", err)
	}
	currentRevision, err := adapter.ReadRecordRevision(ctx, spec, recordKey)
	if err != nil {
		t.Fatalf("read updated redis revision: %v", err)
	}
	if currentRevision == previousRevision {
		t.Fatalf("redis revision after update = %q, want value different from %q", currentRevision, previousRevision)
	}

	applied, current, err := adapter.ApplyTTLWithRevision(ctx, spec, recordKey, previousRevision, spec.Storage.DelayDelete.Duration)
	if err != nil {
		t.Fatalf("conditionally converge older redis revision TTL: %v", err)
	}
	if applied || current != currentRevision {
		t.Fatalf("conditional TTL with stale revision = applied:%v current:%q, want false and %q", applied, current, currentRevision)
	}
	doc := integrationRedisJSONDocument(t, ctx, redisClient, prefix)
	if doc["name"] != "newer-hot-document" {
		t.Fatalf("redis role after stale conditional TTL = %#v, want newer document", doc)
	}
	integrationAssertRedisRoleTTLNear(t, ctx, redisClient, roleID, dirtyTTL)

	applied, current, err = adapter.ApplyTTLWithRevision(ctx, spec, recordKey, currentRevision, spec.Storage.DelayDelete.Duration)
	if err != nil {
		t.Fatalf("conditionally converge current redis revision TTL: %v", err)
	}
	if !applied || current != "" {
		t.Fatalf("conditional TTL with current revision = applied:%v current:%q, want true and empty revision", applied, current)
	}
	integrationAssertRedisRoleTTLNear(t, ctx, redisClient, roleID, spec.Storage.DelayDelete.Duration)
}

func TestRedisVersionKeyExpiresWithHotDocuments(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)

	roleID := time.Now().UnixMicro()
	prefix := integrationRedisRoleKey(roleID)
	if err := redisClient.Del(ctx, prefix, prefix+":items", prefix+"@v").Err(); err != nil {
		t.Fatalf("clear redis role keys: %v", err)
	}
	t.Cleanup(func() {
		_ = redisClient.Del(context.Background(), prefix, prefix+":items", prefix+"@v").Err()
	})

	adapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	spec := storageIntegrationSpec()
	key := txstorage.RecordKey{ID: roleID}
	if err := applyStorageIntegrationStore(ctx, adapter, spec, key, newStorageIntegrationStore(roleID), txstorage.ApplyOptions{Upsert: true, TTL: time.Second}); err != nil {
		t.Fatalf("create short-lived redis role: %v", err)
	}
	integrationAssertRedisKeysShareTTL(t, ctx, redisClient, prefix, prefix+":items", prefix+"@v")

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		count, err := redisClient.Exists(ctx, prefix, prefix+":items", prefix+"@v").Result()
		if err != nil {
			t.Fatalf("read expired redis role keys: %v", err)
		}
		if count == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("redis hot documents and version key did not expire together: %s", prefix)
}

func TestFusionDelayedSaveRequeuesWhenRedisChangesBeforeTTLConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)

	spec := storageIntegrationSpec()
	roleID := time.Now().UnixMicro()
	key := txstorage.RecordKey{ID: roleID}
	integrationCleanupStorage(ctx, redisClient, mongoDB, roleID)
	t.Cleanup(func() {
		integrationCleanupStorage(context.Background(), redisClient, mongoDB, roleID)
	})
	redisAdapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	mongoAdapter := mustMongoAdapter(t, txmongodriver.NewDatabase(mongoDB))
	dirtyTTL := 7 * 24 * time.Hour
	if err := applyStorageIntegrationStore(ctx, redisAdapter, spec, key, newStorageIntegrationStore(roleID), txstorage.ApplyOptions{Upsert: true, TTL: dirtyTTL}); err != nil {
		t.Fatalf("seed redis role: %v", err)
	}

	mutated := false
	queue := &integrationRecordingDelayQueue{}
	types := meta.NewRegistry()
	if err := types.RegisterType(spec); err != nil {
		t.Fatalf("register type spec: %v", err)
	}
	replacer := integrationRedisMutatingDocumentReplacer{
		delegate: mongoAdapter,
		after: func(ctx context.Context) error {
			mutated = true
			store := newStorageIntegrationStore(roleID)
			store.SetName("changed-during-save")
			return applyStorageIntegrationStore(ctx, redisAdapter, spec, key, store, txstorage.ApplyOptions{TTL: dirtyTTL})
		},
	}
	handler := txfusion.NewDelayTaskHandlerWithOptions(types, redisAdapter, replacer, txfusion.DelayTaskHandlerOptions{
		RedisRevisionReader: redisAdapter,
		RedisRevisionTTL:    redisAdapter,
		RedisLocker:         redisAdapter,
		DelayedSaveQueue:    queue,
	})

	err := handler.HandleDelayTask(ctx, task.DelayTask{
		Type:     spec.Type.Ref.FullName,
		ID:       roleID,
		ShardKey: key.ShardKey,
		Action:   "save",
	})
	if err != nil {
		t.Fatalf("handle delayed save: %v", err)
	}
	if !mutated {
		t.Fatal("redis mutation during mongo write was not executed")
	}
	if len(queue.ops) != 1 || queue.ops[0].Key.ID != roleID {
		t.Fatalf("delayed save requeue = %#v, want one delayed save for role", queue.ops)
	}
	doc := integrationRedisJSONDocument(t, ctx, redisClient, integrationRedisRoleKey(roleID))
	if doc["name"] != "changed-during-save" {
		t.Fatalf("redis role name after delayed save = %#v, want changed document preserved", doc["name"])
	}
	integrationAssertRedisRoleTTLNear(t, ctx, redisClient, roleID, dirtyTTL)
}

func TestFusionDelayedSaveRequeuesWriteDuringLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)

	spec := storageIntegrationSpec()
	roleID := time.Now().UnixMicro()
	key := txstorage.RecordKey{ID: roleID}
	taskPrefix := fmt.Sprintf("tx:test:fusion-followup:%d", roleID)
	integrationCleanupStorage(ctx, redisClient, mongoDB, roleID)
	integrationCleanupRedisKeys(ctx, redisClient, taskPrefix+":*")
	t.Cleanup(func() {
		integrationCleanupStorage(context.Background(), redisClient, mongoDB, roleID)
		integrationCleanupRedisKeys(context.Background(), redisClient, taskPrefix+":*")
	})

	redisRuntimeClient := txgoredis.NewClient(redisClient)
	redisAdapter := mustRedisAdapter(t, redisRuntimeClient)
	mongoAdapter := mustMongoAdapter(t, txmongodriver.NewDatabase(mongoDB))
	taskBackend := txtaskredis.NewBackend(redisRuntimeClient, txtaskredis.Options{
		Prefix:      taskPrefix,
		BucketCount: 1,
	})
	taskQueue := txfusion.NewTaskQueueWithOptions(taskBackend, txfusion.TaskQueueOptions{Now: func() time.Time {
		return time.Now().Add(-spec.Storage.DelaySave.Duration)
	}})
	fusionStore := txfusion.Strategy{
		RedisApplier:   redisAdapter,
		MongoLoader:    mongoAdapter,
		DelaySubmitter: integrationImmediateDelaySubmitter{queue: taskQueue},
	}

	role := newStorageIntegrationStore(roleID)
	role.SetName("before-worker")
	if err := applyStorageIntegrationStore(ctx, fusionStore, spec, key, role, txstorage.ApplyOptions{Upsert: true}); err != nil {
		t.Fatalf("apply first fusion changes: %v", err)
	}
	role.ClearChanges()

	types := meta.NewRegistry()
	if err := types.RegisterType(spec); err != nil {
		t.Fatalf("register role type: %v", err)
	}
	replacer := &integrationBlockingDocumentReplacer{
		delegate: mongoAdapter,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	handler := txfusion.NewDelayTaskHandlerWithOptions(types, redisAdapter, replacer, txfusion.DelayTaskHandlerOptions{
		RedisRevisionReader: redisAdapter,
		RedisRevisionTTL:    redisAdapter,
		RedisLocker:         redisAdapter,
		DelayedSaveQueue:    taskQueue,
	})
	scheduler := txfusion.NewSchedulerWithOptions(taskBackend, txfusion.SchedulerOptions{BatchSize: 1, Handler: handler})
	firstRun := make(chan error, 1)
	go func() {
		claimed, runErr := scheduler.RunOnce(ctx, time.Now())
		if runErr == nil && claimed != 1 {
			runErr = fmt.Errorf("claimed %d tasks, want 1", claimed)
		}
		firstRun <- runErr
	}()
	select {
	case <-replacer.entered:
	case <-ctx.Done():
		t.Fatalf("delayed save did not reach mongo replace: %v", ctx.Err())
	}

	role.SetName("during-worker")
	if err := applyStorageIntegrationStore(ctx, fusionStore, spec, key, role, txstorage.ApplyOptions{}); err != nil {
		t.Fatalf("apply fusion changes during lease: %v", err)
	}
	role.ClearChanges()
	close(replacer.release)
	if err := <-firstRun; err != nil {
		t.Fatalf("run first delayed save: %v", err)
	}

	claimed, err := scheduler.RunOnce(ctx, time.Now())
	if err != nil {
		t.Fatalf("run follow-up delayed save: %v", err)
	}
	if claimed != 1 {
		t.Fatalf("follow-up delayed save claimed %d tasks, want 1", claimed)
	}
	var mongoDoc bson.M
	if err := mongoDB.Collection("storage_integration").FindOne(ctx, bson.M{"roleId": roleID}).Decode(&mongoDoc); err != nil {
		t.Fatalf("load mongo role after follow-up save: %v", err)
	}
	if mongoDoc["name"] != "during-worker" {
		t.Fatalf("mongo role name = %#v, want latest Redis value", mongoDoc["name"])
	}
}

func TestFusionDispatcherRecoversRedisPublicationFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)

	spec := storageIntegrationSpec()
	roleID := time.Now().UnixMicro()
	taskPrefix := fmt.Sprintf("tx:test:fusion-publish:%d", roleID)
	integrationCleanupStorage(ctx, redisClient, mongoDB, roleID)
	integrationCleanupRedisKeys(ctx, redisClient, taskPrefix+":*")
	t.Cleanup(func() {
		integrationCleanupStorage(context.Background(), redisClient, mongoDB, roleID)
		integrationCleanupRedisKeys(context.Background(), redisClient, taskPrefix+":*")
	})

	redisRuntimeClient := txgoredis.NewClient(redisClient)
	redisAdapter := mustRedisAdapter(t, redisRuntimeClient)
	mongoAdapter := mustMongoAdapter(t, txmongodriver.NewDatabase(mongoDB))
	taskBackend := txtaskredis.NewBackend(redisRuntimeClient, txtaskredis.Options{
		Prefix:      taskPrefix,
		BucketCount: 1,
	})
	taskQueue := txfusion.NewTaskQueueWithOptions(taskBackend, txfusion.TaskQueueOptions{Now: func() time.Time {
		return time.Now().Add(-spec.Storage.DelaySave.Duration)
	}})
	dispatcher := txfusion.NewDelayTaskDispatcher(taskQueue)
	fusionStore := txfusion.Strategy{
		RedisApplier: redisAdapter, MongoLoader: mongoAdapter, DelaySubmitter: dispatcher,
	}
	// payload 是入队脚本的第一个写入点；错误类型使真实发布失败而不留下半写入任务。
	payloadKey := taskPrefix + ":{0}:payload"
	if err := redisClient.Set(ctx, payloadKey, "blocked", 0).Err(); err != nil {
		t.Fatalf("block task publication: %v", err)
	}
	role := newStorageIntegrationStore(roleID)
	role.SetName("before-recovery")
	if err := txstorage.Create(ctx, fusionStore, role); err != nil {
		t.Fatalf("create hot role while publication blocked: %v", err)
	}
	if role.HasChanges() {
		t.Fatal("successful hot create retained changes")
	}
	if err := mongoDB.Collection("storage_integration").FindOne(ctx, bson.M{"roleId": roleID}).Err(); !errors.Is(err, mongodriver.ErrNoDocuments) {
		t.Fatalf("mongo before publication = %v, want no document", err)
	}

	// 发布故障期间业务仍可保存；恢复只发布根记录，worker 读取最新热状态。
	role.SetName("after-recovery")
	if err := txstorage.Save(ctx, fusionStore, role); err != nil {
		t.Fatalf("save hot role while publication blocked: %v", err)
	}
	if role.HasChanges() {
		t.Fatal("successful hot save retained changes")
	}
	failedCtx, failedCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	err := dispatcher.Drain(failedCtx)
	failedCancel()
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("blocked drain = %v, want deadline and Redis WRONGTYPE", err)
	}
	key := txstorage.RecordKey{ID: roleID}
	revision, err := redisAdapter.ReadRecordRevision(ctx, spec, key)
	if err != nil {
		t.Fatalf("read hot revision before recovery: %v", err)
	}
	if err := redisClient.Del(ctx, payloadKey).Err(); err != nil {
		t.Fatalf("restore task publication: %v", err)
	}
	if err := dispatcher.Drain(ctx); err != nil {
		t.Fatalf("recover pending publication: %v", err)
	}

	if after, err := redisAdapter.ReadRecordRevision(ctx, spec, key); err != nil || after != revision {
		t.Fatalf("publication changed hot revision: before=%q after=%q err=%v", revision, after, err)
	}

	types := meta.NewRegistry()
	if err := types.RegisterType(spec); err != nil {
		t.Fatalf("register role type: %v", err)
	}
	handler := txfusion.NewDelayTaskHandlerWithOptions(types, redisAdapter, mongoAdapter, txfusion.DelayTaskHandlerOptions{
		RedisRevisionReader: redisAdapter,
		RedisRevisionTTL:    redisAdapter,
		RedisLocker:         redisAdapter,
		DelayedSaveQueue:    taskQueue,
	})
	scheduler := txfusion.NewSchedulerWithOptions(taskBackend, txfusion.SchedulerOptions{BatchSize: 1, Handler: handler})
	if claimed, err := scheduler.RunOnce(ctx, time.Now()); err != nil || claimed != 1 {
		t.Fatalf("recovered task: claimed=%d err=%v, want 1 and nil", claimed, err)
	}
	var mongoDoc bson.M
	if err := mongoDB.Collection("storage_integration").FindOne(ctx, bson.M{"roleId": roleID}).Decode(&mongoDoc); err != nil {
		t.Fatalf("load recovered mongo role: %v", err)
	}
	if mongoDoc["name"] != "after-recovery" {
		t.Fatalf("mongo name = %#v, want latest hot value", mongoDoc["name"])
	}
	if err := dispatcher.Drain(ctx); err != nil {
		t.Fatalf("drain completed dispatcher: %v", err)
	}
	if claimed, err := scheduler.RunOnce(ctx, time.Now()); err != nil || claimed != 0 {
		t.Fatalf("after completion: claimed=%d err=%v, want 0 and nil", claimed, err)
	}
}

func TestFusionConcurrentRedisMissBackfillsFromMongoOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)

	spec := storageIntegrationSpec()
	roleID := time.Now().UnixMicro()
	key := txstorage.RecordKey{ID: roleID}
	integrationCleanupStorage(ctx, redisClient, mongoDB, roleID)
	t.Cleanup(func() {
		integrationCleanupStorage(context.Background(), redisClient, mongoDB, roleID)
	})

	redisAdapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	mongoAdapter := mustMongoAdapter(t, txmongodriver.NewDatabase(mongoDB))
	if err := applyStorageIntegrationStore(ctx, mongoAdapter, spec, key, newStorageIntegrationStore(roleID), txstorage.ApplyOptions{Upsert: true}); err != nil {
		t.Fatalf("seed mongo role: %v", err)
	}

	redisBarrier := &integrationFirstLoadsBarrier{
		delegate: redisAdapter,
		ready:    make(chan struct{}),
		release:  make(chan struct{}),
	}
	mongoLoader := &integrationCountingDocumentLoader{delegate: mongoAdapter}
	fusion := txfusion.Strategy{
		RedisLoader:     redisBarrier,
		RedisBackfiller: redisAdapter,
		RedisLocker:     redisAdapter,
		MongoLoader:     mongoLoader,
	}

	type loadResult struct {
		doc    map[string]any
		exists bool
		err    error
	}
	results := make(chan loadResult, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			doc, exists, err := fusion.LoadDocument(ctx, spec, key, "Name")
			results <- loadResult{doc: doc, exists: exists, err: err}
		}()
	}
	close(start)
	select {
	case <-redisBarrier.ready:
	case <-ctx.Done():
		t.Fatalf("concurrent redis misses did not reach barrier: %v", ctx.Err())
	}
	close(redisBarrier.release)

	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("fusion concurrent load: %v", result.err)
		}
		if !result.exists || result.doc["name"] != "storage-integration" {
			t.Fatalf("fusion concurrent document = %#v exists=%v", result.doc, result.exists)
		}
	}
	if got := mongoLoader.Count(); got != 1 {
		t.Fatalf("mongo fallback calls = %d, want 1 after redis lock double-check", got)
	}
	integrationAssertRedisRoleLoad(t, ctx, redisAdapter, key, roleID)
}

func TestFusionBackfillDoesNotOverwriteExistingHotDocument(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)

	spec := storageIntegrationSpec()
	roleID := time.Now().UnixMicro()
	key := txstorage.RecordKey{ID: roleID}
	integrationCleanupStorage(ctx, redisClient, mongoDB, roleID)
	t.Cleanup(func() {
		integrationCleanupStorage(context.Background(), redisClient, mongoDB, roleID)
	})

	redisAdapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	mongoAdapter := mustMongoAdapter(t, txmongodriver.NewDatabase(mongoDB))
	if err := applyStorageIntegrationStore(ctx, mongoAdapter, spec, key, newStorageIntegrationStore(roleID), txstorage.ApplyOptions{Upsert: true}); err != nil {
		t.Fatalf("seed mongo role: %v", err)
	}
	mongoBarrier := &integrationFirstDocumentLoadBarrier{
		delegate: mongoAdapter,
		ready:    make(chan struct{}),
		release:  make(chan struct{}),
	}
	strategy := txfusion.Strategy{
		RedisLoader:     redisAdapter,
		RedisBackfiller: redisAdapter,
		RedisLocker:     redisAdapter,
		MongoLoader:     mongoBarrier,
	}

	type loadResult struct {
		doc    map[string]any
		exists bool
		err    error
	}
	result := make(chan loadResult, 1)
	go func() {
		doc, exists, err := strategy.LoadDocument(ctx, spec, key, "Name")
		result <- loadResult{doc: doc, exists: exists, err: err}
	}()
	select {
	case <-mongoBarrier.ready:
	case <-ctx.Done():
		t.Fatalf("mongo candidate load did not reach barrier: %v", ctx.Err())
	}

	freshStore := newStorageIntegrationStore(roleID)
	freshStore.SetName("fresh-redis-document")
	if err := applyStorageIntegrationStore(ctx, redisAdapter, spec, key, freshStore, txstorage.ApplyOptions{Upsert: true, TTL: time.Minute}); err != nil {
		t.Fatalf("write fresh redis role: %v", err)
	}
	close(mongoBarrier.release)

	loaded := <-result
	if loaded.err != nil {
		t.Fatalf("fusion load: %v", loaded.err)
	}
	if !loaded.exists || loaded.doc["name"] != "fresh-redis-document" {
		t.Fatalf("fusion document = %#v exists=%v, want fresh Redis role", loaded.doc, loaded.exists)
	}
	doc, exists, err := redisAdapter.LoadDocument(ctx, spec, key)
	if err != nil {
		t.Fatalf("load redis role after backfill race: %v", err)
	}
	if !exists || doc["name"] != "fresh-redis-document" {
		t.Fatalf("redis document after backfill race = %#v exists=%v, want fresh role", doc, exists)
	}
}

func TestFusionBatchRedisMissBackfillsFromMongoOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)

	spec := storageIntegrationSpec()
	baseID := time.Now().UnixMicro()
	roleIDs := []int64{baseID, baseID + 1}
	keys := []txstorage.RecordKey{
		{ID: roleIDs[0]},
		{ID: roleIDs[1]},
	}
	for _, roleID := range roleIDs {
		integrationCleanupStorage(ctx, redisClient, mongoDB, roleID)
	}
	t.Cleanup(func() {
		for _, roleID := range roleIDs {
			integrationCleanupStorage(context.Background(), redisClient, mongoDB, roleID)
		}
	})

	redisAdapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	mongoAdapter := mustMongoAdapter(t, txmongodriver.NewDatabase(mongoDB))
	for _, roleID := range roleIDs {
		if err := applyStorageIntegrationStore(ctx, mongoAdapter, spec, txstorage.RecordKey{ID: roleID}, newStorageIntegrationStore(roleID), txstorage.ApplyOptions{Upsert: true}); err != nil {
			t.Fatalf("seed mongo role %d: %v", roleID, err)
		}
	}
	mongoLoader := &integrationCountingBatchDocumentLoader{delegate: mongoAdapter}
	fusion := txfusion.Strategy{RedisApplier: redisAdapter, MongoLoader: mongoLoader}

	docs, exists, err := fusion.LoadDocuments(ctx, spec, keys, "Name")
	if err != nil {
		t.Fatalf("batch fusion load: %v", err)
	}
	if len(docs) != len(roleIDs) || len(exists) != len(roleIDs) {
		t.Fatalf("batch fusion result lengths docs=%d exists=%d, want %d", len(docs), len(exists), len(roleIDs))
	}
	for i, roleID := range roleIDs {
		if !exists[i] || len(docs[i]) != 1 || docs[i]["name"] != "storage-integration" {
			t.Fatalf("batch fusion document[%d] = %#v exists=%v, want projected name for role %d", i, docs[i], exists[i], roleID)
		}
		integrationAssertRedisRoleLoad(t, ctx, redisAdapter, keys[i], roleID)
	}
	if single, batch := mongoLoader.Counts(); single != 0 || batch != 1 {
		t.Fatalf("mongo loader calls single/batch = %d/%d, want 0/1", single, batch)
	}
}

func TestFusionConcurrentBatchBackfillKeepsNewRedisDocuments(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)

	spec := storageIntegrationSpec()
	baseID := time.Now().UnixMicro()
	roleIDs := []int64{baseID, baseID + 1}
	keys := []txstorage.RecordKey{
		{ID: roleIDs[0]},
		{ID: roleIDs[1]},
	}
	for _, roleID := range roleIDs {
		integrationCleanupStorage(ctx, redisClient, mongoDB, roleID)
	}
	t.Cleanup(func() {
		for _, roleID := range roleIDs {
			integrationCleanupStorage(context.Background(), redisClient, mongoDB, roleID)
		}
	})

	redisAdapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	mongoAdapter := mustMongoAdapter(t, txmongodriver.NewDatabase(mongoDB))
	for _, roleID := range roleIDs {
		if err := applyStorageIntegrationStore(ctx, mongoAdapter, spec, txstorage.RecordKey{ID: roleID}, newStorageIntegrationStore(roleID), txstorage.ApplyOptions{Upsert: true}); err != nil {
			t.Fatalf("seed mongo role %d: %v", roleID, err)
		}
	}
	mongoBarrier := &integrationFirstBatchLoadsBarrier{
		delegate: mongoAdapter,
		ready:    make(chan struct{}),
		release:  make(chan struct{}),
	}
	fusion := txfusion.Strategy{
		RedisApplier: redisAdapter,
		MongoLoader:  mongoBarrier,
	}

	type loadResult struct {
		docs   []map[string]any
		exists []bool
		err    error
	}
	results := make(chan loadResult, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			documents, exists, err := fusion.LoadDocuments(ctx, spec, keys, "Name")
			results <- loadResult{docs: documents, exists: exists, err: err}
		}()
	}
	close(start)
	select {
	case <-mongoBarrier.ready:
	case <-ctx.Done():
		t.Fatalf("concurrent batch Mongo candidates did not reach barrier: %v", ctx.Err())
	}

	for _, roleID := range roleIDs {
		freshStore := newStorageIntegrationStore(roleID)
		freshStore.SetName("fresh-redis-document")
		if err := applyStorageIntegrationStore(ctx, redisAdapter, spec, txstorage.RecordKey{ID: roleID}, freshStore, txstorage.ApplyOptions{Upsert: true, TTL: time.Minute}); err != nil {
			t.Fatalf("write fresh redis role %d: %v", roleID, err)
		}
	}
	close(mongoBarrier.release)

	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent batch fusion load: %v", result.err)
		}
		if len(result.docs) != len(roleIDs) || len(result.exists) != len(roleIDs) {
			t.Fatalf("concurrent batch result lengths docs=%d exists=%d, want %d", len(result.docs), len(result.exists), len(roleIDs))
		}
		for i, roleID := range roleIDs {
			if !result.exists[i] || result.docs[i]["name"] != "fresh-redis-document" {
				t.Fatalf("concurrent batch document[%d] = %#v exists=%v, want fresh Redis role %d", i, result.docs[i], result.exists[i], roleID)
			}
		}
	}
	for i, roleID := range roleIDs {
		doc, exists, err := redisAdapter.LoadDocument(ctx, spec, keys[i])
		if err != nil {
			t.Fatalf("load fresh redis role %d: %v", roleID, err)
		}
		if !exists || doc["name"] != "fresh-redis-document" {
			t.Fatalf("redis role %d after concurrent batch backfill = %#v exists=%v, want fresh document", roleID, doc, exists)
		}
	}
}

func TestRedisCollectionElementSetReplacesExistingDocument(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)

	roleID := time.Now().UnixMicro()
	key := integrationRedisRoleKey(roleID)
	if err := redisClient.Del(ctx, key, key+":items", key+"@v").Err(); err != nil {
		t.Fatalf("clear redis role keys: %v", err)
	}
	t.Cleanup(func() {
		_ = redisClient.Del(context.Background(), key, key+":items", key+"@v").Err()
	})
	integrationSeedRedisRoleDocuments(t, ctx, redisClient, roleID)

	store := newStorageIntegrationStore(roleID)
	store.changes = recordedChanges{{Path: changePath(intKeyNode(changeFieldItems, 1001)), Op: change.OpReplace}}
	store.values[changeFieldItems] = map[string]any{"itemId": int32(1001), "quantity": int64(3)}
	adapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	if err := applyStorageIntegrationStore(ctx, adapter, storageIntegrationSpec(), txstorage.RecordKey{ID: roleID}, store, txstorage.ApplyOptions{Upsert: true, TTL: time.Minute}); err != nil {
		t.Fatalf("apply collection element set: %v", err)
	}

	itemsDoc := integrationRedisJSONDocument(t, ctx, redisClient, key+":items")
	item := itemsDoc["items"].(map[string]any)["1001"].(map[string]any)
	if integrationRedisInt64(t, item["quantity"]) != 3 {
		t.Fatalf("redis items.1001.quantity = %#v, want 3; doc=%#v", item["quantity"], itemsDoc)
	}
	if _, exists := item["quality"]; exists {
		t.Fatalf("redis items.1001 kept stale quality after Set: %#v", itemsDoc)
	}
}

func TestRedisNestedSetCreatesMissingObjectAndRejectsScalarParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)

	roleID := time.Now().UnixMicro()
	key := integrationRedisRoleKey(roleID)
	if err := redisClient.Del(ctx, key, key+"@v").Err(); err != nil {
		t.Fatalf("clear redis role keys: %v", err)
	}
	t.Cleanup(func() {
		_ = redisClient.Del(context.Background(), key, key+"@v").Err()
	})

	if err := redisClient.Set(ctx, key, `{"roleId":590000}`, 0).Err(); err != nil {
		t.Fatalf("seed redis role without rankInfo: %v", err)
	}
	role := newStorageIntegrationStore(roleID)
	role.changes = recordedChanges{{Path: changePath(fieldNode(5), fieldNode(6)), Op: change.OpSet}}
	role.values[5] = int64(7)
	adapter := mustRedisAdapter(t, txgoredis.NewClient(redisClient))
	spec := storageIntegrationSpec()
	recordKey := txstorage.RecordKey{ID: roleID}
	if err := applyStorageIntegrationStore(ctx, adapter, spec, recordKey, role, txstorage.ApplyOptions{Upsert: true, TTL: time.Minute}); err != nil {
		t.Fatalf("apply nested set with missing parent: %v", err)
	}
	mainDoc := integrationRedisJSONDocument(t, ctx, redisClient, key)
	rankInfo, ok := mainDoc["rankInfo"].(map[string]any)
	if !ok || integrationRedisInt64(t, rankInfo["level"]) != 7 {
		t.Fatalf("redis rankInfo after missing-parent set = %#v, want object with level 7", mainDoc["rankInfo"])
	}

	if err := redisClient.Set(ctx, key, `{"roleId":590000,"rankInfo":7}`, 0).Err(); err != nil {
		t.Fatalf("seed redis role with scalar rankInfo: %v", err)
	}
	role.values[5] = int64(8)
	err := applyStorageIntegrationStore(ctx, adapter, spec, recordKey, role, txstorage.ApplyOptions{Upsert: true, TTL: time.Minute})
	if !errors.Is(err, txstorage.ErrPathTypeMismatch) {
		t.Fatalf("apply nested set with scalar parent err = %v, want ErrPathTypeMismatch", err)
	}
	mainDoc = integrationRedisJSONDocument(t, ctx, redisClient, key)
	if integrationRedisInt64(t, mainDoc["rankInfo"]) != 7 {
		t.Fatalf("redis scalar rankInfo = %#v, want unchanged 7", mainDoc["rankInfo"])
	}
}
