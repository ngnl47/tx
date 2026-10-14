package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	txstorage "github.com/ngnl47/tx/storage"
	txfusion "github.com/ngnl47/tx/storage/fusion"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txmongodriver "github.com/ngnl47/tx/storage/mongo/mongodriver"
	txredis "github.com/ngnl47/tx/storage/redis"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	txtaskredis "github.com/ngnl47/tx/storage/task/redis"
	"go.mongodb.org/mongo-driver/bson"
)

func TestFusionProcedureRollbackPreservesPendingAcrossFlushFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := storageIntegrationRedis(t, ctx)
	mongoDB := storageIntegrationMongo(t, ctx)
	spec := storageIntegrationSpec()
	roleID := time.Now().UnixMicro()
	taskPrefix := fmt.Sprintf("tx:test:fusion-procedure:%d", roleID)
	t.Cleanup(func() {
		integrationCleanupRedisKeys(context.Background(), redisClient, integrationRedisRoleKey(roleID)+"*")
		integrationCleanupRedisKeys(context.Background(), redisClient, taskPrefix+":*")
		if _, err := mongoDB.Collection("storage_integration").DeleteOne(context.Background(), bson.M{"roleId": roleID}); err != nil {
			t.Error(err)
		}
	})

	redisRuntimeClient := txgoredis.NewClient(redisClient)
	redisAdapter, err := txredis.NewAdapter(redisRuntimeClient)
	if err != nil {
		t.Fatal(err)
	}
	mongoAdapter, err := txmongo.NewAdapter(txmongodriver.NewDatabase(mongoDB))
	if err != nil {
		t.Fatal(err)
	}
	taskBackend := txtaskredis.NewBackend(redisRuntimeClient, txtaskredis.Options{
		Prefix:      taskPrefix,
		BucketCount: 1,
	})
	taskQueue := txfusion.NewTaskQueueWithOptions(taskBackend, txfusion.TaskQueueOptions{Now: func() time.Time {
		return time.Now().Add(-spec.Storage.DelaySave.Duration)
	}})
	dispatcher := txfusion.NewDelayTaskDispatcher(taskQueue)
	fusionStore := txfusion.Strategy{RedisApplier: redisAdapter, MongoLoader: mongoAdapter, DelaySubmitter: dispatcher}
	role := &fusionProcedureStore{storageIntegrationStore: newStorageIntegrationStore(roleID)}
	role.SetName("initial")
	if err := txstorage.Create(ctx, fusionStore, role); err != nil {
		t.Fatal(err)
	}
	manager := writeback.NewManager(writeback.Dependencies{Writer: fusionStore}, writeback.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatal(err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("role", roleID), role)
	procedure := fusionRoleProcedure{lock: lock, process: func(*txn.Tx) error {
		role.SetName("committed-before-rollback")
		return nil
	}}
	if err := runtime.Call(ctx, procedure); err != nil {
		t.Fatal(err)
	}
	rollbackErr := errors.New("child rollback")
	child := fusionRoleProcedure{lock: lock, process: func(*txn.Tx) error {
		role.SetName("rolled-back-child")
		return rollbackErr
	}}
	procedure.process = func(tx *txn.Tx) error {
		role.SetName("rolled-back-parent")
		return tx.Call(child)
	}
	if err := runtime.Call(ctx, procedure); !errors.Is(err, rollbackErr) {
		t.Fatalf("Call = %v, want child rollback", err)
	}
	if role.values[changeFieldName] != "committed-before-rollback" || !role.HasChanges() || manager.Pending() != 1 {
		t.Fatalf("rollback state: name=%q dirty=%v pending=%d", role.values[changeFieldName], role.HasChanges(), manager.Pending())
	}
	// 保存原始热数据，将根键改为 hash，使真实 Redis GET 在任何 patch 写入前失败。
	hotKey := integrationRedisRoleKey(roleID)
	backupKey := hotKey + ":test-backup"
	t.Cleanup(func() { _ = redisClient.Del(context.Background(), backupKey).Err() })
	if err := redisClient.Rename(ctx, hotKey, backupKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := redisClient.HSet(ctx, hotKey, "blocked", "1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Flush(ctx); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("Flush = %v, want Redis WRONGTYPE", err)
	}
	if manager.Pending() != 1 || !role.HasChanges() {
		t.Fatal("failed flush discarded pending changes")
	}
	if released, err := manager.TryRelease(ctx, lock); err != nil || released {
		t.Fatalf("release dirty root = %v, %v", released, err)
	}
	if err := redisClient.Rename(ctx, backupKey, hotKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Flush(ctx); err != nil {
		t.Fatalf("recovered Flush: %v", err)
	}
	if manager.Pending() != 0 || role.HasChanges() {
		t.Fatal("recovered flush retained pending changes")
	}
	if err := dispatcher.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	types := meta.NewRegistry()
	if err := types.RegisterType(spec); err != nil {
		t.Fatal(err)
	}
	handler := txfusion.NewDelayTaskHandlerWithOptions(types, redisAdapter, mongoAdapter, txfusion.DelayTaskHandlerOptions{
		RedisRevisionReader: redisAdapter, RedisRevisionTTL: redisAdapter, RedisLocker: redisAdapter, DelayedSaveQueue: taskQueue,
	})
	scheduler := txfusion.NewSchedulerWithOptions(taskBackend, txfusion.SchedulerOptions{BatchSize: 1, Handler: handler})
	if claimed, err := scheduler.RunOnce(ctx, time.Now()); err != nil || claimed != 1 {
		t.Fatalf("scheduler claimed=%d err=%v", claimed, err)
	}
	var doc bson.M
	if err := mongoDB.Collection("storage_integration").FindOne(ctx, bson.M{"roleId": roleID}).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if doc["name"] != "committed-before-rollback" {
		t.Fatalf("persisted name = %v, want committed value", doc["name"])
	}
	if released, err := manager.TryRelease(ctx, lock); err != nil || !released {
		t.Fatalf("release clean root = %v, %v", released, err)
	}
}

type fusionRoleProcedure struct {
	lock    txn.LockTarget
	process func(*txn.Tx) error
}

func (p fusionRoleProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{p.lock}, nil
}
func (p fusionRoleProcedure) Process(tx *txn.Tx) error { return p.process(tx) }

// 事务夹具只实现存储/撤销 SPI，不依赖生成器或应用 GD。
type fusionProcedureStore struct {
	*storageIntegrationStore
	recorder change.UndoRecorder
}

func (s *fusionProcedureStore) SetUndoRecorder(recorder change.UndoRecorder) { s.recorder = recorder }
func (s *fusionProcedureStore) SetName(name string) {
	previous, changes := s.values[changeFieldName], s.changes.clone()
	if s.recorder != nil {
		s.recorder.RecordUndo(func() { s.values[changeFieldName], s.changes = previous, changes })
	}
	s.storageIntegrationStore.SetName(name)
}
func (s *fusionProcedureStore) CollapseChanges() { s.AllTouch() }
