package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	"github.com/ngnl47/tx/storage/mongo"
	"github.com/ngnl47/tx/storage/mongo/mongodriver"
	"github.com/ngnl47/tx/storage/redis/goredis"
	"go.mongodb.org/mongo-driver/bson"
)

type persistentNewRoot struct{ *fusionProcedureStore }

func (v *persistentNewRoot) StoreTarget() txn.StoreTarget {
	return txn.NewStoreTarget(txn.NewLockey("creation", 0, v.RecordKey().ID), v)
}

type insertProcedure struct{ value *persistentNewRoot }

func (p insertProcedure) Locks(context.Context) (txn.Lockeys, error) { return nil, nil }
func (p insertProcedure) Process(tx *txn.Tx) error                   { return txn.Create(tx.Context(), p.value) }

// 实际 Redis/Mongo 条件写验证，不依赖业务 GD 或生成器。
func TestProcedureFirstInsertStorageIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	client := storageIntegrationRedis(t, ctx)
	database := storageIntegrationMongo(t, ctx)
	redis := mustRedisAdapter(t, goredis.NewClient(client))
	mongo, err := mongo.NewAdapter(mongodriver.NewDatabase(database))
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []meta.StorageMode{meta.StorageModeRedis, meta.StorageModeMongo, meta.StorageModeFusion} {
		t.Run(string(mode), func(t *testing.T) {
			id := time.Now().UnixMicro()
			t.Cleanup(func() {
				integrationCleanupRedisKeys(context.Background(), client, integrationRedisRoleKey(id)+"*")
				_, err := database.Collection("procedure_create").DeleteOne(context.Background(), bson.M{"_id": id})
				if err != nil {
					t.Error(err)
				}
			})
			value := &persistentNewRoot{&fusionProcedureStore{storageIntegrationStore: newStorageIntegrationStore(id)}}
			value.spec.Storage.Mode = mode
			value.spec.Mongo.Collection = "procedure_create"
			value.spec.Type.Fields[0].BSONName = "_id"
			value.ClearChanges()
			submitter := &recordingDelaySubmitter{}
			var writer storage.ChangeApplier
			var reader storage.DocumentLoader
			switch mode {
			case meta.StorageModeRedis:
				writer, reader = redis, redis
			case meta.StorageModeMongo:
				writer, reader = mongo, mongo
			case meta.StorageModeFusion:
				strategy := fusion.Strategy{RedisApplier: redis, MongoLoader: mongo, DelaySubmitter: submitter}
				writer, reader = strategy, strategy
			}
			manager := writeback.NewManager(writeback.Dependencies{Writer: writer}, writeback.Config{})
			_ = manager.BindCreationResolver(&createCache{})
			runtime := txn.NewRuntime()
			_ = runtime.BindCommitParticipant(manager)
			if err := runtime.Call(ctx, insertProcedure{value}); err != nil {
				t.Fatal(err)
			}
			if err := manager.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			doc, exists, err := reader.LoadDocument(ctx, value.TypeSpec(), value.RecordKey())
			if err != nil || !exists || doc["name"] != "storage-integration" {
				t.Fatalf("readback=%#v exists=%v err=%v", doc, exists, err)
			}
			if mode == meta.StorageModeFusion && len(submitter.keys) != 1 {
				t.Fatal("Fusion creation omitted delayed write")
			}
			// 另一个进程或失败确认后的重试不能覆盖首次写入。
			inserted, err := writer.(storage.DocumentBackfiller).BackfillDocumentWithOptions(ctx, value.TypeSpec(), value.RecordKey(), map[string]any{"roleId": id, "name": "overwrite"}, storage.ApplyOptions{})
			if err != nil || inserted {
				t.Fatal("existing document accepted", inserted, err)
			}
			doc, _, err = reader.LoadDocument(ctx, value.TypeSpec(), value.RecordKey())
			if err != nil || doc["name"] != "storage-integration" {
				t.Fatal("existing data changed", doc, err)
			}
		})
	}
}
