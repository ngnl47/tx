package tests

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txstorage "github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txmongodriver "github.com/ngnl47/tx/storage/mongo/mongodriver"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
)

// 后端边界使用固定 SPI 文档，不依赖应用 GD 或代码生成器。
func TestStorageBackendDocumentContracts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := storageIntegrationRedis(t, ctx)
	db := storageIntegrationMongo(t, ctx)
	id := time.Now().UnixMicro()
	t.Cleanup(func() { integrationCleanupStorage(context.Background(), client, db, id) })
	redis := mustRedisAdapter(t, txgoredis.NewClient(client))
	mongo, err := txmongo.NewAdapter(txmongodriver.NewDatabase(db))
	if err != nil {
		t.Fatal(err)
	}
	spec, key := storageIntegrationSpec(), txstorage.RecordKey{ID: id}
	store := newStorageIntegrationStore(id)

	t.Run("UnsafeMapKeys", func(t *testing.T) {
		values := map[string]any{"stable": int64(1), "a.b": int64(2), "$state": int64(3), "": int64(4)}
		store.values[changeFieldItems] = values
		if err := txstorage.Create(ctx, mongo, store); err != nil {
			t.Fatal(err)
		}
		values["a.b"], values["$state"], values[""] = int64(7), int64(8), int64(9)
		store.changes = recordedChanges{}
		for _, name := range []string{"a.b", "$state", ""} {
			store.changes = append(store.changes, recordedChange{Path: changePath(change.PathNode{Field: changeFieldItems, Key: change.PathKey{Kind: change.KeyString, Str: name}}), Op: change.OpSet})
		}
		if err := txstorage.Save(ctx, mongo, store); err != nil {
			t.Fatal(err)
		}
		doc, exists, err := mongo.LoadDocument(ctx, spec, key)
		want := map[string]any{"stable": json.Number("1"), "a.b": json.Number("7"), "$state": json.Number("8"), "": json.Number("9")}
		if err != nil || !exists || !reflect.DeepEqual(doc["items"], want) {
			t.Fatalf("更新特殊键: doc=%#v exists=%v err=%v", doc, exists, err)
		}
		delete(values, "")
		store.changes = recordedChanges{{Path: changePath(change.PathNode{Field: changeFieldItems, Key: change.PathKey{Kind: change.KeyString, Str: ""}}), Op: change.OpDelete}}
		if err := txstorage.Save(ctx, mongo, store); err != nil {
			t.Fatal(err)
		}
		delete(want, "")
		doc, exists, err = mongo.LoadDocument(ctx, spec, key)
		if err != nil || !exists || !reflect.DeepEqual(doc["items"], want) {
			t.Fatalf("删除特殊键: doc=%#v exists=%v err=%v", doc, exists, err)
		}
	})

	t.Run("NestedCollectionClear", func(t *testing.T) {
		resolver := testFieldNameResolver{
			change.FieldNameGo:   {5: "RankInfo", 6: "Skills", 7: "Flags"},
			change.FieldNameJSON: {5: "rankInfo", 6: "skills", 7: "flags"},
			change.FieldNameBSON: {5: "rankInfo", 6: "skills", 7: "flags"},
		}
		values := change.ValueResolverFunc(func(path change.Path) (any, bool) {
			if path.Nodes[path.Depth-1].Field == 6 {
				return []int{}, true
			}
			return map[string]bool{}, true
		})
		source := recordedChanges{
			{Path: changePath(fieldNode(5), fieldNode(6)), Op: change.OpClear},
			{Path: changePath(fieldNode(5), fieldNode(7)), Op: change.OpClear},
		}
		for name, backend := range map[string]interface {
			txstorage.ChangeApplier
			txstorage.DocumentReplacer
			txstorage.DocumentLoader
		}{"redis": redis, "mongo": mongo} {
			t.Run(name, func(t *testing.T) {
				seed := map[string]any{"roleId": id, "rankInfo": map[string]any{"skills": []int{1}, "flags": map[string]bool{"a": true}}}
				if err := backend.ReplaceDocument(ctx, spec, key, seed); err != nil {
					t.Fatal(err)
				}
				if err := backend.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, txstorage.ApplyOptions{}); err != nil {
					t.Fatal(err)
				}
				doc, exists, err := backend.LoadDocument(ctx, spec, key)
				want := map[string]any{"skills": []any{}, "flags": map[string]any{}}
				if err != nil || !exists || !reflect.DeepEqual(doc["rankInfo"], want) {
					t.Fatalf("清空集合: doc=%#v exists=%v err=%v", doc, exists, err)
				}
			})
		}
	})

	t.Run("ArrayElementUpdate", func(t *testing.T) {
		arraySpec := storageIntegrationSpec()
		arraySpec.Type.Fields = append(arraySpec.Type.Fields, meta.Field{Name: "Queue", JSONName: "queue", BSONName: "queue", Type: "[]map[string]any", Exported: true})
		arraySpec.Fields["Queue"] = meta.FieldSpec{Redis: &meta.RedisFieldSpec{SubKey: "queue"}}
		arraySpec.DocumentFactory = func() any {
			return &struct {
				RoleID int64            `json:"roleId" bson:"roleId"`
				Queue  []map[string]any `json:"queue" bson:"queue"`
			}{}
		}
		t.Cleanup(func() {
			if err := client.Del(context.Background(), integrationRedisRoleKey(id)+":queue").Err(); err != nil {
				t.Error(err)
			}
		})
		path := changePath(intKeyNode(8, 0), fieldNode(9))
		source := recordedChanges{{Path: path, Op: change.OpSet}}
		names := testFieldNameResolver{
			change.FieldNameGo:   {8: "Queue", 9: "Count"},
			change.FieldNameJSON: {8: "queue", 9: "count"},
			change.FieldNameBSON: {8: "queue", 9: "count"},
		}
		values := change.ValueResolverFunc(func(p change.Path) (any, bool) { return int64(8), p.Equal(path) })
		for name, backend := range map[string]interface {
			txstorage.DocumentReplacer
			txstorage.ChangeApplier
			txstorage.DocumentLoader
		}{"redis": redis, "mongo": mongo} {
			t.Run(name, func(t *testing.T) {
				seed := map[string]any{"roleId": id, "queue": []any{map[string]any{"count": int64(5)}, map[string]any{"count": int64(10)}}}
				if err := backend.ReplaceDocument(ctx, arraySpec, key, seed); err != nil {
					t.Fatal(err)
				}
				if err := backend.ApplyChangesWithOptions(ctx, arraySpec, key, source, names, values, txstorage.ApplyOptions{}); err != nil {
					t.Fatal(err)
				}
				doc, exists, err := backend.LoadDocument(ctx, arraySpec, key)
				want := []any{map[string]any{"count": json.Number("8")}, map[string]any{"count": json.Number("10")}}
				if err != nil || !exists || !reflect.DeepEqual(doc["queue"], want) {
					t.Fatalf("数组元素更新: %#v %v %v", doc, exists, err)
				}
			})
		}
	})

	t.Run("FullUpdateAndTTL", func(t *testing.T) {
		integrationSeedRedisRoleDocuments(t, ctx, client, id)
		fullSpec := storageIntegrationSpec()
		field := fullSpec.Fields["Items"]
		field.Redis.FullUpdate = true
		fullSpec.Fields["Items"] = field
		store.values[changeFieldItems] = map[string]any{"1001": map[string]any{"quantity": int64(7)}}
		store.changes = recordedChanges{{Path: changePath(fieldNode(changeFieldItems)), Op: change.OpReplace}}
		if err := applyStorageIntegrationStore(ctx, redis, fullSpec, key, store, txstorage.ApplyOptions{Upsert: true, TTL: time.Minute}); err != nil {
			t.Fatal(err)
		}
		doc := integrationRedisJSONDocument(t, ctx, client, integrationRedisRoleKey(id)+":items")
		want := map[string]any{"1001": map[string]any{"quantity": json.Number("7")}}
		if !reflect.DeepEqual(doc["items"], want) {
			t.Fatalf("整体替换仍含旧数据: %#v", doc)
		}
		store.changes = nil
		store.SetName("ttl-only")
		if err := applyStorageIntegrationStore(ctx, redis, spec, key, store, txstorage.ApplyOptions{TTL: time.Minute}); err != nil {
			t.Fatal(err)
		}
		integrationAssertRedisKeysShareTTL(t, ctx, client, integrationRedisRoleKey(id), integrationRedisRoleKey(id)+":items", integrationRedisRoleKey(id)+"@v")
	})

	t.Run("ProjectedBatchLoad", func(t *testing.T) {
		for name, backend := range map[string]interface {
			txstorage.DocumentReplacer
			txstorage.DocumentLoader
			txstorage.BatchDocumentLoader
		}{"redis": redis, "mongo": mongo} {
			t.Run(name, func(t *testing.T) {
				seed := map[string]any{"roleId": id, "name": "projected", "items": map[string]any{"1": int64(3)}}
				if err := backend.ReplaceDocument(ctx, spec, key, seed); err != nil {
					t.Fatal(err)
				}
				doc, exists, err := backend.LoadDocument(ctx, spec, key, "Name")
				if err != nil || !exists || doc["name"] != "projected" {
					t.Fatalf("部分读取: %#v %v %v", doc, exists, err)
				}
				if _, ok := doc["items"]; ok {
					t.Fatalf("部分读取包含未选择字段: %#v", doc)
				}
				docs, found, err := backend.LoadDocuments(ctx, spec, []txstorage.RecordKey{key, {ID: id + 1}}, "Name")
				if err != nil || len(docs) != 2 || len(found) != 2 || !found[0] || found[1] || docs[0]["name"] != "projected" {
					t.Fatalf("批量部分读取: %#v %v %v", docs, found, err)
				}
				if _, ok := docs[0]["items"]; ok {
					t.Fatalf("批量部分读取包含未选择字段: %#v", docs)
				}
			})
		}
	})

	t.Run("ReplaceSubKeys", func(t *testing.T) {
		options := txstorage.ApplyOptions{TTL: time.Minute}
		if err := redis.ReplaceDocumentWithOptions(ctx, spec, key, map[string]any{"roleId": id, "items": map[string]any{"1": int64(2)}}, options); err != nil {
			t.Fatal(err)
		}
		if err := redis.ReplaceDocumentWithOptions(ctx, spec, key, map[string]any{"roleId": id, "name": "main-only"}, options); err != nil {
			t.Fatal(err)
		}
		if n, err := client.Exists(ctx, integrationRedisRoleKey(id)+":items").Result(); err != nil || n != 0 {
			t.Fatalf("旧子键未清除: %d %v", n, err)
		}
		if err := redis.ReplaceDocumentWithOptions(ctx, spec, key, map[string]any{"items": map[string]any{"2": int64(5)}}, options); err != nil {
			t.Fatal(err)
		}
		raw, err := client.Get(ctx, integrationRedisRoleKey(id)).Result()
		if err != nil || raw != "{}" {
			t.Fatalf("空根文档 = %q, %v", raw, err)
		}
		doc, exists, err := redis.LoadDocument(ctx, spec, key)
		if err != nil || !exists || !reflect.DeepEqual(doc["items"], map[string]any{"2": json.Number("5")}) {
			t.Fatalf("只含子键: %#v %v %v", doc, exists, err)
		}
	})
}
