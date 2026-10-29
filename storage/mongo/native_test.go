package mongo_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/meta"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	"github.com/ngnl47/tx/storage/mongo/mongodriver"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestNativeDatabaseReusesSelectedClient(t *testing.T) {
	client, err := mongo.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	database := client.Database("world")
	routeErr := errors.New("路由不存在")
	cluster, err := txmongo.NewCluster(txmongo.RouterFunc(func(_ context.Context, route string, key any) (string, error) {
		if route != "world" || key != int64(17) {
			return "", routeErr
		}
		return "selected", nil
	}), map[string]txmongo.Client{"selected": mongodriver.NewDatabase(database)})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := cluster.MongoDatabase(t.Context(), "world", int64(17))
		if err != nil || got != database || got.Client() != client {
			t.Fatalf("未复用路由数据库：%v %v", got, err)
		}
	}
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{Route: meta.RouteSpec{Type: "world"}, Collection: "buckets"}}
	collection, err := txmongo.Collection(t.Context(), cluster, spec, int64(17))
	if err != nil || collection.Name() != "buckets" || collection.Database().Client() != client {
		t.Fatalf("集合未使用类型元数据和共享连接：%v %v", collection, err)
	}
	if _, err := txmongo.Collection(t.Context(), cluster, meta.TypeSpec{}, int64(17)); err == nil {
		t.Fatal("非 Mongo 类型仍返回集合")
	}
	if _, err := cluster.MongoDatabase(t.Context(), "missing", int64(17)); !errors.Is(err, routeErr) {
		t.Fatalf("路由错误丢失：%v", err)
	}
}
