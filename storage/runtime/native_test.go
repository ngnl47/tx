package runtime

import (
	"context"
	"errors"
	"testing"

	txstorage "github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	"github.com/ngnl47/tx/storage/mongo/mongodriver"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestNativeMongoLifetimeAndHealth(t *testing.T) {
	r := New(t.Context())
	if _, err := r.MongoDatabase(t.Context(), "world", 1); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("未启动：%v", err)
	}
	client, err := mongo.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	database := client.Database("world")
	r.mongo, err = txmongo.NewCluster(txmongo.RouterFunc(func(context.Context, string, any) (string, error) { return "world", nil }), map[string]txmongo.Client{"world": mongodriver.NewDatabase(database)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := r.MongoDatabase(t.Context(), "world", 1); err != nil || got != database {
		t.Fatalf("数据库：%v %v", got, err)
	}
	r.markBackendUnavailable(txstorage.BackendMongo, errors.New("offline"))
	if _, err := r.MongoDatabase(t.Context(), "world", 1); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("健康闸门：%v", err)
	}
	if err := r.CloseMongo(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.MongoDatabase(t.Context(), "world", 1); err == nil {
		t.Fatal("关闭后仍发放原生句柄")
	}
}
