package server

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/meta"
	txstorageconfig "github.com/ngnl47/tx/storage/config"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

type recordingIndexManager struct {
	collections []string
	indexes     [][]txmongo.IndexModel
}

func (manager *recordingIndexManager) EnsureIndexes(_ context.Context, collection string, indexes []txmongo.IndexModel) error {
	manager.collections = append(manager.collections, collection)
	manager.indexes = append(manager.indexes, indexes)
	return nil
}

func TestStorageRuntimeInitializesGeneratedIndexesOnRouteEndpoints(t *testing.T) {
	first := &recordingIndexManager{}
	second := &recordingIndexManager{}
	unused := &recordingIndexManager{}
	runtime := &storageRuntime{mongoIndexers: map[string]txmongo.IndexManager{
		"mongo-a": first,
		"mongo-b": second,
		"mongo-c": unused,
	}}
	registry := meta.NewRegistry()
	if err := registry.RegisterType(meta.TypeSpec{
		Type: meta.Type{
			Ref: meta.TypeRef{Package: "data", Name: "GDRole"},
			Fields: []meta.Field{
				{Name: "Platform", BSONName: "platform"},
				{Name: "PlatformUId", BSONName: "platformUId"},
			},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeMongo},
		Mongo: &meta.MongoSpec{
			Collection: "roles",
			Route:      meta.RouteSpec{Type: "role"},
		},
		Fields: map[string]meta.FieldSpec{
			"Platform":    {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Name: "p_id", Keys: []meta.IndexKeySpec{{Order: "1"}}}}}},
			"PlatformUId": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Name: "p_id", Keys: []meta.IndexKeySpec{{Order: "1"}}}}}},
		},
	}); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	routes := txstorageconfig.NewRouteTableConfig(map[string]txstorageconfig.RouteTarget{
		"role":  {Rules: []txstorageconfig.RouteRule{{Shards: []string{"mongo-a", "mongo-b"}}}},
		"other": {Rules: []txstorageconfig.RouteRule{{Shards: []string{"mongo-c"}}}},
	})

	if err := runtime.initializeMongoIndexes(context.Background(), routes, registry); err != nil {
		t.Fatalf("initializeMongoIndexes returned error: %v", err)
	}
	for name, manager := range map[string]*recordingIndexManager{"mongo-a": first, "mongo-b": second} {
		if len(manager.collections) != 1 || manager.collections[0] != "roles" {
			t.Fatalf("%s collections = %#v, want roles", name, manager.collections)
		}
		if len(manager.indexes[0]) != 1 || manager.indexes[0][0].Name != "p_id" || len(manager.indexes[0][0].Keys) != 2 {
			t.Fatalf("%s indexes = %#v, want p_id compound index", name, manager.indexes)
		}
	}
	if len(unused.collections) != 0 {
		t.Fatalf("unused route collections = %#v, want none", unused.collections)
	}
}

func TestStorageRuntimeSkipsGeneratedIndexesOutsideConfiguredRoutes(t *testing.T) {
	runtime := &storageRuntime{mongoIndexers: map[string]txmongo.IndexManager{}}
	registry := meta.NewRegistry()
	if err := registry.RegisterType(meta.TypeSpec{
		Type: meta.Type{
			Ref:    meta.TypeRef{Package: "data", Name: "GDAdmin"},
			Fields: []meta.Field{{Name: "AdminId", BSONName: "adminId"}},
		},
		Storage: meta.StorageSpec{Mode: meta.StorageModeMongo},
		Mongo: &meta.MongoSpec{
			Collection: "admins",
			Route:      meta.RouteSpec{Type: "manager"},
		},
		Fields: map[string]meta.FieldSpec{
			"AdminId": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Keys: []meta.IndexKeySpec{{Order: "1"}}}}}},
		},
	}); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}

	if err := runtime.initializeMongoIndexes(context.Background(), txstorageconfig.NewRouteTableConfig(nil), registry); err != nil {
		t.Fatalf("initializeMongoIndexes returned error for unselected route: %v", err)
	}
}
