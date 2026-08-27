package runtime

import (
	"context"
	"testing"

	storageconfig "github.com/ngnl47/tx/storage/config"
	"github.com/ngnl47/tx/wal"
	walmongo "github.com/ngnl47/tx/wal/mongo"
)

type indexingWALClient struct {
	collections []string
}

func (client *indexingWALClient) EnsureIndexes(_ context.Context, collection string) error {
	client.collections = append(client.collections, collection)
	return nil
}

func (*indexingWALClient) Insert(context.Context, string, wal.Record) error { return nil }
func (*indexingWALClient) Load(context.Context, string, wal.Stream, uint64) (wal.Record, bool, error) {
	return wal.Record{}, false, nil
}
func (*indexingWALClient) Read(context.Context, string, wal.Stream, uint64, int) ([]wal.Record, error) {
	return nil, nil
}
func (*indexingWALClient) Trim(context.Context, string, wal.Stream, uint64) error { return nil }

func TestBuildWALOnlyIndexesEndpointsUsedByWALRoute(t *testing.T) {
	routes := storageconfig.NewRouteTableConfig(map[string]storageconfig.RouteTarget{
		"main": {Rules: []storageconfig.RouteRule{{Shards: []string{"mongo-main"}}}},
		"wal":  {Rules: []storageconfig.RouteRule{{Shards: []string{"mongo-wal"}}}},
	})
	mainClient := &indexingWALClient{}
	walClient := &indexingWALClient{}
	store, err := buildWAL(context.Background(), WALConfig{
		Backend: WALBackendMongo,
		Mongo:   WALMongoConfig{RouteType: "wal", Collection: "tx_wal"},
	}, routes, map[string]walmongo.Client{
		"mongo-main": mainClient,
		"mongo-wal":  walClient,
	})
	if err != nil {
		t.Fatalf("buildWAL returned error: %v", err)
	}
	if store == nil {
		t.Fatal("buildWAL returned nil store")
	}
	if len(mainClient.collections) != 0 {
		t.Fatalf("main endpoint WAL indexes = %#v, want none", mainClient.collections)
	}
	if len(walClient.collections) != 1 || walClient.collections[0] != "tx_wal" {
		t.Fatalf("WAL endpoint indexes = %#v, want tx_wal", walClient.collections)
	}
}
