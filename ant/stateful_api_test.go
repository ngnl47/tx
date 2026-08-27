package ant

import (
	"testing"

	"github.com/ngnl47/tx/modules/txn"
)

func TestStatefulConfigurationIsExplicit(t *testing.T) {
	config := Config{
		Runtime: RuntimeConfig{
			Transaction: TransactionConfig{Module: txn.Config{}},
		},
		Storage: StorageConfig{
			WAL: WALConfig{
				Backend: WALBackendMongo,
				Mongo:   WALMongoConfig{RouteType: "main", Collection: "wal"},
			},
		},
	}
	options := RunOptions{LogicalSlot: "zone-1"}

	if config.toFramework().Runtime.Transaction.Module != config.Runtime.Transaction.Module {
		t.Fatal("transaction configuration was not preserved")
	}
	if config.toFramework().Storage.WAL.Mongo.Collection != "wal" {
		t.Fatal("WAL configuration was not preserved")
	}
	if options.toFramework().LogicalSlot != "zone-1" {
		t.Fatal("logical slot was not preserved")
	}
}
