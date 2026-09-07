package mongodriver

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestCounterReserveMongoIntegration(t *testing.T) {
	uri := os.Getenv("TX_MONGO_URI")
	if uri == "" {
		t.Skip("set TX_MONGO_URI to run Mongo counter integration")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	db := client.Database(fmt.Sprintf("tx_counter_test_%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := db.Drop(cleanup); err != nil {
			t.Errorf("drop isolated counter test database: %v", err)
		}
	})
	stores := []Database{NewDatabase(db), NewDatabase(db)}
	const count = 16
	highs := make(chan int64, count)
	errs := make(chan error, count)
	var group sync.WaitGroup
	for index := range count {
		group.Go(func() {
			high, err := stores[index%2].Reserve(ctx, "counters", "concurrent", 100)
			if err != nil {
				errs <- err
				return
			}
			highs <- high
		})
	}
	group.Wait()
	close(highs)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Reserve: %v", err)
	}
	seen := make(map[int64]bool)
	for high := range highs {
		if high < 100 || high > count*100 || high%100 != 0 || seen[high] {
			t.Fatalf("invalid or repeated upper bound %d", high)
		}
		seen[high] = true
	}
	if len(seen) != count {
		t.Fatalf("reserved %d intervals, want %d", len(seen), count)
	}
	if _, err := db.Collection("counters").InsertOne(ctx, bson.M{"_id": "limit", "value": int64(math.MaxInt64 - 10)}); err != nil {
		t.Fatal(err)
	}
	if high, err := stores[0].Reserve(ctx, "counters", "limit", 10); err != nil || high != math.MaxInt64 {
		t.Fatalf("reserve last interval = %d, %v", high, err)
	}
	if high, err := stores[1].Reserve(ctx, "counters", "limit", 1); err == nil || high != 0 {
		t.Fatalf("overflow reservation = %d, %v", high, err)
	}
	var saved struct {
		Value int64 `bson:"value"`
	}
	if err := db.Collection("counters").FindOne(ctx, bson.M{"_id": "limit"}).Decode(&saved); err != nil || saved.Value != math.MaxInt64 {
		t.Fatalf("overflow changed counter: %d, %v", saved.Value, err)
	}
}
