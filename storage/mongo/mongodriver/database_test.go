package mongodriver

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"

	"github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestMongoBackendErrorDoesNotClassifyCallerContextError(t *testing.T) {
	for _, contextErr := range []error{context.Canceled, context.DeadlineExceeded} {
		err := mongoBackendError("find one", contextErr)
		if !errors.Is(err, contextErr) {
			t.Fatalf("mongoBackendError = %v, want %v", err, contextErr)
		}
		if backend, ok := storage.BackendFromError(err); ok {
			t.Fatalf("BackendFromError = %v, true, want unclassified caller context error", backend)
		}
	}
}

func TestMongoBackendErrorClassifiesDriverTimeoutOrNetworkFailure(t *testing.T) {
	for _, driverErr := range []error{
		mongo.CommandError{Code: 50, Message: "operation exceeded time limit"},
		mongo.CommandError{Labels: []string{"NetworkError"}, Message: "connection reset"},
	} {
		err := mongoBackendError("find one", driverErr)
		if backend, ok := storage.BackendFromError(err); !ok || backend != storage.BackendMongo {
			t.Fatalf("BackendFromError = %v, %t, want mongo, true for %v", backend, ok, driverErr)
		}
	}

	decodeErr := errors.New("invalid bson document")
	if err := mongoBackendError("decode", decodeErr); !errors.Is(err, decodeErr) {
		t.Fatalf("mongoBackendError = %v, want original decode error", err)
	} else if _, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError classified decode error: %v", err)
	}
}

func TestDatabaseReturnsErrorWhenMongoDatabaseMissing(t *testing.T) {
	db := NewDatabase(nil)

	if _, err := db.UpdateOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}, map[string]any{"$set": map[string]any{"name": "alice"}}, storage.ApplyOptions{}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("UpdateOne error = %v, want missing database error", err)
	}
	if _, err := db.ReplaceOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}, map[string]any{"name": "alice"}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("ReplaceOne error = %v, want missing database error", err)
	}
	if _, err := db.DeleteOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("DeleteOne error = %v, want missing database error", err)
	}
	if _, _, err := db.FindOne(context.Background(), "roles", txmongo.Filter{Field: "_id", Value: int64(1001)}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("FindOne error = %v, want missing database error", err)
	}
	if _, _, err := db.FindMany(context.Background(), "roles", txmongo.ValuesFilter{Field: "_id", Values: []any{int64(1001)}}); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("FindMany error = %v, want missing database error", err)
	}
	if _, err := db.NextSequence(context.Background(), "tx_sequences", "account_id"); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("NextSequence error = %v, want missing database error", err)
	}
	if err := db.EnsureIndexes(context.Background(), "roles", nil); err == nil || !strings.Contains(err.Error(), "mongo database is not configured") {
		t.Fatalf("EnsureIndexes error = %v, want missing database error", err)
	}
}

func TestDatabaseNextSequenceUsesSingleAtomicFindAndModify(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("increment", func(mt *mtest.T) {
		mt.AddMockResponses(mtest.CreateSuccessResponse(
			bson.E{Key: "lastErrorObject", Value: bson.D{
				{Key: "n", Value: int32(1)},
				{Key: "updatedExisting", Value: false},
				{Key: "upserted", Value: "account_id"},
			}},
			bson.E{Key: "value", Value: bson.D{
				{Key: "_id", Value: "account_id"},
				{Key: "value", Value: int64(1)},
			}},
		))

		value, err := NewDatabase(mt.DB).NextSequence(context.Background(), "tx_sequences", "account_id")
		if err != nil {
			t.Fatalf("NextSequence returned error: %v", err)
		}
		if value != 1 {
			t.Fatalf("NextSequence = %d, want 1", value)
		}

		event := mt.GetStartedEvent()
		if event == nil || event.CommandName != "findAndModify" {
			t.Fatalf("started event = %#v, want one findAndModify", event)
		}
		if extra := mt.GetStartedEvent(); extra != nil {
			t.Fatalf("extra started event = %#v, want a single command", extra)
		}
		if got := event.Command.Lookup("findAndModify").StringValue(); got != "tx_sequences" {
			t.Fatalf("collection = %q, want tx_sequences", got)
		}
		if !event.Command.Lookup("upsert").Boolean() || !event.Command.Lookup("new").Boolean() {
			t.Fatalf("command = %v, want upsert and return-after", event.Command)
		}
		query := event.Command.Lookup("query").Document()
		if got := query.Lookup("_id").StringValue(); got != "account_id" {
			t.Fatalf("sequence name = %q, want account_id", got)
		}
		update := event.Command.Lookup("update").Document()
		increment := update.Lookup("$inc").Document()
		if got := increment.Lookup("value").Int64(); got != 1 {
			t.Fatalf("increment = %d, want 1", got)
		}
	})
}

func TestMongoFilterDocumentIncludesRecordKeyAndFenceConditions(t *testing.T) {
	filter, err := mongoFilterDocument(txmongo.Filter{
		Field: "_id",
		Value: "world-1/7",
		Conditions: []txmongo.Equality{
			{Field: "checkpointGeneration", Value: uint64(3)},
		},
	})
	if err != nil {
		t.Fatalf("mongoFilterDocument returned error: %v", err)
	}
	if !reflect.DeepEqual(filter, bson.M{"_id": "world-1/7", "checkpointGeneration": uint64(3)}) {
		t.Fatalf("filter = %#v, want record key and generation fence", filter)
	}
}

func TestMongoFilterDocumentRejectsDuplicateCondition(t *testing.T) {
	_, err := mongoFilterDocument(txmongo.Filter{
		Field: "_id",
		Value: "world-1/7",
		Conditions: []txmongo.Equality{
			{Field: "_id", Value: "other"},
		},
	})
	if err == nil {
		t.Fatal("mongoFilterDocument error = nil, want duplicate condition error")
	}
}

func TestMongoIndexModelsPreserveKeyTypesAndOptions(t *testing.T) {
	expireAfter := int64(60)
	models, err := mongoIndexModels([]txmongo.IndexModel{{
		Name:   "search",
		Keys:   []txmongo.IndexKey{{Path: "createdAt", Order: "-1"}, {Path: "content", Order: "text"}},
		Unique: true,
		Weights: []txmongo.IndexWeight{
			{Path: "content", Weight: 8},
		},
		ExpireAfter: &expireAfter,
	}})
	if err != nil {
		t.Fatalf("mongoIndexModels returned error: %v", err)
	}
	if len(models) != 1 {
		t.Fatalf("models = %#v, want one model", models)
	}
	keys, ok := models[0].Keys.(bson.D)
	if !ok || !reflect.DeepEqual(keys, bson.D{{Key: "createdAt", Value: -1}, {Key: "content", Value: "text"}}) {
		t.Fatalf("keys = %#v, want typed Mongo keys", models[0].Keys)
	}
	if models[0].Options.Name == nil || *models[0].Options.Name != "search" || models[0].Options.Unique == nil || !*models[0].Options.Unique {
		t.Fatalf("options = %#v, want named unique index", models[0].Options)
	}
	if models[0].Options.ExpireAfterSeconds == nil || *models[0].Options.ExpireAfterSeconds != 60 {
		t.Fatalf("expireAfter = %#v, want 60", models[0].Options.ExpireAfterSeconds)
	}
	weights, ok := models[0].Options.Weights.(bson.D)
	if !ok || !reflect.DeepEqual(weights, bson.D{{Key: "content", Value: 8}}) {
		t.Fatalf("weights = %#v, want content weight", models[0].Options.Weights)
	}
}

func TestMongoIndexModelsRejectExpireAfterOverflow(t *testing.T) {
	expireAfter := int64(math.MaxInt32) + 1
	_, err := mongoIndexModels([]txmongo.IndexModel{{
		Name:        "ttl",
		Keys:        []txmongo.IndexKey{{Path: "expireAt", Order: "1"}},
		ExpireAfter: &expireAfter,
	}})
	if err == nil {
		t.Fatal("mongoIndexModels returned nil error, want expireAfter overflow")
	}
}

func TestMongoIndexModelsOmitDisabledBooleanOptions(t *testing.T) {
	models, err := mongoIndexModels([]txmongo.IndexModel{{
		Name: "plain",
		Keys: []txmongo.IndexKey{{Path: "name", Order: "1"}},
	}})
	if err != nil {
		t.Fatalf("mongoIndexModels returned error: %v", err)
	}
	if models[0].Options.Unique != nil {
		t.Fatalf("unique = %#v, want omitted option", models[0].Options.Unique)
	}
}

func TestProjectionWithRequiredFieldIncludesFilterField(t *testing.T) {
	fields := []string{"name"}
	projection := projectionWithRequiredField(fields, "roleId")

	if len(projection) != 2 || projection["name"] != 1 || projection["roleId"] != 1 {
		t.Fatalf("projection = %#v, want name and roleId", projection)
	}
	if len(fields) != 1 || fields[0] != "name" {
		t.Fatalf("fields = %#v, want original caller fields unchanged", fields)
	}
}

func TestProjectionWithRequiredFieldKeepsFullDocumentProjectionUnset(t *testing.T) {
	if projection := projectionWithRequiredField(nil, "roleId"); projection != nil {
		t.Fatalf("projection = %#v, want nil for full document", projection)
	}
}

func TestMongoValueIdentityMatchesBinaryInputAndDecodedValue(t *testing.T) {
	input, err := mongoValueIdentity([]byte{1, 2, 3})
	if err != nil {
		t.Fatalf("mongoValueIdentity input error: %v", err)
	}
	decoded, err := mongoValueIdentity(primitive.Binary{Subtype: 0, Data: []byte{1, 2, 3}})
	if err != nil {
		t.Fatalf("mongoValueIdentity decoded error: %v", err)
	}
	if input != decoded {
		t.Fatalf("binary identities differ: input=%q decoded=%q", input, decoded)
	}
}
