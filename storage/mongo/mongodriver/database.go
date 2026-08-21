package mongodriver

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

var errMongoDatabaseNotConfigured = errors.New("mongo database is not configured")

// Database 将官方 MongoDB driver 的 Database 适配为 tx Mongo runtime 使用的最小接口。
type Database struct {
	db *mongo.Database
}

var _ txmongo.Client = Database{}

func NewDatabase(db *mongo.Database) Database {
	return Database{db: db}
}

func (d Database) UpdateOne(ctx context.Context, collection string, filter txmongo.Filter, update map[string]any, applyOptions storage.ApplyOptions) (txmongo.UpdateResult, error) {
	if d.db == nil {
		return txmongo.UpdateResult{}, errMongoDatabaseNotConfigured
	}
	result, err := d.db.Collection(collection).UpdateOne(
		ctx,
		bson.M{filter.Field: filter.Value},
		update,
		options.Update().SetUpsert(applyOptions.Upsert),
	)
	if err != nil {
		return txmongo.UpdateResult{}, mongoBackendError("update one", err)
	}
	return txmongo.UpdateResult{MatchedCount: result.MatchedCount, UpsertedCount: result.UpsertedCount}, nil
}

func (d Database) ReplaceOne(ctx context.Context, collection string, filter txmongo.Filter, doc map[string]any) (txmongo.ReplaceResult, error) {
	if d.db == nil {
		return txmongo.ReplaceResult{}, errMongoDatabaseNotConfigured
	}
	result, err := d.db.Collection(collection).ReplaceOne(
		ctx,
		bson.M{filter.Field: filter.Value},
		doc,
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return txmongo.ReplaceResult{}, mongoBackendError("replace one", err)
	}
	return txmongo.ReplaceResult{MatchedCount: result.MatchedCount, UpsertedCount: result.UpsertedCount}, nil
}

func (d Database) DeleteOne(ctx context.Context, collection string, filter txmongo.Filter) (txmongo.DeleteResult, error) {
	if d.db == nil {
		return txmongo.DeleteResult{}, errMongoDatabaseNotConfigured
	}
	result, err := d.db.Collection(collection).DeleteOne(ctx, bson.M{filter.Field: filter.Value})
	if err != nil {
		return txmongo.DeleteResult{}, mongoBackendError("delete one", err)
	}
	return txmongo.DeleteResult{DeletedCount: result.DeletedCount}, nil
}

func (d Database) FindOne(ctx context.Context, collection string, filter txmongo.Filter, fields ...string) (map[string]any, bool, error) {
	if d.db == nil {
		return nil, false, errMongoDatabaseNotConfigured
	}
	result := d.db.Collection(collection).FindOne(ctx, bson.M{filter.Field: filter.Value}, options.FindOne().SetProjection(projection(fields...)))
	err := result.Err()
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, mongoBackendError("find one", err)
	}
	var doc map[string]any
	if err := result.Decode(&doc); err != nil {
		return nil, false, err
	}
	return doc, true, nil
}

func (d Database) FindMany(ctx context.Context, collection string, filter txmongo.ValuesFilter, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(filter.Values))
	exists := make([]bool, len(filter.Values))
	if len(filter.Values) == 0 {
		return docs, exists, nil
	}
	if d.db == nil {
		return nil, nil, errMongoDatabaseNotConfigured
	}
	cursor, err := d.db.Collection(collection).Find(ctx, bson.M{filter.Field: bson.M{"$in": filter.Values}}, options.Find().SetProjection(projectionWithRequiredField(fields, filter.Field)))
	if err != nil {
		return nil, nil, mongoBackendError("find many", err)
	}
	defer cursor.Close(ctx)

	positions := make(map[any][]int, len(filter.Values))
	for i, value := range filter.Values {
		positions[value] = append(positions[value], i)
	}
	for cursor.Next(ctx) {
		var doc map[string]any
		if err := cursor.Decode(&doc); err != nil {
			return nil, nil, err
		}
		value, ok := doc[filter.Field]
		if !ok {
			return nil, nil, fmt.Errorf("mongo: batch document is missing filter field %q", filter.Field)
		}
		indexes, ok := positions[value]
		if !ok {
			return nil, nil, fmt.Errorf("mongo: batch document returned unexpected filter value %v for field %q", value, filter.Field)
		}
		for _, index := range indexes {
			docs[index] = doc
			exists[index] = true
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, nil, mongoBackendError("find many cursor", err)
	}
	return docs, exists, nil
}

func mongoBackendError(operation string, err error) error {
	if err == nil || (!mongo.IsNetworkError(err) && !mongo.IsTimeout(err)) {
		return err
	}
	return storage.NewBackendError(storage.BackendMongo, operation, err)
}

func projection(fields ...string) bson.M {
	if len(fields) == 0 {
		return nil
	}
	out := bson.M{}
	for _, field := range fields {
		out[field] = 1
	}
	return out
}

func projectionWithRequiredField(fields []string, required string) bson.M {
	if len(fields) == 0 {
		return nil
	}
	projection := projection(fields...)
	if required != "" {
		projection[required] = 1
	}
	return projection
}
