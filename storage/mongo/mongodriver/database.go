package mongodriver

import (
	"context"
	"errors"

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

func NewDatabase(db *mongo.Database) Database {
	return Database{db: db}
}

func (d Database) UpdateOne(ctx context.Context, collection string, filter txmongo.Filter, update map[string]any, applyOptions storage.ApplyOptions) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).UpdateOne(
		ctx,
		bson.M{filter.Field: filter.Value},
		update,
		options.Update().SetUpsert(applyOptions.Upsert),
	)
	return err
}

func (d Database) ReplaceOne(ctx context.Context, collection string, filter txmongo.Filter, doc map[string]any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).ReplaceOne(
		ctx,
		bson.M{filter.Field: filter.Value},
		doc,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (d Database) DeleteOne(ctx context.Context, collection string, filter txmongo.Filter) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).DeleteOne(ctx, bson.M{filter.Field: filter.Value})
	return err
}

func (d Database) FindOne(ctx context.Context, collection string, filter txmongo.Filter, fields ...string) (map[string]any, bool, error) {
	if d.db == nil {
		return nil, false, errMongoDatabaseNotConfigured
	}
	var doc map[string]any
	err := d.db.Collection(collection).FindOne(ctx, bson.M{filter.Field: filter.Value}, options.FindOne().SetProjection(projection(fields...))).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
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
	cursor, err := d.db.Collection(collection).Find(ctx, bson.M{filter.Field: bson.M{"$in": filter.Values}}, options.Find().SetProjection(projection(fields...)))
	if err != nil {
		return nil, nil, err
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
		for _, index := range positions[doc[filter.Field]] {
			docs[index] = doc
			exists[index] = true
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, nil, err
	}
	return docs, exists, nil
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
