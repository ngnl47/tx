package mongodriver

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var errMongoDatabaseNotConfigured = errors.New("mongo database is not configured")

// Database 将官方 MongoDB driver 的 Database 适配为 tx Mongo runtime 使用的最小接口。
type Database struct {
	db *mongo.Database
}

func NewDatabase(db *mongo.Database) Database {
	return Database{db: db}
}

func (d Database) UpdateByID(ctx context.Context, collection string, id any, update map[string]any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).UpdateByID(
		ctx,
		id,
		update,
		options.Update().SetUpsert(true),
	)
	return err
}

func (d Database) UpdateByField(ctx context.Context, collection string, field string, value any, update map[string]any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).UpdateOne(
		ctx,
		bson.M{field: value},
		update,
		options.Update().SetUpsert(true),
	)
	return err
}

func (d Database) ReplaceByID(ctx context.Context, collection string, id any, doc map[string]any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).ReplaceOne(
		ctx,
		bson.M{"_id": id},
		doc,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (d Database) ReplaceByField(ctx context.Context, collection string, field string, value any, doc map[string]any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).ReplaceOne(
		ctx,
		bson.M{field: value},
		doc,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (d Database) DeleteByID(ctx context.Context, collection string, id any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).DeleteOne(ctx, bson.M{"_id": id})
	return err
}

func (d Database) DeleteByField(ctx context.Context, collection string, field string, value any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := d.db.Collection(collection).DeleteOne(ctx, bson.M{field: value})
	return err
}

func (d Database) FindByID(ctx context.Context, collection string, id any, fields ...string) (map[string]any, bool, error) {
	if d.db == nil {
		return nil, false, errMongoDatabaseNotConfigured
	}
	var doc map[string]any
	err := d.db.Collection(collection).FindOne(ctx, bson.M{"_id": id}, options.FindOne().SetProjection(projection(fields...))).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return doc, true, nil
}

func (d Database) FindByField(ctx context.Context, collection string, field string, value any, fields ...string) (map[string]any, bool, error) {
	if d.db == nil {
		return nil, false, errMongoDatabaseNotConfigured
	}
	var doc map[string]any
	err := d.db.Collection(collection).FindOne(ctx, bson.M{field: value}, options.FindOne().SetProjection(projection(fields...))).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return doc, true, nil
}

func (d Database) FindManyByID(ctx context.Context, collection string, ids []any, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(ids))
	exists := make([]bool, len(ids))
	if len(ids) == 0 {
		return docs, exists, nil
	}
	if d.db == nil {
		return nil, nil, errMongoDatabaseNotConfigured
	}
	cursor, err := d.db.Collection(collection).Find(ctx, bson.M{"_id": bson.M{"$in": ids}}, options.Find().SetProjection(projection(fields...)))
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(ctx)

	positions := make(map[any][]int, len(ids))
	for i, id := range ids {
		positions[id] = append(positions[id], i)
	}
	for cursor.Next(ctx) {
		var doc map[string]any
		if err := cursor.Decode(&doc); err != nil {
			return nil, nil, err
		}
		for _, index := range positions[doc["_id"]] {
			docs[index] = doc
			exists[index] = true
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, nil, err
	}
	return docs, exists, nil
}

func (d Database) FindManyByField(ctx context.Context, collection string, field string, values []any, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(values))
	exists := make([]bool, len(values))
	if len(values) == 0 {
		return docs, exists, nil
	}
	if d.db == nil {
		return nil, nil, errMongoDatabaseNotConfigured
	}
	cursor, err := d.db.Collection(collection).Find(ctx, bson.M{field: bson.M{"$in": values}}, options.Find().SetProjection(projection(fields...)))
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(ctx)

	positions := make(map[any][]int, len(values))
	for i, value := range values {
		positions[value] = append(positions[value], i)
	}
	for cursor.Next(ctx) {
		var doc map[string]any
		if err := cursor.Decode(&doc); err != nil {
			return nil, nil, err
		}
		for _, index := range positions[doc[field]] {
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
