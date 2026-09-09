package mongodriver

import (
	"context"

	txmongo "github.com/ngnl47/tx/storage/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// InsertDocument 只创建不存在的记录，由唯一索引拒绝并发创建，不降级为覆盖。
func (d Database) InsertDocument(ctx context.Context, collection string, doc map[string]any) error {
	if d.db == nil {
		return errMongoDatabaseNotConfigured
	}
	journal := true
	_, err := d.db.Collection(collection, options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &journal})).InsertOne(ctx, doc)
	return mongoBackendError("insert versioned document", err)
}

// ReplaceOneConditional 在同一 Mongo 更新中检查版本；不允许 upsert 绕过版本冲突。
func (d Database) ReplaceOneConditional(ctx context.Context, collection string, filter txmongo.Filter, doc map[string]any) (txmongo.ReplaceResult, error) {
	if d.db == nil {
		return txmongo.ReplaceResult{}, errMongoDatabaseNotConfigured
	}
	query, err := mongoFilterDocument(filter)
	if err != nil {
		return txmongo.ReplaceResult{}, err
	}
	journal := true
	result, err := d.db.Collection(collection, options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &journal})).ReplaceOne(ctx, query, doc)
	if err != nil {
		return txmongo.ReplaceResult{}, mongoBackendError("replace versioned document", err)
	}
	return txmongo.ReplaceResult{MatchedCount: result.MatchedCount}, nil
}
