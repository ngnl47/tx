package runtime

import (
	"context"

	"github.com/ngnl47/tx/meta"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDatabase 取得当前服务声明的 Mongo 原生能力，不创建连接。
func MongoDatabase(ctx context.Context, route string, key any) (*mongo.Database, error) {
	return require(ctx).MongoDatabase(ctx, route, key)
}

// MongoCollection 按 GD 元数据选择原生集合；不经过 EntityCache 或 Procedure。
// 调用者管理操作期限和游标，不得关闭宿主拥有的共享 Client。
func MongoCollection(ctx context.Context, spec meta.TypeSpec, key any) (*mongo.Collection, error) {
	return txmongo.Collection(ctx, require(ctx), spec, key)
}
