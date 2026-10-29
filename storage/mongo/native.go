package mongo

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/meta"
	driver "go.mongodb.org/mongo-driver/mongo"
)

// DatabaseProvider 按既有存储路由取得原生数据库，连接生命周期归宿主。
type DatabaseProvider interface {
	MongoDatabase(context.Context, string, any) (*driver.Database, error)
}

// MongoDatabase 复用选中分片的数据库，不建立新连接。
func (c *Cluster) MongoDatabase(ctx context.Context, route string, key any) (*driver.Database, error) {
	adapter, err := c.adapterForRoute(ctx, route, key)
	if err != nil {
		return nil, err
	}
	client, err := adapter.clientFor()
	if err != nil {
		return nil, err
	}
	native, ok := client.(interface{ NativeDatabase() *driver.Database })
	if !ok || native.NativeDatabase() == nil {
		return nil, fmt.Errorf("mongo: route %q does not expose a native database", route)
	}
	return native.NativeDatabase(), nil
}

// Collection 从 GD 元数据选择集合；原生查询不读取 EntityCache，写入不加入 Procedure。
// 调用者负责游标关闭和操作期限，不得关闭共享 Client。
func Collection(ctx context.Context, databases DatabaseProvider, spec meta.TypeSpec, key any) (*driver.Collection, error) {
	if spec.Mongo == nil {
		return nil, fmt.Errorf("mongo: type %q has no Mongo metadata", spec.Type.Ref.Name)
	}
	db, err := databases.MongoDatabase(ctx, mongoRouteType(spec), key)
	if err != nil {
		return nil, err
	}
	return db.Collection(mongoCollection(spec)), nil
}
