package runtime

import (
	"context"
	"fmt"
	"github.com/ngnl47/tx/meta"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDatabase 在取得句柄时校验生命周期和健康状态，复用已配置路由与连接。
// 返回后由官方驱动执行操作；其错误不会被转换为 tx 错误，也不自动加入事务或缓存。
func (s *Runtime) MongoDatabase(ctx context.Context, route string, key any) (*mongo.Database, error) {
	if s == nil || s.mongo == nil {
		return nil, ErrNotConfigured
	}
	if s.mongoClosed.Load() {
		return nil, fmt.Errorf("storage runtime: Mongo is closed")
	}
	if err := s.requireStorageMode(meta.StorageModeMongo); err != nil {
		return nil, err
	}
	return s.mongo.MongoDatabase(ctx, route, key)
}
