package runtime

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/modules/entitycache"
)

// EntityCache 返回当前应用的唯一实体运行时，未装配时不回退普通读取。
func EntityCache(ctx context.Context) (*entitycache.Runtime, error) {
	if runtime := entitycache.FromContext(ctx); runtime != nil {
		return runtime, nil
	}
	if provider, ok := ProviderFrom(ctx).(interface {
		EntityCache() (*entitycache.Runtime, error)
	}); ok {
		return provider.EntityCache()
	}
	return nil, fmt.Errorf("runtime: entity cache is not bound")
}
