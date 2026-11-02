package runtime

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/modules/entitycache"
)

// EntityCache 返回当前应用缓存的受限能力，生命周期归宿主，未装配时不回退普通读取。
func EntityCache(ctx context.Context) (*entitycache.Access, error) {
	if runtime := entitycache.FromContext(ctx); runtime != nil {
		return runtime.Access(), nil
	}
	if provider, ok := ProviderFrom(ctx).(interface {
		EntityCache() (*entitycache.Access, error)
	}); ok {
		return provider.EntityCache()
	}
	return nil, fmt.Errorf("runtime: entity cache is not bound")
}
