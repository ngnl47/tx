//go:build !txbench

package runtime

import (
	"github.com/ngnl47/tx/storage"
	redis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/event"
)

// 普通构建不安装采集钩子，也不分配统计状态。
type ioStats struct{}

func (*ioStats) attachRedis(*redis.Client)           {}
func (*ioStats) mongoMonitor() *event.CommandMonitor { return nil }

// IOStats 在普通构建中不采集数据；压测构建需显式使用 txbench 标签。
func (*Runtime) IOStats() []storage.IOStat { return nil }
