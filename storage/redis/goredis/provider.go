package goredis

import redis "github.com/redis/go-redis/v9"

// ClientProvider 按存储名称提供共享命令接口；创建和关闭连接仍由运行时负责。
type ClientProvider interface {
	Client(name string) (redis.Cmdable, error)
}
