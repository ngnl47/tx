// Package msgqredis 将已经装配的具名 Redis 连接适配为 msgq 会话存储。
package msgqredis

import (
	"fmt"
	"strings"
	"time"

	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
)

// Options 配置短期会话 owner 使用的 Redis 命名空间。
type Options struct {
	StoreName string
	Prefix    string
	TTL       time.Duration
}

// New 复用已经装配的 Redis 客户端，不创建或持有 Redis client 与连接池。
func New(clients txgoredis.ClientProvider, options Options) (txmsgq.SessionStore, error) {
	if clients == nil {
		return nil, fmt.Errorf("msgq redis adapter: clients are required")
	}
	if strings.TrimSpace(options.StoreName) == "" || strings.TrimSpace(options.Prefix) == "" || options.TTL <= 0 {
		return nil, fmt.Errorf("msgq redis adapter: invalid options")
	}
	client, err := clients.Client(options.StoreName)
	if err != nil {
		return nil, err
	}
	return txmsgq.NewRedisSessionStore(txgoredis.NewClient(client), txmsgq.SessionRedisOptions{Prefix: options.Prefix, TTL: options.TTL})
}
