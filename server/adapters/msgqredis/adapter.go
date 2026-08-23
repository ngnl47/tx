// Package msgqredis 将已经装配的 Redis 路由适配为 msgq 会话存储。
package msgqredis

import (
	"context"
	"fmt"
	"strings"
	"time"

	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txredis "github.com/ngnl47/tx/storage/redis"
)

// Options 配置短期会话 owner 使用的 Redis 命名空间。
type Options struct {
	RouteType string
	Prefix    string
	TTL       time.Duration
}

// New 复用已经装配的 Redis 路由表，不创建或持有 Redis client 与连接池。
func New(routes txredis.ScriptRoutes, options Options) (txmsgq.SessionStore, error) {
	if routes == nil {
		return nil, fmt.Errorf("msgq redis adapter: routes are required")
	}
	if strings.TrimSpace(options.RouteType) == "" || strings.TrimSpace(options.Prefix) == "" || options.TTL <= 0 {
		return nil, fmt.Errorf("msgq redis adapter: invalid options")
	}
	return txmsgq.NewRoutedRedisSessionStore(resolver{routes: routes, routeType: options.RouteType}, txmsgq.SessionRedisOptions{
		Prefix: options.Prefix,
		TTL:    options.TTL,
	})
}

type resolver struct {
	routes    txredis.ScriptRoutes
	routeType string
}

func (resolver resolver) ResolveSessionRedis(ctx context.Context, roleID int64) (txmsgq.SessionRedisClient, error) {
	client, err := resolver.routes.ScriptClientFor(ctx, resolver.routeType, roleID)
	if err != nil {
		return nil, err
	}
	resolved, ok := client.(txmsgq.SessionRedisClient)
	if !ok {
		return nil, fmt.Errorf("msgq redis adapter: client does not support session commands")
	}
	return resolved, nil
}
