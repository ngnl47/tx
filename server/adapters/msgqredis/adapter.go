// Package msgqredis adapts assembled Redis routes to a msgq session store.
package msgqredis

import (
	"context"
	"fmt"
	"strings"
	"time"

	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txredis "github.com/ngnl47/tx/storage/redis"
)

// Options configures the Redis namespace used for short-lived session owners.
type Options struct {
	RouteType string
	Prefix    string
	TTL       time.Duration
}

// New reuses an assembled Redis routing table. It does not create or own a
// Redis client or connection pool.
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
