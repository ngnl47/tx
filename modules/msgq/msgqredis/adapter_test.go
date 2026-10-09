package msgqredis

import (
	"context"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func TestNewSelectsNamedRedisStore(t *testing.T) {
	client := &sessionClient{}
	routes := &recordingRoutes{client: client}
	store, err := New(routes, Options{StoreName: "role", Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if _, found, err := store.Find(context.Background(), 47001); err != nil || found {
		t.Fatalf("Find = found %v, err %v", found, err)
	}
	if routes.routeType != "role" {
		t.Fatalf("store = %q", routes.routeType)
	}
}

type recordingRoutes struct {
	client    redis.Cmdable
	routeType string
}

func (routes *recordingRoutes) Client(routeType string) (redis.Cmdable, error) {
	routes.routeType = routeType
	return routes.client, nil
}

type sessionClient struct{ redis.Cmdable }

func (*sessionClient) Get(context.Context, string) *redis.StringCmd {
	return redis.NewStringResult("", redis.Nil)
}
