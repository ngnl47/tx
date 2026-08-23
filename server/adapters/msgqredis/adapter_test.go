package msgqredis

import (
	"context"
	"testing"
	"time"

	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestNewRoutesSessionLookupThroughConfiguredRedisRoute(t *testing.T) {
	client := &sessionClient{}
	routes := &recordingRoutes{client: client}
	store, err := New(routes, Options{RouteType: "role", Prefix: "tx:test", TTL: time.Minute})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if _, found, err := store.Find(context.Background(), 47001); err != nil || found {
		t.Fatalf("Find = found %v, err %v", found, err)
	}
	if routes.routeType != "role" || routes.shardKey != int64(47001) {
		t.Fatalf("route = (%q, %#v)", routes.routeType, routes.shardKey)
	}
}

type recordingRoutes struct {
	client    txredis.ScriptClient
	routeType string
	shardKey  any
}

func (routes *recordingRoutes) ScriptClientFor(_ context.Context, routeType string, shardKey any) (txredis.ScriptClient, error) {
	routes.routeType = routeType
	routes.shardKey = shardKey
	return routes.client, nil
}

type sessionClient struct{}

func (*sessionClient) Get(context.Context, string) (string, bool, error) { return "", false, nil }
func (*sessionClient) Eval(context.Context, string, []string, ...any) (string, error) {
	return "", nil
}
