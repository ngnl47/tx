package server

import (
	"strings"
	"testing"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	storageconfig "github.com/ngnl47/tx/storage/config"
)

func TestWALCModValidatesSelectedConfiguration(t *testing.T) {
	routes := storageconfig.NewRouteTableConfig(map[string]storageconfig.RouteTarget{
		"main": {Rules: []storageconfig.RouteRule{{Shards: []string{"mongodb://localhost:27017/tx"}}}},
		"wal":  {Rules: []storageconfig.RouteRule{{Shards: []string{"mongodb://localhost:27017/tx"}}}},
	})
	base := Config{
		AppID: "wal-test",
		Storage: StorageConfig{
			Redis:   routes,
			MongoDB: routes,
			WAL: WALConfig{
				Backend: WALBackendMongo,
				Mongo:   WALMongoConfig{RouteType: "wal", Collection: "tx_wal"},
			},
		},
		Runtime: RuntimeConfig{Health: HealthConfig{Interval: time.Second, Timeout: time.Second}},
	}
	if err := base.Storage.WAL.validate(base.Storage.MongoDB); err != nil {
		t.Fatalf("valid WAL config returned error: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "unsupported backend", mutate: func(config *Config) { config.Storage.WAL.Backend = "rocketmq" }, want: "unsupported"},
		{name: "missing route", mutate: func(config *Config) { config.Storage.WAL.Mongo.RouteType = "missing" }, want: "not configured"},
		{name: "blank collection", mutate: func(config *Config) { config.Storage.WAL.Mongo.Collection = " " }, want: "collection"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := base.clone()
			tt.mutate(&config)
			if err := config.Storage.WAL.validate(config.Storage.MongoDB); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestRuntimeConfigRejectsInvalidDelayTaskLeasePrecision(t *testing.T) {
	for _, lease := range []time.Duration{-time.Second, time.Nanosecond} {
		t.Run(lease.String(), func(t *testing.T) {
			config := DelayTaskConfig{Lease: lease}
			if err := config.validate(); err == nil {
				t.Fatal("DelayTaskConfig.validate error = nil, want invalid delay task lease")
			}
		})
	}
}

func TestConfigRejectsNegativeLifecycleDrainTimeout(t *testing.T) {
	config := Config{AppID: "lifecycle-test", Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: -time.Second}}}
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "drain timeout") {
		t.Fatalf("Config.Validate error = %v, want invalid drain timeout", err)
	}
}

func TestConfigCloneOwnsMutableRoutesAndEndpoints(t *testing.T) {
	routes := storageconfig.NewRouteTableConfig(map[string]storageconfig.RouteTarget{
		"main": {Rules: []storageconfig.RouteRule{{Min: 1, Shards: []string{"endpoint-1"}}}},
	})
	config := Config{
		Storage: StorageConfig{
			Redis:   routes,
			MongoDB: routes,
			WAL:     WALConfig{Backend: WALBackendMongo, Mongo: WALMongoConfig{RouteType: "main", Collection: "tx_wal"}},
		},
		Actor:     ActorConfig{Runtime: txactor.Config{Etcd: txactor.EtcdConfig{Endpoints: []string{"actor-1"}}}},
		Discovery: DiscoveryConfig{Etcd: discovery.EtcdConfig{Endpoints: []string{"discovery-1"}}},
	}
	cloned := config.clone()

	config.Storage.Redis.Routes["main"] = storageconfig.RouteTarget{}
	config.Storage.MongoDB.Routes["main"] = storageconfig.RouteTarget{}
	config.Storage.WAL.Mongo.Collection = "mutated"
	config.Actor.Runtime.Etcd.Endpoints[0] = "actor-2"
	config.Discovery.Etcd.Endpoints[0] = "discovery-2"

	if got := cloned.Storage.Redis.Routes["main"].Rules[0].Shards[0]; got != "endpoint-1" {
		t.Fatalf("cloned Redis endpoint = %q", got)
	}
	if got := cloned.Storage.MongoDB.Routes["main"].Rules[0].Shards[0]; got != "endpoint-1" {
		t.Fatalf("cloned Mongo endpoint = %q", got)
	}
	if got := cloned.Storage.WAL.Mongo.Collection; got != "tx_wal" {
		t.Fatalf("cloned WAL collection = %q", got)
	}
	if got := cloned.Actor.Runtime.Etcd.Endpoints[0]; got != "actor-1" {
		t.Fatalf("cloned actor endpoint = %q", got)
	}
	if got := cloned.Discovery.Etcd.Endpoints[0]; got != "discovery-1" {
		t.Fatalf("cloned discovery endpoint = %q", got)
	}
}
