package tests

import (
	"context"
	"reflect"
	"testing"

	storageconfig "github.com/ngnl47/tx/storage/config"
	"gopkg.in/yaml.v3"
)

func TestStorageConfigRouteTableFromYAMLShapes(t *testing.T) {
	var cfg storageconfig.RouteTableConfig
	err := yaml.Unmarshal([]byte(`
main: "redis://main"
match:
  - "redis://match-a"
  - "redis://match-b"
role:
  - min: 1
    max: 9
    shards:
      - "redis://role-a"
      - "redis://role-b"
  - min: 10
    shards:
      - "redis://role-c"
`), &cfg)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	endpoints := cfg.Endpoints()
	wantEndpoints := map[string]string{
		"redis://main":    "redis://main",
		"redis://match-a": "redis://match-a",
		"redis://match-b": "redis://match-b",
		"redis://role-a":  "redis://role-a",
		"redis://role-b":  "redis://role-b",
		"redis://role-c":  "redis://role-c",
	}
	if !reflect.DeepEqual(endpoints, wantEndpoints) {
		t.Fatalf("Endpoints = %#v, want %#v", endpoints, wantEndpoints)
	}

	table, err := cfg.RouteTable()
	if err != nil {
		t.Fatalf("RouteTable returned error: %v", err)
	}
	tests := []struct {
		routeType string
		shardKey  any
		want      string
	}{
		{routeType: "main", shardKey: "global-id", want: "redis://main"},
		{routeType: "match", shardKey: int64(1), want: "redis://match-b"},
		{routeType: "match", shardKey: int64(2), want: "redis://match-a"},
		{routeType: "role", shardKey: int64(3), want: "redis://role-b"},
		{routeType: "role", shardKey: int64(11), want: "redis://role-c"},
	}
	for _, tt := range tests {
		got, err := table.Route(context.Background(), tt.routeType, tt.shardKey)
		if err != nil {
			t.Fatalf("Route(%s,%v) returned error: %v", tt.routeType, tt.shardKey, err)
		}
		if got != tt.want {
			t.Fatalf("Route(%s,%v) = %q, want %q", tt.routeType, tt.shardKey, got, tt.want)
		}
	}
}

func TestStorageConfigRouteTableSupportsYAMLAnchors(t *testing.T) {
	var cfg struct {
		Routes storageconfig.RouteTableConfig `yaml:"routes"`
	}
	err := yaml.Unmarshal([]byte(`
endpoint: &role "redis://role"
routes:
  role:
    - min: 1
      shards:
        - *role
`), &cfg)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	endpoints := cfg.Routes.Endpoints()
	if endpoints["redis://role"] != "redis://role" {
		t.Fatalf("Endpoints = %#v, want redis://role", endpoints)
	}
	table, err := cfg.Routes.RouteTable()
	if err != nil {
		t.Fatalf("RouteTable returned error: %v", err)
	}
	got, err := table.Route(context.Background(), "role", int64(1))
	if err != nil {
		t.Fatalf("Route returned error: %v", err)
	}
	if got != "redis://role" {
		t.Fatalf("Route = %q, want redis://role", got)
	}
}

func TestStorageConfigRouteTableRejectsInvalidRules(t *testing.T) {
	tests := []struct {
		name string
		cfg  storageconfig.RouteTableConfig
	}{
		{
			name: "empty shards",
			cfg: storageconfig.RouteTableConfig{
				Routes: map[string]storageconfig.RouteTarget{
					"role": {Rules: []storageconfig.RouteRule{{Min: 1}}},
				},
			},
		},
		{
			name: "max smaller than min",
			cfg: storageconfig.RouteTableConfig{
				Routes: map[string]storageconfig.RouteTarget{
					"role": {Rules: []storageconfig.RouteRule{{Min: 10, Max: 9, Shards: []string{"redis://role"}}}},
				},
			},
		},
		{
			name: "blank route type",
			cfg: storageconfig.RouteTableConfig{
				Routes: map[string]storageconfig.RouteTarget{
					"": {Rules: []storageconfig.RouteRule{{Min: 1, Shards: []string{"redis://role"}}}},
				},
			},
		},
		{
			name: "route type with surrounding whitespace",
			cfg: storageconfig.RouteTableConfig{
				Routes: map[string]storageconfig.RouteTarget{
					" role ": {Rules: []storageconfig.RouteRule{{Min: 1, Shards: []string{"redis://role"}}}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if table, err := tt.cfg.RouteTable(); err == nil {
				t.Fatalf("RouteTable = %#v, want error", table)
			}
		})
	}
}

func TestStorageConfigRouteTableRejectsInvalidYAMLShape(t *testing.T) {
	var cfg storageconfig.RouteTableConfig
	err := yaml.Unmarshal([]byte(`
role:
  enabled: true
`), &cfg)
	if err == nil {
		t.Fatalf("Unmarshal returned nil error, want invalid route shape error")
	}
}

func TestStorageConfigRouteTableRejectsUnknownRuleField(t *testing.T) {
	var cfg storageconfig.RouteTableConfig
	err := yaml.Unmarshal([]byte(`
role:
  - min: 1
    shardz:
      - "redis://role"
`), &cfg)
	if err == nil {
		t.Fatal("Unmarshal returned nil error for an unknown route rule field")
	}
}

func TestStorageConfigRouteTableRejectsInvalidRouteType(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "duplicate",
			body: `
role: redis://first
role: redis://second
`,
		},
		{
			name: "blank",
			body: `
"": redis://main
`,
		},
		{
			name: "surrounding whitespace",
			body: `
" role ": redis://main
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg storageconfig.RouteTableConfig
			if err := yaml.Unmarshal([]byte(tt.body), &cfg); err == nil {
				t.Fatal("Unmarshal returned nil error for an invalid route type")
			}
		})
	}
}
