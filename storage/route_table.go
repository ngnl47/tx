package storage

import (
	"context"
	"fmt"
	"math"
)

// RouteRule 描述一个 routeType 在指定 routeBy 区间内的分片规则。
type RouteRule struct {
	Min    int64    `json:"min"`
	Max    int64    `json:"max"`
	Shards []string `json:"shards"`
}

// RouteTable 按 routeType 和 shardKey 选择 Mongo 端点名称。
type RouteTable struct {
	routes map[string][]RouteRule
}

// NewRouteTable 创建通用路由表。规则会被复制，避免调用方后续修改影响运行时路由。
func NewRouteTable(routes map[string][]RouteRule) *RouteTable {
	table := &RouteTable{
		routes: make(map[string][]RouteRule, len(routes)),
	}
	for routeType, rules := range routes {
		copied := make([]RouteRule, len(rules))
		for index, rule := range rules {
			copied[index] = rule
			copied[index].Shards = append([]string(nil), rule.Shards...)
		}
		table.routes[routeType] = copied
	}
	return table
}

// Route 按 routeType 和 shardKey 选择端点名称。字符串 shardKey 使用稳定 FNV-1a 32 位哈希。
func (t *RouteTable) Route(_ context.Context, routeType string, shardKey any) (string, error) {
	if t == nil {
		return "", fmt.Errorf("storage: route table is not configured")
	}
	rules, ok := t.routes[routeType]
	if !ok || len(rules) == 0 {
		return "", fmt.Errorf("storage: route %q is not configured", routeType)
	}
	routeBy, err := routeKeyValue(shardKey)
	if err != nil {
		return "", fmt.Errorf("storage: route %q shardKey %v cannot be routed: %w", routeType, shardKey, err)
	}
	for _, rule := range rules {
		min, max := normalizeRouteRange(rule)
		if len(rule.Shards) == 0 {
			return "", fmt.Errorf("storage: route %q has empty shards", routeType)
		}
		if routeBy < min || routeBy > max {
			continue
		}
		return rule.Shards[routeBy%int64(len(rule.Shards))], nil
	}
	return "", fmt.Errorf("storage: route %q not found for shardKey %v", routeType, shardKey)
}

// RouteMongo 让 RouteTable 可直接作为 Mongo cluster router 使用。
func (t *RouteTable) RouteMongo(ctx context.Context, routeType string, shardKey any) (string, error) {
	return t.Route(ctx, routeType, shardKey)
}

func normalizeRouteRange(rule RouteRule) (int64, int64) {
	min := rule.Min
	if min <= 0 {
		min = 1
	}
	max := rule.Max
	if max <= 0 {
		max = math.MaxInt64
	}
	return min, max
}

func routeKeyValue(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return 0, fmt.Errorf("uint overflows int64")
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 overflows int64")
		}
		return int64(v), nil
	case string:
		return int64(stringRouteHash(v)), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", value)
	}
}

func stringRouteHash(value string) uint32 {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	hash := offset32
	for i := 0; i < len(value); i++ {
		hash ^= uint32(value[i])
		hash *= prime32
	}
	if hash == 0 {
		return 1
	}
	return hash
}
