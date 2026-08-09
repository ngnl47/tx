package config

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/ngnl47/tx/storage"
)

// Endpoints 返回配置中出现过的 endpoint URI 集合。key 和 value 都是 URI，便于运行时按 URI 复用客户端。
func (c RouteTableConfig) Endpoints() map[string]string {
	endpoints := map[string]string{}
	for _, target := range c.Routes {
		for _, rule := range target.Rules {
			for _, shard := range rule.Shards {
				if shard != "" {
					endpoints[shard] = shard
				}
			}
		}
	}
	return endpoints
}

// RouteTable 将配置归一化为运行时通用路由表。
func (c RouteTableConfig) RouteTable() (*storage.RouteTable, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	routes := make(map[string][]storage.RouteRule, len(c.Routes))
	for routeType, target := range c.Routes {
		rules := make([]storage.RouteRule, len(target.Rules))
		for i, rule := range target.Rules {
			rules[i] = storage.RouteRule{
				Min:    normalizedMin(rule.Min),
				Max:    normalizedMax(rule.Max),
				Shards: append([]string(nil), rule.Shards...),
			}
		}
		routes[routeType] = rules
	}
	return storage.NewRouteTable(routes), nil
}

// Validate 校验路由配置是否可以构造运行时路由表。
func (c RouteTableConfig) Validate() error {
	routeTypes := make([]string, 0, len(c.Routes))
	for routeType := range c.Routes {
		routeTypes = append(routeTypes, routeType)
	}
	sort.Strings(routeTypes)
	for _, routeType := range routeTypes {
		if strings.TrimSpace(routeType) == "" {
			return fmt.Errorf("storage config: route type must not be blank")
		}
		if routeType != strings.TrimSpace(routeType) {
			return fmt.Errorf("storage config: route type %q must not contain surrounding whitespace", routeType)
		}
		target := c.Routes[routeType]
		if len(target.Rules) == 0 {
			return fmt.Errorf("storage config: route %q has no rules", routeType)
		}
		for i, rule := range target.Rules {
			if len(rule.Shards) == 0 {
				return fmt.Errorf("storage config: route %q rule %d has empty shards", routeType, i)
			}
			min := normalizedMin(rule.Min)
			max := normalizedMax(rule.Max)
			if max < min {
				return fmt.Errorf("storage config: route %q rule %d max %d is smaller than min %d", routeType, i, max, min)
			}
			for j, shard := range rule.Shards {
				if shard == "" {
					return fmt.Errorf("storage config: route %q rule %d shard %d is empty", routeType, i, j)
				}
			}
		}
	}
	return nil
}

func normalizedMin(value int64) int64 {
	if value <= 0 {
		return 1
	}
	return value
}

func normalizedMax(value int64) int64 {
	if value <= 0 {
		return math.MaxInt64
	}
	return value
}
