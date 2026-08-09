package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// RouteTableConfig 表示 redis.yml 或 mongodb.yml 中的 routeType 路由表。
type RouteTableConfig struct {
	Routes map[string]RouteTarget `yaml:",inline" json:"routes"`
}

// RouteTarget 表示一个 routeType 的路由目标，支持标量、字符串数组和规则数组。
type RouteTarget struct {
	Rules []RouteRule `json:"rules"`
}

// RouteRule 描述一个 routeBy 区间内可用的 endpoint 列表。
type RouteRule struct {
	Min    int64    `yaml:"min" json:"min"`
	Max    int64    `yaml:"max" json:"max"`
	Shards []string `yaml:"shards" json:"shards"`
}

func NewRouteTableConfig(routes map[string]RouteTarget) RouteTableConfig {
	copied := make(map[string]RouteTarget, len(routes))
	for routeType, target := range routes {
		copied[routeType] = target.clone()
	}
	return RouteTableConfig{Routes: copied}
}

func (c *RouteTableConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("storage config: route table must be a mapping")
	}
	routes := make(map[string]RouteTarget, len(value.Content)/2)
	for i := 0; i < len(value.Content); i += 2 {
		key := value.Content[i]
		val := value.Content[i+1]
		if key.Kind != yaml.ScalarNode {
			return fmt.Errorf("storage config: route type must be a scalar")
		}
		routeType := key.Value
		if strings.TrimSpace(routeType) == "" {
			return fmt.Errorf("storage config: route type must not be blank")
		}
		if routeType != strings.TrimSpace(routeType) {
			return fmt.Errorf("storage config: route type %q must not contain surrounding whitespace", routeType)
		}
		if _, exists := routes[routeType]; exists {
			return fmt.Errorf("storage config: duplicate route type %q", routeType)
		}
		var target RouteTarget
		if err := target.UnmarshalYAML(val); err != nil {
			return fmt.Errorf("storage config: route %q: %w", routeType, err)
		}
		routes[routeType] = target
	}
	c.Routes = routes
	return nil
}

func (t *RouteTarget) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode, yaml.AliasNode:
		var endpoint string
		if err := value.Decode(&endpoint); err != nil {
			return err
		}
		if endpoint == "" {
			return fmt.Errorf("endpoint is empty")
		}
		t.Rules = []RouteRule{{Min: 1, Shards: []string{endpoint}}}
		return nil
	case yaml.SequenceNode:
		return t.unmarshalSequence(value)
	default:
		return fmt.Errorf("unsupported route target kind %d", value.Kind)
	}
}

func (t *RouteTarget) unmarshalSequence(value *yaml.Node) error {
	if len(value.Content) == 0 {
		return fmt.Errorf("route target sequence is empty")
	}
	if isEndpointNode(value.Content[0]) {
		shards := make([]string, 0, len(value.Content))
		for _, item := range value.Content {
			if !isEndpointNode(item) {
				return fmt.Errorf("endpoint sequence cannot mix scalar and rule items")
			}
			var endpoint string
			if err := item.Decode(&endpoint); err != nil {
				return err
			}
			if endpoint == "" {
				return fmt.Errorf("endpoint is empty")
			}
			shards = append(shards, endpoint)
		}
		t.Rules = []RouteRule{{Min: 1, Shards: shards}}
		return nil
	}

	rules := make([]RouteRule, 0, len(value.Content))
	for _, item := range value.Content {
		if item.Kind != yaml.MappingNode {
			return fmt.Errorf("rule sequence cannot mix rule and scalar items")
		}
		if err := validateRuleFields(item); err != nil {
			return err
		}
		var rule RouteRule
		if err := item.Decode(&rule); err != nil {
			return err
		}
		rules = append(rules, rule)
	}
	t.Rules = rules
	return nil
}

func validateRuleFields(value *yaml.Node) error {
	for i := 0; i < len(value.Content); i += 2 {
		key := value.Content[i]
		switch key.Value {
		case "min", "max", "shards":
		default:
			return fmt.Errorf("unknown route rule field %q", key.Value)
		}
	}
	return nil
}

func isEndpointNode(value *yaml.Node) bool {
	return value.Kind == yaml.ScalarNode || value.Kind == yaml.AliasNode
}

func (t RouteTarget) clone() RouteTarget {
	rules := make([]RouteRule, len(t.Rules))
	for i, rule := range t.Rules {
		rules[i] = RouteRule{
			Min:    rule.Min,
			Max:    rule.Max,
			Shards: append([]string(nil), rule.Shards...),
		}
	}
	return RouteTarget{Rules: rules}
}
