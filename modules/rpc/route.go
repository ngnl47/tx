package rpc

import (
	"errors"
	"strings"

	"github.com/ngnl47/tx/modules/discovery"
)

var (
	ErrDirectoryNil     = errors.New("rpc: service directory is nil")
	ErrInvalidRoute     = errors.New("rpc: invalid route")
	ErrRouteUnavailable = errors.New("rpc: route has no available instance")
)

// RouteKind 标识消息目标的解析方式。
type RouteKind uint8

const (
	// RouteUnknown 表示尚未指定有效路由方式。
	RouteUnknown RouteKind = iota
	// RouteService 从指定服务的 running 实例中选择目标。
	RouteService
	// RouteInstance 精确指向一个已知实例。
	RouteInstance
)

// Route 保存传输层路由元数据。
type Route struct {
	// Kind 决定使用 Service 还是 InstanceID 解析目标。
	Kind RouteKind
	// Service 是 RouteService 使用的稳定服务名。
	Service string
	// InstanceID 是 RouteInstance 使用的精确实例身份。
	InstanceID string
}

// ServiceRoute 创建一个从 running 服务副本中选择目标的路由。
func ServiceRoute(service string) Route {
	return Route{Kind: RouteService, Service: service}
}

// InstanceRoute 创建一个指向已知 running 或 stopping 实例的路由。
func InstanceRoute(instanceID string) Route {
	return Route{Kind: RouteInstance, InstanceID: instanceID}
}

func (r Route) Validate() error {
	switch r.Kind {
	case RouteService:
		if validRouteValue(r.Service) {
			return nil
		}
	case RouteInstance:
		if validRouteValue(r.InstanceID) {
			return nil
		}
	}
	return ErrInvalidRoute
}

// Resolve 将服务路由解析为 running 副本，将精确路由解析为指定实例。
func Resolve(directory discovery.Directory, route Route) (discovery.Instance, error) {
	if directory == nil {
		return discovery.Instance{}, ErrDirectoryNil
	}
	if err := route.Validate(); err != nil {
		return discovery.Instance{}, err
	}

	var (
		instance discovery.Instance
		ok       bool
	)
	if route.Kind == RouteService {
		instance, ok = directory.NextInstance(route.Service)
	} else {
		instance, ok = directory.LookupInstance(route.InstanceID)
	}
	if !ok {
		return discovery.Instance{}, ErrRouteUnavailable
	}
	return instance, nil
}

func validRouteValue(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}
