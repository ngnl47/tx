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

// RouteKind identifies how a delivery target is resolved.
type RouteKind uint8

const (
	RouteUnknown RouteKind = iota
	RouteService
	RouteInstance
)

// Route contains transport-level routing metadata.
type Route struct {
	Kind       RouteKind
	Service    string
	InstanceID string
}

// ServiceRoute creates a route that selects a running service replica.
func ServiceRoute(service string) Route {
	return Route{Kind: RouteService, Service: service}
}

// InstanceRoute creates a route that selects a known running or stopping instance.
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

// Resolve resolves service routes to a running replica and exact routes to one known instance.
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
