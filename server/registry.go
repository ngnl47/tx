package server

import (
	"errors"
	"fmt"
	"strings"
)

type registry struct {
	services []Service
	byName   map[string]Service
}

func newRegistry(services ...Service) (*registry, error) {
	result := &registry{byName: make(map[string]Service, len(services))}
	for _, service := range services {
		if service == nil {
			return nil, errors.New("server: service is nil")
		}
		spec := service.Spec()
		name := strings.TrimSpace(spec.Name)
		if name == "" {
			return nil, errors.New("server: service name is empty")
		}
		if name != spec.Name {
			return nil, fmt.Errorf("server: service name %q contains surrounding whitespace", spec.Name)
		}
		if _, exists := result.byName[name]; exists {
			return nil, fmt.Errorf("server: duplicate service %q", name)
		}
		result.services = append(result.services, service)
		result.byName[name] = service
	}
	return result, nil
}

func (registry *registry) selectServices(expression string) ([]Service, error) {
	if registry == nil {
		return nil, errors.New("server: service registry is nil")
	}
	include := make(map[string]bool, len(registry.services))
	exclude := make(map[string]bool, len(registry.services))
	for _, token := range strings.Split(expression, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if token == "*" {
			for _, service := range registry.services {
				include[service.Spec().Name] = true
			}
			continue
		}
		negative := strings.HasPrefix(token, "!")
		name := strings.TrimSpace(strings.TrimPrefix(token, "!"))
		if _, exists := registry.byName[name]; !exists {
			return nil, fmt.Errorf("server: unknown service %q", name)
		}
		if negative {
			exclude[name] = true
		} else {
			include[name] = true
		}
	}
	selected := make([]Service, 0, len(include))
	for _, service := range registry.services {
		name := service.Spec().Name
		if include[name] && !exclude[name] {
			selected = append(selected, service)
		}
	}
	if len(selected) == 0 {
		return nil, errors.New("server: no services selected")
	}
	return selected, nil
}
