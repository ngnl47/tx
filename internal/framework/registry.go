package framework

import (
	"errors"
	"fmt"
	"strings"
)

type registry struct {
	byName       map[string]Service
	actorCatalog map[string]actorTypeSpec
}

func newRegistry(services ...Service) (*registry, error) {
	result := &registry{byName: make(map[string]Service, len(services))}
	for _, service := range services {
		if isNilInterface(service) {
			return nil, errors.New("framework: service is nil")
		}
		spec := service.Spec()
		name := strings.TrimSpace(spec.Name)
		if name == "" {
			return nil, errors.New("framework: service name is empty")
		}
		if name != spec.Name {
			return nil, fmt.Errorf("framework: service name %q contains surrounding whitespace", spec.Name)
		}
		if _, exists := result.byName[name]; exists {
			return nil, fmt.Errorf("framework: duplicate service %q", name)
		}
		result.byName[name] = service
	}
	catalog, err := collectActorTypes(services)
	if err != nil {
		return nil, err
	}
	result.actorCatalog = catalog
	return result, nil
}

// selectServices 校验项目传入的明确启动清单，并保持项目指定的依赖顺序。
func (registry *registry) selectServices(names []string) ([]Service, error) {
	if len(names) == 0 {
		return nil, errors.New("framework: no services selected")
	}
	selected := make([]Service, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		service, ok := registry.byName[name]
		if !ok {
			return nil, fmt.Errorf("framework: unknown service %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("framework: duplicate selected service %q", name)
		}
		seen[name] = true
		selected = append(selected, service)
	}
	return selected, nil
}
