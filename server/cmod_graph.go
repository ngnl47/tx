package server

import (
	"errors"
	"fmt"
	"strings"

	txactor "github.com/ngnl47/tx/modules/actor"
)

type cmodGraph struct {
	ordered []CMod
	roots   []cmodServiceRoot
}

type cmodServiceRoot struct {
	name     string
	requires []CModID
}

func (graph cmodGraph) IDs() []CModID {
	ids := make([]CModID, 0, len(graph.ordered))
	for _, mod := range graph.ordered {
		ids = append(ids, mod.Spec().ID)
	}
	return ids
}

func resolveCModGraph(cmods []CMod, services []Service) (cmodGraph, error) {
	byID := make(map[CModID]CMod, len(cmods))
	for _, mod := range cmods {
		if isNilInterface(mod) {
			return cmodGraph{}, errors.New("server: CMod is nil")
		}
		spec := mod.Spec()
		if err := validateCModID(spec.ID); err != nil {
			return cmodGraph{}, err
		}
		if err := validateComponentSpecs(string(spec.ID), spec.Components); err != nil {
			return cmodGraph{}, err
		}
		if _, exists := byID[spec.ID]; exists {
			return cmodGraph{}, fmt.Errorf("server: duplicate CMod %q", spec.ID)
		}
		for _, dependency := range append(append([]CModID(nil), spec.Requires...), spec.After...) {
			if err := validateCModID(dependency); err != nil {
				return cmodGraph{}, fmt.Errorf("server: CMod %q dependency: %w", spec.ID, err)
			}
		}
		byID[spec.ID] = mod
	}

	selection := newServiceSelection(services)
	type root struct {
		service string
		id      CModID
	}
	roots := make([]root, 0)
	serviceRoots := make([]cmodServiceRoot, 0, len(services))
	for _, service := range services {
		if isNilInterface(service) {
			return cmodGraph{}, errors.New("server: selected service is nil")
		}
		spec := service.Spec()
		required := append([]CModID(nil), spec.Requires...)
		if len(spec.ActorKinds) > 0 {
			required = append(required, CModActor)
			for _, kind := range spec.ActorKinds {
				if kind.IsFixed() {
					required = append(required, CModDiscovery)
					break
				}
			}
		}
		if provider, ok := service.(CModRequirementsProvider); ok {
			conditional, err := provider.RequiredCMods(selection)
			if err != nil {
				return cmodGraph{}, fmt.Errorf("server: resolve CMod requirements for service %q: %w", spec.Name, err)
			}
			required = append(required, conditional...)
		}
		required = uniqueCModIDs(required)
		serviceRoots = append(serviceRoots, cmodServiceRoot{name: spec.Name, requires: append([]CModID(nil), required...)})
		for _, id := range required {
			roots = append(roots, root{service: spec.Name, id: id})
		}
	}

	const (
		unseen uint8 = iota
		visiting
		visited
	)
	selected := make(map[CModID]struct{}, len(byID))
	states := make(map[CModID]uint8, len(byID))
	path := make([]CModID, 0, len(byID))
	var selectRequired func(CModID, string) error
	selectRequired = func(id CModID, service string) error {
		if err := validateCModID(id); err != nil {
			return fmt.Errorf("server: service %q requirement: %w", service, err)
		}
		switch states[id] {
		case visited:
			return nil
		case visiting:
			start := 0
			for start < len(path) && path[start] != id {
				start++
			}
			cycle := append(append([]CModID(nil), path[start:]...), id)
			return fmt.Errorf("server: CMod dependency cycle: %s", joinCModPath(cycle))
		}
		mod, exists := byID[id]
		if !exists {
			missingPath := append([]CModID(nil), path...)
			missingPath = append(missingPath, id)
			return fmt.Errorf("server: missing CMod dependency: %s -> %s", service, joinCModPath(missingPath))
		}
		states[id] = visiting
		path = append(path, id)
		for _, dependency := range mod.Spec().Requires {
			if err := selectRequired(dependency, service); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		states[id] = visited
		selected[id] = struct{}{}
		return nil
	}
	for _, root := range roots {
		if err := selectRequired(root.id, root.service); err != nil {
			return cmodGraph{}, err
		}
	}

	states = make(map[CModID]uint8, len(selected))
	path = path[:0]
	ordered := make([]CMod, 0, len(selected))
	var order func(CModID, string) error
	order = func(id CModID, service string) error {
		switch states[id] {
		case visited:
			return nil
		case visiting:
			start := 0
			for start < len(path) && path[start] != id {
				start++
			}
			cycle := append(append([]CModID(nil), path[start:]...), id)
			return fmt.Errorf("server: CMod dependency cycle: %s", joinCModPath(cycle))
		}
		mod := byID[id]
		states[id] = visiting
		path = append(path, id)
		for _, dependency := range mod.Spec().Requires {
			if err := order(dependency, service); err != nil {
				return err
			}
		}
		for _, predecessor := range mod.Spec().After {
			if _, ok := selected[predecessor]; !ok {
				continue
			}
			if err := order(predecessor, service); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		states[id] = visited
		ordered = append(ordered, mod)
		return nil
	}
	for _, root := range roots {
		if err := order(root.id, root.service); err != nil {
			return cmodGraph{}, err
		}
	}
	return cmodGraph{ordered: ordered, roots: serviceRoots}, nil
}

func collectActorKinds(services []Service) ([]txactor.Kind, error) {
	var kinds []txactor.Kind
	owners := make(map[string]string)
	for _, service := range services {
		if service == nil {
			return nil, errors.New("server: selected service is nil")
		}
		spec := service.Spec()
		for _, kind := range spec.ActorKinds {
			name := kind.Name()
			if name == "" {
				return nil, fmt.Errorf("server: service %q declares invalid Actor kind", spec.Name)
			}
			if owner, exists := owners[name]; exists {
				return nil, fmt.Errorf("server: duplicate Actor kind %q declared by services %q and %q", name, owner, spec.Name)
			}
			owners[name] = spec.Name
			kinds = append(kinds, kind)
		}
	}
	return kinds, nil
}

func uniqueCModIDs(ids []CModID) []CModID {
	seen := make(map[CModID]struct{}, len(ids))
	result := make([]CModID, 0, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func validateCModID(id CModID) error {
	value := string(id)
	if strings.TrimSpace(value) == "" {
		return errors.New("server: CMod ID is empty")
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("server: CMod ID %q contains surrounding whitespace", value)
	}
	return nil
}

func joinCModPath(path []CModID) string {
	return strings.Join(cmodIDStrings(path), " -> ")
}

func cmodIDStrings(ids []CModID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = string(id)
	}
	return result
}
