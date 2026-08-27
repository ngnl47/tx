package framework

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ngnl47/tx/component"
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
	components := make([]component.Component, 0, len(cmods))
	for _, mod := range cmods {
		if isNilInterface(mod) {
			return cmodGraph{}, errors.New("framework: CMod is nil")
		}
		spec := mod.Spec()
		if err := validateCModID(spec.ID); err != nil {
			return cmodGraph{}, err
		}
		if err := validateComponentSpecs(string(spec.ID), spec.Components); err != nil {
			return cmodGraph{}, err
		}
		if _, exists := byID[spec.ID]; exists {
			return cmodGraph{}, fmt.Errorf("framework: duplicate CMod %q", spec.ID)
		}
		for _, dependency := range append(append([]CModID(nil), spec.Requires...), spec.After...) {
			if err := validateCModID(dependency); err != nil {
				return cmodGraph{}, fmt.Errorf("framework: CMod %q dependency: %w", spec.ID, err)
			}
		}
		byID[spec.ID] = mod
		components = append(components, cmodGraphComponent{
			spec: component.Spec{
				ID:       component.ID(spec.ID),
				Requires: componentIDs(spec.Requires),
				After:    componentIDs(spec.After),
			},
		})
	}

	selection := newServiceSelection(services)
	roots := make([]component.Root, 0)
	serviceRoots := make([]cmodServiceRoot, 0, len(services))
	for _, service := range services {
		if isNilInterface(service) {
			return cmodGraph{}, errors.New("framework: selected service is nil")
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
				return cmodGraph{}, fmt.Errorf("framework: resolve CMod requirements for service %q: %w", spec.Name, err)
			}
			required = append(required, conditional...)
		}
		required = uniqueCModIDs(required)
		serviceRoots = append(serviceRoots, cmodServiceRoot{name: spec.Name, requires: append([]CModID(nil), required...)})
		for _, id := range required {
			roots = append(roots, component.Root{Source: spec.Name, ID: component.ID(id)})
		}
	}

	resolved, err := component.Resolve(components, roots)
	if err != nil {
		return cmodGraph{}, fmt.Errorf("framework: resolve CMod graph: %w", err)
	}
	ordered := make([]CMod, 0, len(resolved.IDs()))
	for _, id := range resolved.IDs() {
		ordered = append(ordered, byID[CModID(id)])
	}
	return cmodGraph{ordered: ordered, roots: serviceRoots}, nil
}

type cmodGraphComponent struct {
	spec component.Spec
}

func (current cmodGraphComponent) Spec() component.Spec {
	return current.spec
}

func (cmodGraphComponent) Start(component.Context) error {
	return errors.New("framework: graph-only CMod adapter cannot be started")
}

func componentIDs(ids []CModID) []component.ID {
	result := make([]component.ID, len(ids))
	for index := range ids {
		result[index] = component.ID(ids[index])
	}
	return result
}

func collectActorKinds(services []Service) ([]txactor.Kind, error) {
	var kinds []txactor.Kind
	owners := make(map[string]string)
	for _, service := range services {
		if service == nil {
			return nil, errors.New("framework: selected service is nil")
		}
		spec := service.Spec()
		for _, kind := range spec.ActorKinds {
			name := kind.Name()
			if name == "" {
				return nil, fmt.Errorf("framework: service %q declares invalid Actor kind", spec.Name)
			}
			if owner, exists := owners[name]; exists {
				return nil, fmt.Errorf("framework: duplicate Actor kind %q declared by services %q and %q", name, owner, spec.Name)
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
		return errors.New("framework: CMod ID is empty")
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("framework: CMod ID %q contains surrounding whitespace", value)
	}
	return nil
}

func cmodIDStrings(ids []CModID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = string(id)
	}
	return result
}
