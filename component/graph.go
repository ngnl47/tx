package component

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Graph 是已经完成依赖闭包选择和稳定拓扑排序的只读组件图。
type Graph struct {
	ordered []graphEntry
}

type graphEntry struct {
	component Component
	spec      Spec
}

// IDs 按稳定拓扑顺序返回组件 ID 的副本。
func (graph Graph) IDs() []ID {
	ids := make([]ID, len(graph.ordered))
	for index := range graph.ordered {
		ids[index] = graph.ordered[index].spec.ID
	}
	return ids
}

// Components 按稳定拓扑顺序返回组件接口的副本。
func (graph Graph) Components() []Component {
	components := make([]Component, len(graph.ordered))
	for index := range graph.ordered {
		components[index] = graph.ordered[index].component
	}
	return components
}

// Resolve 选择 Roots 的完整依赖闭包，并按 Requires 和已选中的 After 稳定排序。
func Resolve(components []Component, roots []Root) (Graph, error) {
	entries := make(map[ID]graphEntry, len(components))
	for _, current := range components {
		if isNil(current) {
			return Graph{}, errors.New("component: component is nil")
		}
		spec, err := copyAndValidateSpec(current.Spec())
		if err != nil {
			return Graph{}, err
		}
		if _, exists := entries[spec.ID]; exists {
			return Graph{}, fmt.Errorf("component: duplicate component %q", spec.ID)
		}
		entries[spec.ID] = graphEntry{component: current, spec: spec}
	}

	selected := make(map[ID]struct{}, len(entries))
	states := make(map[ID]visitState, len(entries))
	path := make([]ID, 0, len(entries))
	var selectRequired func(ID, string) error
	selectRequired = func(id ID, source string) error {
		switch states[id] {
		case visitDone:
			return nil
		case visitActive:
			return dependencyCycleError(path, id)
		}
		entry, exists := entries[id]
		if !exists {
			missing := append(append([]ID(nil), path...), id)
			return fmt.Errorf("component: missing dependency: %s -> %s", source, joinPath(missing))
		}
		states[id] = visitActive
		path = append(path, id)
		for _, dependency := range entry.spec.Requires {
			if err := selectRequired(dependency, source); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		states[id] = visitDone
		selected[id] = struct{}{}
		return nil
	}
	for _, root := range roots {
		source, err := validateRoot(root)
		if err != nil {
			return Graph{}, err
		}
		if err := selectRequired(root.ID, source); err != nil {
			return Graph{}, err
		}
	}

	states = make(map[ID]visitState, len(selected))
	path = path[:0]
	ordered := make([]graphEntry, 0, len(selected))
	var order func(ID) error
	order = func(id ID) error {
		switch states[id] {
		case visitDone:
			return nil
		case visitActive:
			return dependencyCycleError(path, id)
		}
		entry := entries[id]
		states[id] = visitActive
		path = append(path, id)
		for _, dependency := range entry.spec.Requires {
			if err := order(dependency); err != nil {
				return err
			}
		}
		for _, predecessor := range entry.spec.After {
			if _, exists := selected[predecessor]; !exists {
				continue
			}
			if err := order(predecessor); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		states[id] = visitDone
		ordered = append(ordered, entry)
		return nil
	}
	for _, root := range roots {
		if err := order(root.ID); err != nil {
			return Graph{}, err
		}
	}
	return Graph{ordered: ordered}, nil
}

type visitState uint8

const (
	visitUnseen visitState = iota
	visitActive
	visitDone
)

func copyAndValidateSpec(spec Spec) (Spec, error) {
	if err := validateID(spec.ID); err != nil {
		return Spec{}, err
	}
	spec.Requires = append([]ID(nil), spec.Requires...)
	spec.After = append([]ID(nil), spec.After...)
	for _, dependency := range append(append([]ID(nil), spec.Requires...), spec.After...) {
		if err := validateID(dependency); err != nil {
			return Spec{}, fmt.Errorf("component: component %q dependency: %w", spec.ID, err)
		}
	}
	return spec, nil
}

func validateRoot(root Root) (string, error) {
	source := strings.TrimSpace(root.Source)
	if source == "" || source != root.Source {
		return "", fmt.Errorf("component: root source %q is invalid", root.Source)
	}
	if err := validateID(root.ID); err != nil {
		return "", fmt.Errorf("component: root %q: %w", source, err)
	}
	return source, nil
}

func validateID(id ID) error {
	value := string(id)
	if strings.TrimSpace(value) == "" {
		return errors.New("component: ID is empty")
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("component: ID %q contains surrounding whitespace", value)
	}
	return nil
}

func dependencyCycleError(path []ID, id ID) error {
	start := 0
	for start < len(path) && path[start] != id {
		start++
	}
	cycle := append(append([]ID(nil), path[start:]...), id)
	return fmt.Errorf("component: dependency cycle: %s", joinPath(cycle))
}

func joinPath(path []ID) string {
	values := make([]string, len(path))
	for index := range path {
		values[index] = string(path[index])
	}
	return strings.Join(values, " -> ")
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
