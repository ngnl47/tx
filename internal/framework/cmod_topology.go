package framework

import "strings"

func cmodTopology(mods []CMod, services ServiceSelection) []string {
	topology := make([]string, 0, len(mods))
	for _, mod := range mods {
		if mod == nil {
			continue
		}
		spec := mod.Spec()
		components := activeComponentTopology(spec.Components, services)
		if len(components) == 0 {
			continue
		}
		topology = append(topology, string(spec.ID)+"("+strings.Join(components, ",")+")")
	}
	return topology
}

func activeComponentTopology(components []ComponentSpec, services ServiceSelection) []string {
	active := make([]string, 0, len(components))
	for _, component := range components {
		if !componentEnabled(component, services) {
			continue
		}
		name := component.Role + "=" + component.Name
		children := activeComponentTopology(component.Components, services)
		if len(children) > 0 {
			name += "[" + strings.Join(children, ",") + "]"
		}
		active = append(active, name)
	}
	return active
}

func componentEnabled(component ComponentSpec, services ServiceSelection) bool {
	if !component.Optional {
		return true
	}
	for _, service := range component.Services {
		if services.Has(service) {
			return true
		}
	}
	return false
}
