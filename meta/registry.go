package meta

import (
	"fmt"
	"sync"
)

// Registry stores type metadata during initialization and becomes immutable
// after Freeze.
type Registry struct {
	mu     sync.RWMutex
	frozen bool
	types  map[string]TypeSpec
}

func NewRegistry() *Registry {
	return &Registry{
		types: make(map[string]TypeSpec),
	}
}

func (r *Registry) RegisterType(spec TypeSpec) error {
	spec = cloneTypeSpec(spec)
	key := spec.Type.Ref.key()
	if key == "" {
		return fmt.Errorf("%w: missing type name", ErrInvalidTypeRef)
	}
	if err := validateTypeSpec(spec); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("%w: cannot register type %s", ErrRegistryFrozen, key)
	}
	if _, exists := r.types[key]; exists {
		return fmt.Errorf("%w: type %s already registered", ErrDuplicateType, key)
	}
	r.types[key] = spec
	return nil
}

func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

func (r *Registry) Lookup(ref TypeRef) (TypeSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, ok := r.types[ref.key()]
	if !ok {
		return TypeSpec{}, false
	}
	return cloneTypeSpec(spec), true
}

func (r *Registry) ApplyOverlay(overlay Overlay) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("%w: cannot apply overlay", ErrRegistryFrozen)
	}
	if overlay == nil {
		return nil
	}

	next := make(map[string]TypeSpec, len(r.types))
	for key, spec := range r.types {
		overlaid, err := overlay.Apply(cloneTypeSpec(spec))
		if err != nil {
			return fmt.Errorf("meta: overlay type %s: %w", key, err)
		}
		overlaid = cloneTypeSpec(overlaid)
		if err := validateTypeSpec(overlaid); err != nil {
			return err
		}
		nextKey := overlaid.Type.Ref.key()
		if existing, exists := next[nextKey]; exists {
			return fmt.Errorf("%w: type %s conflicts with type %s", ErrDuplicateType, nextKey, existing.Type.Ref.FullName)
		}
		next[nextKey] = overlaid
	}
	r.types = next
	return nil
}

func validateTypeSpec(spec TypeSpec) error {
	typeName := spec.Type.Ref.key()
	aliases := make(map[string]string, len(spec.Type.Fields))
	for _, field := range spec.Type.Fields {
		if field.Alias == "" {
			continue
		}
		if previous, exists := aliases[field.Alias]; exists {
			return fmt.Errorf(
				"%w: type %s field %s uses alias %q already used by field %s",
				ErrDuplicateFieldAlias,
				typeName,
				field.Name,
				field.Alias,
				previous,
			)
		}
		aliases[field.Alias] = field.Name
	}
	return nil
}
