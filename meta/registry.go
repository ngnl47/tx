package meta

import (
	"fmt"
	"sync"
)

// Registry 在初始化阶段保存类型元数据，并在 Freeze 后变为不可变。
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
	spec = CloneTypeSpec(spec)
	spec.Type.Ref = spec.Type.Ref.normalized()
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
	return CloneTypeSpec(spec), true
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
