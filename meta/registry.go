// Package meta 定义生成数据的类型、字段与存储描述，不持有业务数据或存储连接。
//
// 生成包通过 Registry 登记描述并 Freeze；框架通过只读 TypeRegistry 查询以完成能力装配。
// 元数据描述存储模式，不替应用启用 CMod，也不承担 GD 的加载、缓存或持久化。
package meta

import (
	"fmt"
	"sort"
	"sync"
)

// TypeRegistry 提供只读的类型元数据查询，供持久化装配与生成代码共同使用。
type TypeRegistry interface {
	// Lookup 按类型身份查询描述；使用者应将返回元数据视为只读。
	Lookup(TypeRef) (TypeSpec, bool)
	// TypeSpecs 枚举类型描述；使用者应将返回元数据视为只读。
	TypeSpecs() []TypeSpec
}

// Registry 在初始化阶段保存类型元数据，并在 Freeze 后变为不可变。
type Registry struct {
	mu     sync.RWMutex
	frozen bool
	types  map[string]TypeSpec
}

// NewRegistry 创建可登记的类型目录；登记完成后调用 Freeze 固定描述。
func NewRegistry() *Registry {
	return &Registry{
		types: make(map[string]TypeSpec),
	}
}

// RegisterType 复制、规范化并校验输入后登记；后续修改输入不影响目录。
// 重复类型或 Freeze 后登记返回错误，不覆盖已有描述。
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

// Freeze 禁止后续登记，不影响查询；可重复调用。
func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

// Lookup 返回指定类型的独立描述副本；修改返回值不影响目录。
func (r *Registry) Lookup(ref TypeRef) (TypeSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, ok := r.types[ref.key()]
	if !ok {
		return TypeSpec{}, false
	}
	return CloneTypeSpec(spec), true
}

// TypeSpecs 按类型全名稳定返回注册元数据的独立副本。
func (r *Registry) TypeSpecs() []TypeSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.types))
	for key := range r.types {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	specs := make([]TypeSpec, 0, len(keys))
	for _, key := range keys {
		specs = append(specs, CloneTypeSpec(r.types[key]))
	}
	return specs
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
