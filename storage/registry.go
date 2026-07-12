package storage

import (
	"fmt"
	"sort"
	"sync"
)

type Registry struct {
	mu     sync.RWMutex
	frozen bool
	types  map[string]TypeConfig
}

func NewRegistry() *Registry {
	return &Registry{types: map[string]TypeConfig{}}
}

func (r *Registry) Register(typeName string, cfg TypeConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("%w: cannot register %s", ErrRegistryFrozen, typeName)
	}
	if _, exists := r.types[typeName]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateType, typeName)
	}
	cfg.TypeName = typeName
	r.types[typeName] = cloneTypeConfig(cfg)
	return nil
}

func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

func (r *Registry) Lookup(typeName string) (TypeConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.types[typeName]
	if !ok {
		return TypeConfig{}, false
	}
	return cloneTypeConfig(cfg), true
}

func (r *Registry) TypeNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.types))
	for name := range r.types {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func cloneTypeConfig(cfg TypeConfig) TypeConfig {
	if cfg.Redis != nil {
		copyRedis := *cfg.Redis
		cfg.Redis = &copyRedis
	}
	if cfg.Mongo != nil {
		copyMongo := *cfg.Mongo
		cfg.Mongo = &copyMongo
	}
	if cfg.Fusion != nil {
		copyFusion := *cfg.Fusion
		cfg.Fusion = &copyFusion
	}
	if cfg.Fields != nil {
		fields := make(map[string]FieldConfig, len(cfg.Fields))
		for name, field := range cfg.Fields {
			if field.Redis != nil {
				copyRedis := *field.Redis
				field.Redis = &copyRedis
			}
			if field.Mongo != nil {
				copyMongo := *field.Mongo
				if field.Mongo.Indexes != nil {
					copyMongo.Indexes = append([]IndexConfig(nil), field.Mongo.Indexes...)
				}
				field.Mongo = &copyMongo
			}
			fields[name] = field
		}
		cfg.Fields = fields
	}
	return cfg
}
