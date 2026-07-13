package storage

import (
	"github.com/ngnl47/gobricks/meta"
)

type TypeLookup interface {
	Lookup(meta.TypeRef) (meta.TypeSpec, bool)
}

func ResolveMeta(specs []meta.TypeSpec, types TypeLookup) (*Registry, error) {
	reg := NewRegistry()
	for _, spec := range specs {
		if hasMetaStorage(spec) {
			if astType, ok := types.Lookup(meta.TypeRef{FullName: spec.Type.Ref.FullName}); ok {
				spec.Type = astType.Type
			}
			if err := meta.Validate(spec, spec); err != nil {
				return nil, err
			}
			cfg := TypeConfig{
				TypeName:    spec.Type.Ref.FullName,
				Mode:        Mode(spec.Storage.Mode),
				MemoryCache: spec.Storage.MemoryCache.String(),
				Fields:      resolveMetaFields(spec.Fields),
			}
			if spec.Storage.MemoryCache.Duration == 0 {
				cfg.MemoryCache = ""
			}
			if spec.Redis != nil {
				cfg.Redis = &RedisConfig{
					Route:     RouteConfig{Type: spec.Redis.Route.Type, By: spec.Redis.Route.By},
					ID:        spec.Redis.IDField,
					Key:       spec.Redis.Key,
					OmitEmpty: spec.Redis.OmitEmpty,
					StoreType: spec.Redis.StoreType,
				}
			}
			if spec.Mongo != nil {
				cfg.Mongo = &MongoConfig{
					Route:            RouteConfig{Type: spec.Mongo.Route.Type, By: spec.Mongo.Route.By},
					ID:               spec.Mongo.IDField,
					Collection:       spec.Mongo.Collection,
					Sharding:         ShardingConfig{Enabled: spec.Mongo.Sharding.Enabled},
					UseMongoObjectID: spec.Mongo.UseMongoObjectID,
				}
			}
			if spec.Fusion != nil || spec.Storage.Mode == meta.StorageModeFusion {
				cfg.Fusion = &FusionConfig{}
			}
			if err := reg.Register(spec.Type.Ref.FullName, cfg); err != nil {
				return nil, err
			}
		}
	}
	reg.Freeze()
	return reg, nil
}

func hasMetaStorage(spec meta.TypeSpec) bool {
	return spec.Storage.Mode != "" || spec.Redis != nil || spec.Mongo != nil
}

func resolveMetaFields(fields map[string]meta.FieldSpec) map[string]FieldConfig {
	out := make(map[string]FieldConfig, len(fields))
	for name, spec := range fields {
		cfg := FieldConfig{}
		if spec.Redis != nil {
			cfg.Redis = &RedisFieldConfig{
				SubKey:     spec.Redis.SubKey,
				Ignore:     spec.Redis.Ignore,
				FullUpdate: spec.Redis.FullUpdate,
			}
		}
		if spec.Mongo != nil {
			mongo := &MongoFieldConfig{
				Ignore: spec.Mongo.Ignore,
			}
			for _, idx := range spec.Mongo.Indexes {
				mongo.Indexes = append(mongo.Indexes, IndexConfig{
					Unique:      idx.Unique,
					Order:       idx.Order,
					Weight:      idx.Weight,
					ExpireAfter: idx.ExpireAfter,
				})
			}
			cfg.Mongo = mongo
		}
		out[name] = cfg
	}
	return out
}
