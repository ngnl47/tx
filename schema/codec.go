package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/ngnl47/gobricks/meta"
)

func Export(specs []meta.TypeSpec) ([]byte, error) {
	doc := Document{Version: 1, Types: map[string]TypeSpec{}}
	for _, spec := range specs {
		name := spec.Type.Ref.FullName
		if name == "" {
			name = spec.Type.Ref.Name
		}
		doc.Types[name] = typeSpecFromMeta(spec)
	}
	return json.MarshalIndent(doc, "", "  ")
}

func Import(data []byte) ([]meta.TypeSpec, error) {
	doc, err := LoadBytes(data)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(doc.Types))
	for name := range doc.Types {
		names = append(names, name)
	}
	sort.Strings(names)
	specs := make([]meta.TypeSpec, 0, len(names))
	for _, name := range names {
		specs = append(specs, metaSpecFromSchema(name, doc.Types[name]))
	}
	return specs, nil
}

func CheckConflicts(annotationSpecs, schemaSpecs []meta.TypeSpec) error {
	byName := map[string]meta.TypeSpec{}
	for _, spec := range annotationSpecs {
		byName[spec.Type.Ref.FullName] = spec
	}
	for _, spec := range schemaSpecs {
		if existing, ok := byName[spec.Type.Ref.FullName]; ok {
			if !reflect.DeepEqual(comparableMeta(existing), comparableMeta(spec)) {
				return fmt.Errorf("%w: %s", ErrConflict, spec.Type.Ref.FullName)
			}
		}
	}
	return nil
}

func typeSpecFromMeta(spec meta.TypeSpec) TypeSpec {
	out := TypeSpec{
		Storage: StorageSpec{
			Mode:        StorageMode(spec.Storage.Mode),
			DelaySave:   Duration{Duration: spec.Storage.DelaySave.Duration},
			DelayDelete: Duration{Duration: spec.Storage.DelayDelete.Duration},
		},
		Fields: map[string]FieldSpec{},
	}
	if spec.Redis != nil {
		out.Redis = &RedisSpec{
			Route:     RouteSpec{Type: spec.Redis.Route.Type, By: spec.Redis.Route.By},
			ID:        spec.Redis.IDField,
			Key:       spec.Redis.Key,
			OmitEmpty: spec.Redis.OmitEmpty,
			StoreType: spec.Redis.StoreType,
		}
	}
	if spec.Mongo != nil {
		out.Mongo = &MongoSpec{
			Route:      RouteSpec{Type: spec.Mongo.Route.Type, By: spec.Mongo.Route.By},
			ID:         spec.Mongo.IDField,
			Collection: spec.Mongo.Collection,
			Sharding:   ShardingSpec{Enabled: spec.Mongo.Sharding.Enabled},
		}
	}
	for name, field := range spec.Fields {
		outField := FieldSpec{}
		if field.Redis != nil {
			outField.Redis = &RedisFieldSpec{
				SubKey:     field.Redis.SubKey,
				Ignore:     field.Redis.Ignore,
				FullUpdate: field.Redis.FullUpdate,
			}
		}
		if field.Mongo != nil {
			outField.Mongo = &MongoFieldSpec{Ignore: field.Mongo.Ignore}
			for _, idx := range field.Mongo.Indexes {
				outField.Mongo.Indexes = append(outField.Mongo.Indexes, IndexSpec{
					Unique:      idx.Unique,
					Order:       idx.Order,
					Weight:      idx.Weight,
					ExpireAfter: idx.ExpireAfter,
				})
			}
		}
		out.Fields[name] = outField
	}
	return out
}

func metaSpecFromSchema(typeName string, spec TypeSpec) meta.TypeSpec {
	out := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{FullName: typeName}},
		Storage: meta.StorageSpec{
			Mode:        meta.StorageMode(spec.Storage.Mode),
			DelaySave:   meta.Duration{Duration: spec.Storage.DelaySave.Duration},
			DelayDelete: meta.Duration{Duration: spec.Storage.DelayDelete.Duration},
		},
		Fields: map[string]meta.FieldSpec{},
	}
	if spec.Redis != nil {
		out.Redis = &meta.RedisSpec{
			Route:     meta.RouteSpec{Type: spec.Redis.Route.Type, By: spec.Redis.Route.By},
			IDField:   spec.Redis.ID,
			Key:       spec.Redis.Key,
			OmitEmpty: spec.Redis.OmitEmpty,
			StoreType: spec.Redis.StoreType,
		}
	}
	if spec.Mongo != nil {
		out.Mongo = &meta.MongoSpec{
			Route:      meta.RouteSpec{Type: spec.Mongo.Route.Type, By: spec.Mongo.Route.By},
			IDField:    spec.Mongo.ID,
			Collection: spec.Mongo.Collection,
			Sharding:   meta.ShardingSpec{Enabled: spec.Mongo.Sharding.Enabled},
		}
	}
	if spec.Storage.Mode == StorageModeFusion {
		out.Fusion = &meta.FusionSpec{}
	}
	for name, field := range spec.Fields {
		outField := meta.FieldSpec{}
		if field.Redis != nil {
			outField.Redis = &meta.RedisFieldSpec{
				SubKey:     field.Redis.SubKey,
				Ignore:     field.Redis.Ignore,
				FullUpdate: field.Redis.FullUpdate,
			}
		}
		if field.Mongo != nil {
			outField.Mongo = &meta.MongoFieldSpec{Ignore: field.Mongo.Ignore}
			for _, idx := range field.Mongo.Indexes {
				outField.Mongo.Indexes = append(outField.Mongo.Indexes, meta.IndexSpec{
					Unique:      idx.Unique,
					Order:       idx.Order,
					Weight:      idx.Weight,
					ExpireAfter: idx.ExpireAfter,
				})
			}
		}
		out.Fields[name] = outField
	}
	return out
}

func comparableMeta(spec meta.TypeSpec) meta.TypeSpec {
	spec.Type.Fields = nil
	spec.Type.Methods = nil
	return spec
}
