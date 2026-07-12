package anno

import (
	"fmt"

	"github.com/ngnl47/gobricks/meta"
)

func Validate(spec meta.TypeSpec, astType meta.TypeSpec) error {
	aliases := map[string]struct{}{}
	for _, field := range astType.Type.Fields {
		if field.Alias != "" {
			aliases[field.Alias] = struct{}{}
		}
	}
	switch spec.Storage.Mode {
	case meta.StorageModeRedis:
		if spec.Redis == nil {
			return fmt.Errorf("%w: redis", ErrMissingConfig)
		}
	case meta.StorageModeMongo:
		if spec.Mongo == nil {
			return fmt.Errorf("%w: mongodb", ErrMissingConfig)
		}
	case meta.StorageModeFusion:
		if spec.Redis == nil || spec.Mongo == nil {
			return fmt.Errorf("%w: fusion requires redis and mongodb", ErrMissingConfig)
		}
	default:
		return fmt.Errorf("%w: storage.mode %q", ErrUnsupportedMode, spec.Storage.Mode)
	}
	if spec.Redis != nil {
		if err := requireField("redis.idField", spec.Redis.IDField, aliases); err != nil {
			return err
		}
		if err := requireField("redis.routeBy", spec.Redis.Route.By, aliases); err != nil {
			return err
		}
	}
	if spec.Mongo != nil {
		if spec.Mongo.Collection == "" {
			return fmt.Errorf("%w: mongodb.collection", ErrMissingConfig)
		}
		if err := requireField("mongodb.idField", spec.Mongo.IDField, aliases); err != nil {
			return err
		}
		if err := requireField("mongodb.routeBy", spec.Mongo.Route.By, aliases); err != nil {
			return err
		}
	}
	for field := range spec.Fields {
		if _, ok := aliases[field]; !ok {
			return fmt.Errorf("%w: fields.%s", ErrUnknownField, field)
		}
	}
	return nil
}

func requireField(property, field string, aliases map[string]struct{}) error {
	if field == "" {
		return fmt.Errorf("%w: %s", ErrMissingConfig, property)
	}
	if _, ok := aliases[field]; !ok {
		return fmt.Errorf("%w: %s references %q", ErrUnknownField, property, field)
	}
	return nil
}
