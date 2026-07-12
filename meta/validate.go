package meta

import "fmt"

// Validate 根据扫描得到的 Go 类型元数据校验内置存储元数据。
//
// 它会检查 storage mode 所需配置是否齐全，并确认 id/route 字段引用的是
// astType 中存在的字段别名。例如结构体中不存在 alias 为 roleId 的字段时，
// `idField:"roleId"` 会在这里报错。
func Validate(spec TypeSpec, astType TypeSpec) error {
	aliases := map[string]struct{}{}
	for _, field := range astType.Type.Fields {
		if field.Alias != "" {
			aliases[field.Alias] = struct{}{}
		}
	}
	switch spec.Storage.Mode {
	case StorageModeRedis:
		if spec.Redis == nil {
			return fmt.Errorf("%w: redis", ErrMissingConfig)
		}
	case StorageModeMongo:
		if spec.Mongo == nil {
			return fmt.Errorf("%w: mongodb", ErrMissingConfig)
		}
	case StorageModeFusion:
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
