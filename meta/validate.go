package meta

import "fmt"

// Validate 根据扫描得到的 Go 类型元数据校验内置存储元数据。
//
// 它会检查 storage mode 所需配置是否齐全，并确认 id/route 字段引用的是
// astType 中存在的 Go 字段名。例如结构体中不存在 RoleId 字段时，
// `idField:"RoleId"` 会在这里报错。
func Validate(spec TypeSpec, astType TypeSpec) error {
	fields := map[string]struct{}{}
	for _, field := range astType.Type.Fields {
		if field.Name != "" {
			fields[field.Name] = struct{}{}
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
		if err := requireField("redis.idField", spec.Redis.IDField, fields); err != nil {
			return err
		}
		if err := requireField("redis.routeBy", spec.Redis.Route.By, fields); err != nil {
			return err
		}
		if err := validateRouteFieldType("redis.routeBy", spec.Redis.Route.By, astType); err != nil {
			return err
		}
	}
	if spec.Mongo != nil {
		if spec.Mongo.Collection == "" {
			return fmt.Errorf("%w: mongodb.collection", ErrMissingConfig)
		}
		if err := requireField("mongodb.idField", spec.Mongo.IDField, fields); err != nil {
			return err
		}
		if err := requireField("mongodb.routeBy", spec.Mongo.Route.By, fields); err != nil {
			return err
		}
		if err := validateRouteFieldType("mongodb.routeBy", spec.Mongo.Route.By, astType); err != nil {
			return err
		}
	}
	if spec.Storage.Mode == StorageModeFusion {
		if err := validateFusionRecordIdentity(spec); err != nil {
			return err
		}
	}
	for field := range spec.Fields {
		if _, ok := fields[field]; !ok {
			return fmt.Errorf("%w: fields.%s", ErrUnknownField, field)
		}
	}
	return nil
}

func validateRouteFieldType(property, fieldName string, astType TypeSpec) error {
	field, ok := astType.FieldByName(fieldName)
	if !ok {
		return nil
	}
	switch field.Type {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"byte", "rune", "string":
		return nil
	default:
		return fmt.Errorf("%s %s field type %s is unsupported; use a built-in integer or string", property, fieldName, field.Type)
	}
}

func validateFusionRecordIdentity(spec TypeSpec) error {
	if spec.Redis.IDField != spec.Mongo.IDField {
		return fmt.Errorf(
			"meta: fusion redis idField %q differs from mongodb idField %q",
			spec.Redis.IDField,
			spec.Mongo.IDField,
		)
	}
	if spec.Redis.Route.By != spec.Mongo.Route.By {
		return fmt.Errorf(
			"meta: fusion redis routeBy %q differs from mongodb routeBy %q",
			spec.Redis.Route.By,
			spec.Mongo.Route.By,
		)
	}
	return nil
}

func requireField(property, field string, fields map[string]struct{}) error {
	if field == "" {
		return fmt.Errorf("%w: %s", ErrMissingConfig, property)
	}
	if _, ok := fields[field]; !ok {
		return fmt.Errorf("%w: %s references %q", ErrUnknownField, property, field)
	}
	return nil
}
