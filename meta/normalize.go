package meta

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ngnl47/tx/anno"
)

// Normalize 将内置存储注解指令转换为 TypeSpec。
//
// 该函数只解释 storage 体系的内置指令："storage"、"redis" 和
// "mongodb"。其他业务自定义注解应由其所属模块从 astscan.PackageSet 中自行消费。
//
// 输入示例：
//
//	@storage -> Params{"mode":"fusion"}
//	@redis   -> Params{"routeBy":"roleId", "idField":"roleId"}
//
// 输出示例：
//
//	TypeSpec{
//		Storage: StorageSpec{Mode: StorageModeFusion},
//		Redis:   &RedisSpec{IDField: "roleId", Route: ...},
//	}
func Normalize(directives []anno.Directive) (TypeSpec, error) {
	spec := TypeSpec{Fields: map[string]FieldSpec{}}
	for _, directive := range directives {
		if directive.Scope == anno.ScopeField {
			field, err := normalizeFieldDirective(directive)
			if err != nil {
				return TypeSpec{}, err
			}
			current := spec.Fields[directive.Field]
			if field.Redis != nil {
				current.Redis = field.Redis
			}
			if field.Mongo != nil {
				current.Mongo = field.Mongo
			}
			spec.Fields[directive.Field] = current
			continue
		}
		switch directive.Name {
		case "storage":
			storage, err := normalizeStorage(directive)
			if err != nil {
				return TypeSpec{}, err
			}
			spec.Storage = storage
		case "redis":
			redis, err := normalizeRedis(directive)
			if err != nil {
				return TypeSpec{}, err
			}
			spec.Redis = &redis
		case "mongodb":
			mongo, err := normalizeMongo(directive)
			if err != nil {
				return TypeSpec{}, err
			}
			spec.Mongo = &mongo
		default:
			return TypeSpec{}, fmt.Errorf("%w: %s", ErrUnknownTag, directive.Name)
		}
	}
	if spec.Storage.Mode == StorageModeFusion {
		spec.Fusion = &FusionSpec{}
	}
	return spec, nil
}

func normalizeStorage(d anno.Directive) (StorageSpec, error) {
	var out StorageSpec
	for key, value := range d.Params {
		switch key {
		case "mode":
			out.Mode = StorageMode(value)
		case "delaySaveTime", "saveTime", "delaySave":
			duration, err := parseDurationParam("storage."+key, value)
			if err != nil {
				return StorageSpec{}, err
			}
			out.DelaySave = Duration{Duration: duration}
		case "delayDelTime", "delTime", "delayDelete":
			duration, err := parseDurationParam("storage."+key, value)
			if err != nil {
				return StorageSpec{}, err
			}
			out.DelayDelete = Duration{Duration: duration}
		case "memoryCache":
			duration, err := parseDurationParam("storage."+key, value)
			if err != nil {
				return StorageSpec{}, err
			}
			out.MemoryCache = Duration{Duration: duration}
		default:
			return StorageSpec{}, fmt.Errorf("%w: storage.%s", ErrUnknownParam, key)
		}
	}
	if out.Mode != StorageModeRedis && out.Mode != StorageModeMongo && out.Mode != StorageModeFusion {
		return StorageSpec{}, fmt.Errorf("%w: storage.mode %q", ErrUnsupportedMode, out.Mode)
	}
	return out, nil
}

func normalizeRedis(d anno.Directive) (RedisSpec, error) {
	var out RedisSpec
	for key, value := range d.Params {
		switch key {
		case "routeType", "shardType", "config":
			out.Route.Type = value
		case "routeBy", "shardBy":
			out.Route.By = value
		case "idField", "id":
			out.IDField = value
		case "key":
			out.Key = value
		case "storeType":
			out.StoreType = value
		case "omitempty":
			parsed, err := parseBoolParam("redis."+key, value)
			if err != nil {
				return RedisSpec{}, err
			}
			out.OmitEmpty = parsed
		default:
			return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, key)
		}
	}
	for flag := range d.Flags {
		switch flag {
		case "omitempty":
			out.OmitEmpty = true
		default:
			return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, flag)
		}
	}
	return out, nil
}

func normalizeMongo(d anno.Directive) (MongoSpec, error) {
	var out MongoSpec
	for key, value := range d.Params {
		switch key {
		case "routeType", "shardType", "config":
			out.Route.Type = value
		case "routeBy", "shardBy":
			out.Route.By = value
		case "idField", "id":
			out.IDField = value
		case "collection", "c", "col":
			out.Collection = value
		case "enableSharding":
			parsed, err := parseBoolParam("mongodb."+key, value)
			if err != nil {
				return MongoSpec{}, err
			}
			out.Sharding.Enabled = parsed
		default:
			return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, key)
		}
	}
	for flag := range d.Flags {
		switch flag {
		case "enableSharding":
			out.Sharding.Enabled = true
		default:
			return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, flag)
		}
	}
	return out, nil
}

func normalizeFieldDirective(d anno.Directive) (FieldSpec, error) {
	var out FieldSpec
	switch d.Name {
	case "redis":
		field := &RedisFieldSpec{}
		for key, value := range d.Params {
			switch key {
			case "subKey":
				field.SubKey = value
			case "ignore":
				parsed, err := parseBoolParam("redis."+key, value)
				if err != nil {
					return FieldSpec{}, err
				}
				field.Ignore = parsed
			case "fullUpdate":
				parsed, err := parseBoolParam("redis."+key, value)
				if err != nil {
					return FieldSpec{}, err
				}
				field.FullUpdate = parsed
			default:
				return FieldSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, key)
			}
		}
		for flag := range d.Flags {
			switch flag {
			case "ignore":
				field.Ignore = true
			case "fullUpdate":
				field.FullUpdate = true
			default:
				return FieldSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, flag)
			}
		}
		out.Redis = field
	case "mongodb":
		field := &MongoFieldSpec{}
		index := IndexSpec{}
		hasIndex := false
		for key, value := range d.Params {
			switch key {
			case "order", "indexType":
				index.Order = value
				hasIndex = true
			case "weight":
				weight, err := parseIntParam("mongodb."+key, value)
				if err != nil {
					return FieldSpec{}, err
				}
				index.Weight = weight
				hasIndex = true
			case "expire", "expireAfter":
				if err := validateNonNegativeInt64Param("mongodb."+key, value); err != nil {
					return FieldSpec{}, err
				}
				index.ExpireAfter = value
				hasIndex = true
			case "ignore":
				parsed, err := parseBoolParam("mongodb."+key, value)
				if err != nil {
					return FieldSpec{}, err
				}
				field.Ignore = parsed
			case "index":
				hasIndex = hasIndex || parseIndexMarker(value)
			case "unique":
				parsed, err := parseBoolParam("mongodb."+key, value)
				if err != nil {
					return FieldSpec{}, err
				}
				index.Unique = parsed
				hasIndex = hasIndex || index.Unique
			default:
				return FieldSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, key)
			}
		}
		for flag := range d.Flags {
			switch flag {
			case "ignore":
				field.Ignore = true
			case "index":
				hasIndex = true
			case "unique":
				index.Unique = true
				hasIndex = true
			default:
				return FieldSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, flag)
			}
		}
		if hasIndex {
			field.Indexes = append(field.Indexes, index)
		}
		out.Mongo = field
	default:
		return FieldSpec{}, fmt.Errorf("%w: %s", ErrUnknownTag, d.Name)
	}
	return out, nil
}

func parseDurationParam(property, value string) (time.Duration, error) {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		duration, err := time.ParseDuration(value + "s")
		if err != nil {
			return 0, fmt.Errorf("meta: parse %s %q: %w", property, value, err)
		}
		return duration, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("meta: parse %s %q: %w", property, value, err)
	}
	return duration, nil
}

func parseBoolParam(property, value string) (bool, error) {
	if value == "" {
		return true, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("meta: parse %s %q: %w", property, value, err)
	}
	return parsed, nil
}

func parseIntParam(property, value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("meta: parse %s %q: %w", property, value, err)
	}
	return parsed, nil
}

func validateNonNegativeInt64Param(property, value string) error {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("meta: parse %s %q: %w", property, value, err)
	}
	if parsed < 0 {
		return fmt.Errorf("meta: parse %s %q: value must be non-negative", property, value)
	}
	return nil
}

func parseIndexMarker(value string) bool {
	if value == "" {
		return true
	}
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
}
