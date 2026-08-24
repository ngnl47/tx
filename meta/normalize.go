package meta

import (
	"fmt"
	"strconv"
	"strings"
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
	for _, argument := range d.Arguments {
		if !argument.HasValue {
			return StorageSpec{}, fmt.Errorf("%w: storage.%s", ErrUnknownParam, argument.Name)
		}
		switch argument.Name {
		case "mode":
			out.Mode = StorageMode(argument.Value)
		case "delaySaveTime", "saveTime", "delaySave":
			duration, err := parseDurationParam("storage."+argument.Name, argument.Value)
			if err != nil {
				return StorageSpec{}, err
			}
			out.DelaySave = Duration{Duration: duration}
		case "delayDelTime", "delTime", "delayDelete":
			duration, err := parseDurationParam("storage."+argument.Name, argument.Value)
			if err != nil {
				return StorageSpec{}, err
			}
			out.DelayDelete = Duration{Duration: duration}
		case "memoryCache":
			duration, err := parseDurationParam("storage."+argument.Name, argument.Value)
			if err != nil {
				return StorageSpec{}, err
			}
			out.MemoryCache = Duration{Duration: duration}
		default:
			return StorageSpec{}, fmt.Errorf("%w: storage.%s", ErrUnknownParam, argument.Name)
		}
	}
	if out.Mode != StorageModeRedis && out.Mode != StorageModeMongo && out.Mode != StorageModeFusion {
		return StorageSpec{}, fmt.Errorf("%w: storage.mode %q", ErrUnsupportedMode, out.Mode)
	}
	return out, nil
}

func normalizeRedis(d anno.Directive) (RedisSpec, error) {
	var out RedisSpec
	for _, argument := range d.Arguments {
		switch argument.Name {
		case "routeType", "shardType", "config":
			if !argument.HasValue {
				return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
			}
			out.Route.Type = argument.Value
		case "routeBy", "shardBy":
			if !argument.HasValue {
				return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
			}
			out.Route.By = argument.Value
		case "idField", "id":
			if !argument.HasValue {
				return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
			}
			out.IDField = argument.Value
		case "key":
			if !argument.HasValue {
				return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
			}
			out.Key = argument.Value
		case "storeType":
			if !argument.HasValue {
				return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
			}
			out.StoreType = argument.Value
		case "omitempty":
			parsed, err := parseBoolArgument("redis."+argument.Name, argument)
			if err != nil {
				return RedisSpec{}, err
			}
			out.OmitEmpty = parsed
		default:
			return RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
		}
	}
	return out, nil
}

func normalizeMongo(d anno.Directive) (MongoSpec, error) {
	var out MongoSpec
	for _, argument := range d.Arguments {
		switch argument.Name {
		case "routeType", "shardType", "config":
			if !argument.HasValue {
				return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
			}
			out.Route.Type = argument.Value
		case "routeBy", "shardBy":
			if !argument.HasValue {
				return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
			}
			out.Route.By = argument.Value
		case "idField", "id":
			if !argument.HasValue {
				return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
			}
			out.IDField = argument.Value
		case "collection", "c", "col":
			if !argument.HasValue {
				return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
			}
			out.Collection = argument.Value
		case "enableSharding":
			parsed, err := parseBoolArgument("mongodb."+argument.Name, argument)
			if err != nil {
				return MongoSpec{}, err
			}
			out.Sharding.Enabled = parsed
		default:
			return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
		}
	}
	return out, nil
}

func normalizeFieldDirective(d anno.Directive) (FieldSpec, error) {
	var out FieldSpec
	switch d.Name {
	case "redis":
		field := &RedisFieldSpec{}
		for _, argument := range d.Arguments {
			switch argument.Name {
			case "subKey":
				if !argument.HasValue {
					return FieldSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
				}
				field.SubKey = argument.Value
			case "ignore":
				parsed, err := parseBoolArgument("redis."+argument.Name, argument)
				if err != nil {
					return FieldSpec{}, err
				}
				field.Ignore = parsed
			case "fullUpdate":
				parsed, err := parseBoolArgument("redis."+argument.Name, argument)
				if err != nil {
					return FieldSpec{}, err
				}
				field.FullUpdate = parsed
			default:
				return FieldSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, argument.Name)
			}
		}
		out.Redis = field
	case "mongodb":
		field := &MongoFieldSpec{}
		currentIndex := -1
		current := func(name string) (*IndexSpec, error) {
			if currentIndex < 0 {
				return nil, fmt.Errorf("meta: mongodb.%s requires a preceding index", name)
			}
			return &field.Indexes[currentIndex], nil
		}
		for _, argument := range d.Arguments {
			switch argument.Name {
			case "index":
				index, enabled, err := parseMongoIndexArgument(argument)
				if err != nil {
					return FieldSpec{}, err
				}
				if !enabled {
					currentIndex = -1
					continue
				}
				field.Indexes = append(field.Indexes, index)
				currentIndex = len(field.Indexes) - 1
			case "order", "indexType":
				if !argument.HasValue {
					return FieldSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
				}
				index, err := current(argument.Name)
				if err != nil {
					return FieldSpec{}, err
				}
				if len(index.Keys) > 0 {
					index.Keys[0].Order = argument.Value
				}
			case "weight":
				index, err := current(argument.Name)
				if err != nil {
					return FieldSpec{}, err
				}
				weights, err := parseMongoWeightArgument(argument)
				if err != nil {
					return FieldSpec{}, err
				}
				index.Weights = append(index.Weights, weights...)
			case "expire", "expireAfter":
				if !argument.HasValue {
					return FieldSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
				}
				index, err := current(argument.Name)
				if err != nil {
					return FieldSpec{}, err
				}
				if err := validateNonNegativeInt64Param("mongodb."+argument.Name, argument.Value); err != nil {
					return FieldSpec{}, err
				}
				expireAfter, _ := strconv.ParseInt(argument.Value, 10, 64)
				index.ExpireAfter = &expireAfter
			case "ignore":
				parsed, err := parseBoolArgument("mongodb."+argument.Name, argument)
				if err != nil {
					return FieldSpec{}, err
				}
				field.Ignore = parsed
			case "unique":
				index, err := current(argument.Name)
				if err != nil {
					return FieldSpec{}, err
				}
				parsed, err := parseBoolArgument("mongodb."+argument.Name, argument)
				if err != nil {
					return FieldSpec{}, err
				}
				index.Unique = parsed
			default:
				return FieldSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, argument.Name)
			}
		}
		out.Mongo = field
	default:
		return FieldSpec{}, fmt.Errorf("%w: %s", ErrUnknownTag, d.Name)
	}
	return out, nil
}

func parseBoolArgument(property string, argument anno.Argument) (bool, error) {
	if !argument.HasValue {
		return true, nil
	}
	return parseBoolParam(property, argument.Value)
}

func parseMongoIndexArgument(argument anno.Argument) (IndexSpec, bool, error) {
	if !argument.HasValue || argument.Value == "" || argument.Value == "true" || argument.Value == "1" {
		return IndexSpec{Keys: []IndexKeySpec{{Order: "1"}}}, true, nil
	}
	if argument.Value == "false" || argument.Value == "0" {
		return IndexSpec{}, false, nil
	}
	parts := strings.Split(argument.Value, ":")
	index := IndexSpec{Name: parts[0]}
	switch len(parts) {
	case 1:
		index.Keys = append(index.Keys, IndexKeySpec{Order: "1"})
	case 2:
		if parts[1] == "" {
			return IndexSpec{}, false, fmt.Errorf("meta: parse mongodb.index %q: index order is empty", argument.Value)
		}
		index.Keys = append(index.Keys, IndexKeySpec{Order: parts[1]})
	default:
		if len(parts)%2 == 0 {
			return IndexSpec{}, false, fmt.Errorf("meta: parse mongodb.index %q: compound keys must use path/order pairs", argument.Value)
		}
		for i := 1; i < len(parts); i += 2 {
			if parts[i] == "" || parts[i+1] == "" {
				return IndexSpec{}, false, fmt.Errorf("meta: parse mongodb.index %q: compound path and order must not be empty", argument.Value)
			}
			index.Keys = append(index.Keys, IndexKeySpec{Path: parts[i], Order: parts[i+1]})
		}
	}
	return index, true, nil
}

func parseMongoWeightArgument(argument anno.Argument) ([]IndexWeightSpec, error) {
	if !argument.HasValue {
		return nil, fmt.Errorf("%w: mongodb.weight", ErrUnknownParam)
	}
	parts := strings.Split(argument.Value, ":")
	if len(parts) == 1 {
		weight, err := parseIntParam("mongodb.weight", parts[0])
		if err != nil {
			return nil, err
		}
		return []IndexWeightSpec{{Weight: weight}}, nil
	}
	if len(parts) < 3 || len(parts)%2 == 0 {
		return nil, fmt.Errorf("meta: parse mongodb.weight %q: compound weights must use name/path/weight", argument.Value)
	}
	weights := make([]IndexWeightSpec, 0, (len(parts)-1)/2)
	for i := 1; i < len(parts); i += 2 {
		if parts[i] == "" {
			return nil, fmt.Errorf("meta: parse mongodb.weight %q: path is empty", argument.Value)
		}
		weight, err := parseIntParam("mongodb.weight", parts[i+1])
		if err != nil {
			return nil, err
		}
		weights = append(weights, IndexWeightSpec{Path: parts[i], Weight: weight})
	}
	return weights, nil
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
