package meta

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ngnl47/gobricks/anno"
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
			out.DelaySave = Duration{Duration: parseDuration(value)}
		case "delayDelTime", "delTime", "delayDelete":
			out.DelayDelete = Duration{Duration: parseDuration(value)}
		case "memoryCache":
			out.MemoryCache = Duration{Duration: parseDuration(value)}
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
			out.OmitEmpty = parseBool(value)
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
			out.Sharding.Enabled = parseBool(value)
		case "useMongoObjectID":
			out.UseMongoObjectID = parseBool(value)
		default:
			return MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, key)
		}
	}
	for flag := range d.Flags {
		switch flag {
		case "enableSharding":
			out.Sharding.Enabled = true
		case "useMongoObjectID":
			out.UseMongoObjectID = true
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
				field.Ignore = parseBool(value)
			case "fullUpdate":
				field.FullUpdate = parseBool(value)
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
				index.Weight, _ = strconv.Atoi(value)
				hasIndex = true
			case "expire", "expireAfter":
				index.ExpireAfter = value
				hasIndex = true
			case "ignore":
				field.Ignore = parseBool(value)
			case "index":
				hasIndex = hasIndex || parseBool(value)
			case "unique":
				index.Unique = parseBool(value)
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

func parseDuration(value string) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Duration(seconds) * time.Second
	}
	dur, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return dur
}

func parseBool(value string) bool {
	if value == "" {
		return true
	}
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
}
