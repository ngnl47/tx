package anno

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ngnl47/gobricks/meta"
)

func Normalize(directives []Directive) (meta.TypeSpec, error) {
	spec := meta.TypeSpec{Fields: map[string]meta.FieldSpec{}}
	for _, directive := range directives {
		if directive.Scope == ScopeField {
			field, err := normalizeFieldDirective(directive)
			if err != nil {
				return meta.TypeSpec{}, err
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
				return meta.TypeSpec{}, err
			}
			spec.Storage = storage
		case "redis":
			redis, err := normalizeRedis(directive)
			if err != nil {
				return meta.TypeSpec{}, err
			}
			spec.Redis = &redis
		case "mongodb":
			mongo, err := normalizeMongo(directive)
			if err != nil {
				return meta.TypeSpec{}, err
			}
			spec.Mongo = &mongo
		default:
			return meta.TypeSpec{}, fmt.Errorf("%w: %s", ErrUnknownTag, directive.Name)
		}
	}
	if spec.Storage.Mode == meta.StorageModeFusion {
		spec.Fusion = &meta.FusionSpec{}
	}
	return spec, nil
}

func normalizeStorage(d Directive) (meta.StorageSpec, error) {
	var out meta.StorageSpec
	for key, value := range d.Params {
		switch key {
		case "mode":
			out.Mode = meta.StorageMode(value)
		case "delaySaveTime", "saveTime", "delaySave":
			out.DelaySave = meta.Duration{Duration: parseDuration(value)}
		case "delayDelTime", "delTime", "delayDelete":
			out.DelayDelete = meta.Duration{Duration: parseDuration(value)}
		default:
			return meta.StorageSpec{}, fmt.Errorf("%w: storage.%s", ErrUnknownParam, key)
		}
	}
	if out.Mode != meta.StorageModeRedis && out.Mode != meta.StorageModeMongo && out.Mode != meta.StorageModeFusion {
		return meta.StorageSpec{}, fmt.Errorf("%w: storage.mode %q", ErrUnsupportedMode, out.Mode)
	}
	return out, nil
}

func normalizeRedis(d Directive) (meta.RedisSpec, error) {
	var out meta.RedisSpec
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
		default:
			return meta.RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, key)
		}
	}
	for flag := range d.Flags {
		switch flag {
		case "omitempty":
			out.OmitEmpty = true
		default:
			return meta.RedisSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, flag)
		}
	}
	return out, nil
}

func normalizeMongo(d Directive) (meta.MongoSpec, error) {
	var out meta.MongoSpec
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
		default:
			return meta.MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, key)
		}
	}
	for flag := range d.Flags {
		switch flag {
		case "enableSharding":
			out.Sharding.Enabled = true
		default:
			return meta.MongoSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, flag)
		}
	}
	return out, nil
}

func normalizeFieldDirective(d Directive) (meta.FieldSpec, error) {
	var out meta.FieldSpec
	switch d.Name {
	case "redis":
		field := &meta.RedisFieldSpec{}
		for key, value := range d.Params {
			switch key {
			case "subKey":
				field.SubKey = value
			default:
				return meta.FieldSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, key)
			}
		}
		for flag := range d.Flags {
			switch flag {
			case "ignore":
				field.Ignore = true
			case "fullUpdate":
				field.FullUpdate = true
			default:
				return meta.FieldSpec{}, fmt.Errorf("%w: redis.%s", ErrUnknownParam, flag)
			}
		}
		out.Redis = field
	case "mongodb":
		field := &meta.MongoFieldSpec{}
		index := meta.IndexSpec{}
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
			default:
				return meta.FieldSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, key)
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
				return meta.FieldSpec{}, fmt.Errorf("%w: mongodb.%s", ErrUnknownParam, flag)
			}
		}
		if hasIndex {
			field.Indexes = append(field.Indexes, index)
		}
		out.Mongo = field
	default:
		return meta.FieldSpec{}, fmt.Errorf("%w: %s", ErrUnknownTag, d.Name)
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
