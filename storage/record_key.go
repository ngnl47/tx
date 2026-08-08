package storage

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ngnl47/tx/meta"
)

// ValidateRecordKey 校验存储操作的稳定记录定位。ID 始终必填；当元数据把
// routeBy 与 idField 分开时，ShardKey 也必须存在，不能悄悄回退为 ID。
func ValidateRecordKey(spec meta.TypeSpec, key RecordKey) error {
	if isMissingRecordKeyValue(key.ID) {
		return fmt.Errorf("%w: missing id", ErrInvalidRecordKey)
	}
	if requiresRecordShardKey(spec) && isMissingRecordKeyValue(key.ShardKey) {
		return fmt.Errorf("%w: missing shard key for distinct route field", ErrInvalidRecordKey)
	}
	return nil
}

// ValidateRecordKeys 校验批量存储操作的全部记录定位。
func ValidateRecordKeys(spec meta.TypeSpec, keys []RecordKey) error {
	for index, key := range keys {
		if err := ValidateRecordKey(spec, key); err != nil {
			return fmt.Errorf("%w: index %d", err, index)
		}
	}
	return nil
}

func requiresRecordShardKey(spec meta.TypeSpec) bool {
	return (spec.Redis != nil && routeRequiresShardKey(spec.Redis.Route, spec.Redis.IDField)) ||
		(spec.Mongo != nil && routeRequiresShardKey(spec.Mongo.Route, spec.Mongo.IDField))
}

func routeRequiresShardKey(route meta.RouteSpec, idField string) bool {
	return route.By != "" && idField != "" && route.By != idField
}

func isMissingRecordKeyValue(value any) bool {
	if value == nil {
		return true
	}
	if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
