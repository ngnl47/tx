// Package redis 提供 ChangeSet 到 Redis 写入模型的转换。
package redis

import (
	"strings"

	"github.com/ngnl47/tx/change"
)

// BuildHashChanges 按顶层路径把字段变更分组成 Redis hash 写入。
//
// 示例：
// Path{"items", "1", "quantity"} 会变成 hash "items" 下的字段
// "1.quantity"。这对应 Mandarin 中 redis subKey 拆分后的局部落地模型。
func BuildHashChanges(set change.ChangeSet) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, fieldChange := range set.Changes() {
		if fieldChange.Kind == change.KindUnset || len(fieldChange.Path) == 0 {
			continue
		}
		hash := fieldChange.Path[0]
		field := strings.Join(fieldChange.Path[1:], ".")
		if field == "" {
			field = hash
		}
		if out[hash] == nil {
			out[hash] = map[string]any{}
		}
		out[hash][field] = fieldChange.Value
	}
	return out
}

// BuildHashDeletes 按顶层路径把 unset 变更分组成 Redis hash 删除操作。
//
// 示例：Path{"items", "1", "quantity"} 会变成 hash "items" 下 HDEL
// 字段 "1.quantity"。
func BuildHashDeletes(set change.ChangeSet) map[string][]string {
	out := map[string][]string{}
	for _, fieldChange := range set.Changes() {
		if fieldChange.Kind != change.KindUnset || len(fieldChange.Path) == 0 {
			continue
		}
		hash := fieldChange.Path[0]
		field := strings.Join(fieldChange.Path[1:], ".")
		if field == "" {
			field = hash
		}
		out[hash] = append(out[hash], field)
	}
	return out
}
