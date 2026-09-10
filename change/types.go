package change

import (
	"slices"
	"strings"
)

// FieldID 是由 txgen 为结构体字段生成的稳定编号。
type FieldID uint16

const (
	// FieldNone 表示未使用字段编号。
	FieldNone FieldID = 0
)

// FieldNameKind 表示字段名所属的命名体系。
type FieldNameKind string

const (
	FieldNameGo      FieldNameKind = "go"
	FieldNameJSON    FieldNameKind = "json"
	FieldNameBSON    FieldNameKind = "bson"
	FieldNameMsgpack FieldNameKind = "msgpack"
)

// Valid 判断字段名体系是否为运行时支持的固定集合。
func (k FieldNameKind) Valid() bool {
	switch k {
	case FieldNameGo, FieldNameJSON, FieldNameBSON, FieldNameMsgpack:
		return true
	default:
		return false
	}
}

// FieldNameResolver 按命名体系把 FieldID 解析为字段名。
//
// FieldID 只在单个生成类型内部稳定，不能放进全局字段名表。生成代码应该
// 为每个类型提供自己的字段名函数。
type FieldNameResolver interface {
	FieldName(FieldID, FieldNameKind) string
}

// PathFieldNameResolver 可按完整 Path 和节点位置解析字段名。
//
// 嵌套 Path 中的 FieldID 只在对应节点的数据类型内部稳定。生成 Store
// 知道父字段指向的子类型时，可以实现该接口，为子节点切换到子类型字段名。
type PathFieldNameResolver interface {
	PathFieldName(Path, int, FieldNameKind) string
}

// FieldNameResolverFunc 允许用生成的函数直接实现 FieldNameResolver。
type FieldNameResolverFunc func(FieldID, FieldNameKind) string

func (f FieldNameResolverFunc) FieldName(field FieldID, kind FieldNameKind) string {
	if f == nil {
		return ""
	}
	return f(field, kind)
}

// DocumentPath 是从根对象到变更字段的已解析文档路径。
//
// 例如角色名字段是 DocumentPath{"name"}，物品数量可以表示为
// DocumentPath{"items", "1001", "quantity"}。
type DocumentPath []string

// String 返回点分隔路径，便于转换为 Mongo `$set` 字段或日志输出。
func (p DocumentPath) String() string {
	return strings.Join(p, ".")
}

// Equal 判断两个路径是否逐段相等。
func (p DocumentPath) Equal(other DocumentPath) bool {
	return slices.Equal(p, other)
}
