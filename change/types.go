package change

import "strings"

// FieldID 是由 txgen 为结构体字段生成的稳定编号。
type FieldID uint16

const (
	// FieldNone 表示未使用字段编号。
	FieldNone FieldID = 0
)

// NameBackend 表示字段名所属的命名体系。
type NameBackend string

const (
	NameBackendGo      NameBackend = "go"
	NameBackendJSON    NameBackend = "json"
	NameBackendBSON    NameBackend = "bson"
	NameBackendMsgpack NameBackend = "msgpack"
)

// FieldNameResolver 按命名体系把 FieldID 解析为字段名。
//
// FieldID 只在单个生成类型内部稳定，不能放进全局字段名表。生成代码应该
// 为每个类型提供自己的字段名函数。
type FieldNameResolver interface {
	FieldName(FieldID, NameBackend) string
}

// FieldNameResolverFunc 允许用生成的函数直接实现 FieldNameResolver。
type FieldNameResolverFunc func(FieldID, NameBackend) string

func (f FieldNameResolverFunc) FieldName(field FieldID, backend NameBackend) string {
	if f == nil {
		return ""
	}
	return f(field, backend)
}

// Path 是从根对象到变更字段的别名路径。
//
// 例如角色名字段是 Path{"name"}，物品数量可以表示为
// Path{"items", "1001", "quantity"}。
type Path []string

// String 返回点分隔路径，便于转换为 Mongo `$set` 字段或日志输出。
func (p Path) String() string {
	return strings.Join(p, ".")
}

// Equal 判断两个路径是否逐段相等。
func (p Path) Equal(other Path) bool {
	if len(p) != len(other) {
		return false
	}
	for i := range p {
		if p[i] != other[i] {
			return false
		}
	}
	return true
}
