package anno

import "reflect"

// Scope 标识注解允许出现的位置。
type Scope string

const (
	// ScopeType 表示注解绑定在类型声明注释上。
	ScopeType Scope = "type"
	// ScopeField 表示注解绑定在结构体字段 tag 上。
	ScopeField Scope = "field"
)

// Definition 声明扫描器允许绑定的一种注解。
//
// Type 记录调用方为该注解预定义的结构体形态。解析器仍然返回原始字符串参数；
// 下游包自行决定如何把这些值转换成 int、bool、time.Duration 或领域类型。
type Definition struct {
	Name  string       // tag 名，例如 "storage"、"redis" 或 "custom"。
	Scope Scope        // tag 在 type 注释还是 field tag 上生效。
	Type  reflect.Type // 该注解的预定义结构体形态。
}

// Definitions 是一次解析或扫描允许识别的注解集合。
type Definitions []Definition

// TypeTag 根据用户定义的结构体创建 type 级注解定义。
//
// 示例：
//
//	type CacheTag struct { TTL int }
//	def := anno.TypeTag[CacheTag]("cache")
//
// 使用该定义的扫描器会接受如下写法：
//
//	// @cache:`ttl:"600"`
func TypeTag[T any](name string) Definition {
	var zero T
	return Definition{Name: name, Scope: ScopeType, Type: reflect.TypeOf(zero)}
}

// FieldTag 根据用户定义的结构体创建 field 级注解定义。
//
// 示例：
//
//	type CacheFieldTag struct { Key string }
//	def := anno.FieldTag[CacheFieldTag]("cache")
//
// 使用该定义的扫描器会接受如下字段 tag：
//
//	Field string `cache:"key:name"`
func FieldTag[T any](name string) Definition {
	var zero T
	return Definition{Name: name, Scope: ScopeField, Type: reflect.TypeOf(zero)}
}

// BuiltinDefinitions 返回 tx 内置的存储相关注解定义。
// 解析或扫描 API 收到空 Definitions 时会使用该集合，以兼容默认存储元数据扫描。
func BuiltinDefinitions() Definitions {
	return Definitions{
		TypeTag[StorageTag]("storage"),
		TypeTag[RedisTag]("redis"),
		TypeTag[MongoDBTag]("mongodb"),
		FieldTag[RedisFieldTag]("redis"),
		FieldTag[MongoDBFieldTag]("mongodb"),
	}
}

// IsZero 判断是否未提供任何注解定义。
func (defs Definitions) IsZero() bool {
	return len(defs) == 0
}

// WithBuiltinsIfEmpty 在 defs 为空时返回 BuiltinDefinitions。
func (defs Definitions) WithBuiltinsIfEmpty() Definitions {
	if len(defs) == 0 {
		return BuiltinDefinitions()
	}
	return defs
}

// Has 判断是否存在指定名称和作用域的注解定义。
func (defs Definitions) Has(name string, scope Scope) bool {
	for _, def := range defs {
		if def.Name == name && def.Scope == scope {
			return true
		}
	}
	return false
}

// Argument 表示注解中的一个有序参数。
// HasValue 为 false 时表示裸 flag，例如 mongodb:"index"；为 true 时表示
// 键值参数，例如 mongodb:"index:p_id"。Arguments 允许同名参数重复出现。
type Argument struct {
	Name     string
	Value    string
	HasValue bool
}

// Directive 表示一个已经解析并绑定到 type 或 field 的注解实例。
//
// Arguments 保留声明顺序、重复项以及键值参数和裸 flag 的区别。
// Field 只会在 AST 绑定 field 级注解后设置，值为 Go 字段名。
type Directive struct {
	Name      string     // 注解名称。
	Scope     Scope      // type 或 field 作用域。
	Arguments []Argument // 有序参数。
	Field     string     // field 级注解绑定到的 Go 字段名。
}

// Param 返回最后一个同名键值参数。需要消费全部重复值时应直接遍历 Arguments。
func (d Directive) Param(name string) (string, bool) {
	for i := len(d.Arguments) - 1; i >= 0; i-- {
		argument := d.Arguments[i]
		if argument.Name == name && argument.HasValue {
			return argument.Value, true
		}
	}
	return "", false
}

// HasFlag 判断是否包含指定裸 flag。
func (d Directive) HasFlag(name string) bool {
	for _, argument := range d.Arguments {
		if argument.Name == name && !argument.HasValue {
			return true
		}
	}
	return false
}

// StorageTag 是内置的 type 级存储模式注解定义。
type StorageTag struct {
	Mode          string // redis、mongo 或 fusion。
	DelaySaveTime string // 保存前延迟，后续解析为 duration。
	DelayDelTime  string // 删除前延迟，后续解析为 duration。
}

// RedisTag 是内置的 type 级 Redis 存储注解定义。
type RedisTag struct {
	RouteType string // 路由配置名称。
	RouteBy   string // 作为分片/hash key 的字段别名。
	IDField   string // 作为逻辑对象 id 的字段别名。
	Key       string // Redis key 前缀或名称。
	OmitEmpty bool   // 是否跳过空值。
	StoreType string // Redis 表示形式，例如 string/hash。
}

// MongoDBTag 是内置的 type 级 MongoDB 注解定义。
type MongoDBTag struct {
	RouteType      string // 路由配置名称。
	RouteBy        string // 作为 shard key 的字段别名。
	IDField        string // 作为逻辑对象 id 的字段别名。
	Collection     string // MongoDB collection 名称。
	EnableSharding bool   // filter 中是否包含分片字段。
}

// RedisFieldTag 是内置的 field 级 Redis 注解定义。
type RedisFieldTag struct {
	SubKey     string // hash 类表示中的 Redis 子 key。
	Ignore     bool   // 该字段是否不写入 Redis。
	FullUpdate bool   // 更新时是否替换完整子 key 值。
}

// MongoDBFieldTag 是内置的 field 级 MongoDB 注解定义。
type MongoDBFieldTag struct {
	Index       bool   // 是否创建非唯一索引。
	Unique      bool   // 是否创建唯一索引。
	Ignore      bool   // 该字段是否不写入 MongoDB。
	Order       string // 索引顺序或类型别名。
	IndexType   string // 旧版索引顺序或类型别名。
	Weight      int    // 文本索引权重。
	Expire      string // TTL 时长别名。
	ExpireAfter string // TTL 时长别名。
}
