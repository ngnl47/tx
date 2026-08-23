package meta

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// TypeRef 在不依赖运行时反射的情况下标识一个 Go 类型。
type TypeRef struct {
	ImportPath string
	Package    string
	Name       string
	FullName   string
}

func (ref TypeRef) normalized() TypeRef {
	ref.ImportPath = strings.TrimSpace(ref.ImportPath)
	ref.Package = strings.TrimSpace(ref.Package)
	ref.Name = strings.TrimSpace(ref.Name)
	ref.FullName = strings.TrimSpace(ref.FullName)
	if ref.FullName == "" {
		ref.FullName = ref.defaultFullName()
	}
	return ref
}

func (ref TypeRef) defaultFullName() string {
	switch {
	case ref.ImportPath != "" && ref.Name != "":
		return ref.ImportPath + "." + ref.Name
	case ref.Package != "" && ref.Name != "":
		return ref.Package + "." + ref.Name
	default:
		return ref.Name
	}
}

func (ref TypeRef) key() string {
	return ref.normalized().FullName
}

// Field 描述 Go 结构体字段及各命名体系使用的字段名。
type Field struct {
	Name        string
	Alias       string
	JSONName    string
	BSONName    string
	MsgpackName string
	Type        string
	Exported    bool
	Embedded    bool
}

// Method 描述从类型上扫描到的 Go 方法。
type Method struct {
	Name     string
	Exported bool
}

// Type 保存 Go 类型的结构元数据。
type Type struct {
	Ref     TypeRef
	Fields  []Field
	Methods []Method
}

// TypeSpec 是按类型注册的元数据单元。
// 从 registry 或生成代码取得后如需修改，应先通过 CloneTypeSpec 获取隔离副本。
type TypeSpec struct {
	Type            Type
	DocumentFactory func() any
	Storage         StorageSpec
	Redis           *RedisSpec
	Mongo           *MongoSpec
	Fusion          *FusionSpec
	Fields          map[string]FieldSpec
}

func (spec TypeSpec) FieldByName(name string) (Field, bool) {
	for _, field := range spec.Type.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return Field{}, false
}

// FieldByJSONName 按公共 JSON 文档字段名查找 Go 字段元数据。
func (spec TypeSpec) FieldByJSONName(name string) (Field, bool) {
	for _, field := range spec.Type.Fields {
		if field.JSONName == name {
			return field, true
		}
	}
	return Field{}, false
}

func (spec TypeSpec) JSONName(name string) string {
	field, ok := spec.FieldByName(name)
	if !ok || field.JSONName == "" {
		return name
	}
	return field.JSONName
}

func (spec TypeSpec) BSONName(name string) string {
	field, ok := spec.FieldByName(name)
	if !ok || field.BSONName == "" {
		return name
	}
	return field.BSONName
}

func (spec TypeSpec) MsgpackName(name string) string {
	field, ok := spec.FieldByName(name)
	if !ok || field.MsgpackName == "" {
		return name
	}
	return field.MsgpackName
}

// CloneTypeSpec 返回可变类型元数据的独立副本。
func CloneTypeSpec(spec TypeSpec) TypeSpec {
	spec.Type.Fields = append([]Field(nil), spec.Type.Fields...)
	spec.Type.Methods = append([]Method(nil), spec.Type.Methods...)
	if spec.Redis != nil {
		redis := *spec.Redis
		spec.Redis = &redis
	}
	if spec.Mongo != nil {
		mongo := *spec.Mongo
		spec.Mongo = &mongo
	}
	if spec.Fusion != nil {
		fusion := *spec.Fusion
		spec.Fusion = &fusion
	}
	if spec.Fields != nil {
		fields := make(map[string]FieldSpec, len(spec.Fields))
		for name, field := range spec.Fields {
			if field.Redis != nil {
				redis := *field.Redis
				field.Redis = &redis
			}
			if field.Mongo != nil {
				mongo := *field.Mongo
				mongo.Indexes = append([]IndexSpec(nil), field.Mongo.Indexes...)
				field.Mongo = &mongo
			}
			fields[name] = field
		}
		spec.Fields = fields
	}
	return spec
}

type StorageMode string

const (
	StorageModeRedis  StorageMode = "redis"
	StorageModeMongo  StorageMode = "mongo"
	StorageModeFusion StorageMode = "fusion"
)

type StorageSpec struct {
	Mode        StorageMode `json:"mode"`
	DelaySave   Duration    `json:"delaySave,omitempty"`
	DelayDelete Duration    `json:"delayDelete,omitempty"`
	MemoryCache Duration    `json:"memoryCache,omitempty"`
}

type RedisSpec struct {
	Route     RouteSpec `json:"route"`
	IDField   string    `json:"idField"`
	Key       string    `json:"key,omitempty"`
	OmitEmpty bool      `json:"omitEmpty,omitempty"`
	StoreType string    `json:"storeType,omitempty"`
}

type MongoSpec struct {
	Route      RouteSpec    `json:"route"`
	IDField    string       `json:"idField"`
	Collection string       `json:"collection"`
	Sharding   ShardingSpec `json:"sharding,omitempty"`
}

type FusionSpec struct{}

type RouteSpec struct {
	Type string `json:"type"`
	By   string `json:"by"`
}

type ShardingSpec struct {
	Enabled bool `json:"enabled,omitempty"`
}

type FieldSpec struct {
	Redis *RedisFieldSpec `json:"redis,omitempty"`
	Mongo *MongoFieldSpec `json:"mongo,omitempty"`
}

type RedisFieldSpec struct {
	SubKey     string `json:"subKey,omitempty"`
	Ignore     bool   `json:"ignore,omitempty"`
	FullUpdate bool   `json:"fullUpdate,omitempty"`
}

type MongoFieldSpec struct {
	Ignore  bool        `json:"ignore,omitempty"`
	Indexes []IndexSpec `json:"indexes,omitempty"`
}

type IndexSpec struct {
	Unique      bool   `json:"unique,omitempty"`
	Order       string `json:"order,omitempty"`
	Weight      int    `json:"weight,omitempty"`
	ExpireAfter string `json:"expireAfter,omitempty"`
}

type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	if d.Duration == 0 {
		return []byte(`""`), nil
	}
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw == "" {
		d.Duration = 0
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("meta: parse duration %q: %w", raw, err)
	}
	d.Duration = parsed
	return nil
}
