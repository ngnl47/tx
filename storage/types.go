package storage

type Mode string

const (
	ModeRedis  Mode = "redis"
	ModeMongo  Mode = "mongo"
	ModeFusion Mode = "fusion"
)

type TypeConfig struct {
	TypeName    string                 `json:"typeName"`
	Mode        Mode                   `json:"mode"`
	DelaySave   string                 `json:"delaySave,omitempty"`
	DelayDelete string                 `json:"delayDelete,omitempty"`
	MemoryCache string                 `json:"memoryCache,omitempty"`
	Redis       *RedisConfig           `json:"redis,omitempty"`
	Mongo       *MongoConfig           `json:"mongo,omitempty"`
	Fusion      *FusionConfig          `json:"fusion,omitempty"`
	Fields      map[string]FieldConfig `json:"fields,omitempty"`
}

type RouteConfig struct {
	Type string `json:"type"`
	By   string `json:"by"`
}

type RedisConfig struct {
	Route     RouteConfig `json:"route"`
	ID        string      `json:"id"`
	Key       string      `json:"key,omitempty"`
	OmitEmpty bool        `json:"omitEmpty,omitempty"`
	StoreType string      `json:"storeType,omitempty"`
}

type MongoConfig struct {
	Route            RouteConfig    `json:"route"`
	ID               string         `json:"id"`
	Collection       string         `json:"collection"`
	Sharding         ShardingConfig `json:"sharding,omitempty"`
	UseMongoObjectID bool           `json:"useMongoObjectID,omitempty"`
}

type FusionConfig struct{}

type ShardingConfig struct {
	Enabled bool `json:"enabled,omitempty"`
}

type FieldConfig struct {
	Redis *RedisFieldConfig `json:"redis,omitempty"`
	Mongo *MongoFieldConfig `json:"mongo,omitempty"`
}

type RedisFieldConfig struct {
	SubKey     string `json:"subKey,omitempty"`
	Ignore     bool   `json:"ignore,omitempty"`
	FullUpdate bool   `json:"fullUpdate,omitempty"`
}

type MongoFieldConfig struct {
	Ignore  bool          `json:"ignore,omitempty"`
	Indexes []IndexConfig `json:"indexes,omitempty"`
}

type IndexConfig struct {
	Unique      bool   `json:"unique,omitempty"`
	Order       string `json:"order,omitempty"`
	Weight      int    `json:"weight,omitempty"`
	ExpireAfter string `json:"expireAfter,omitempty"`
}
