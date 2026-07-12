package schema

import (
	"encoding/json"
	"fmt"
	"time"
)

type StorageMode string

const (
	StorageModeRedis  StorageMode = "redis"
	StorageModeMongo  StorageMode = "mongo"
	StorageModeFusion StorageMode = "fusion"
)

type Document struct {
	Version int                 `json:"version"`
	Types   map[string]TypeSpec `json:"types"`
}

type TypeSpec struct {
	Storage StorageSpec          `json:"storage"`
	Redis   *RedisSpec           `json:"redis,omitempty"`
	Mongo   *MongoSpec           `json:"mongo,omitempty"`
	Fusion  *FusionSpec          `json:"fusion,omitempty"`
	Fields  map[string]FieldSpec `json:"fields,omitempty"`
}

type StorageSpec struct {
	Mode        StorageMode `json:"mode"`
	DelaySave   Duration    `json:"delaySave,omitempty"`
	DelayDelete Duration    `json:"delayDelete,omitempty"`
}

type RedisSpec struct {
	Route     RouteSpec `json:"route"`
	ID        string    `json:"id"`
	Key       string    `json:"key,omitempty"`
	OmitEmpty bool      `json:"omitEmpty,omitempty"`
	StoreType string    `json:"storeType,omitempty"`
}

type MongoSpec struct {
	Route      RouteSpec    `json:"route"`
	ID         string       `json:"id"`
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
		return fmt.Errorf("schema: parse duration %q: %w", raw, err)
	}
	d.Duration = parsed
	return nil
}
