package runtime

import (
	"errors"
	"reflect"
)

var (
	// ErrStorageUnavailable 表示目标存储后端当前不可用。
	ErrStorageUnavailable = errors.New("storage runtime: storage unavailable")
	// ErrNotConfigured 表示调用方尚未装配对应存储能力。
	ErrNotConfigured = errors.New("storage runtime: storage is not configured")
	// ErrWorkerNotConfigured 表示 Fusion 延迟任务消费者尚未装配。
	ErrWorkerNotConfigured = errors.New("storage runtime: delayed task worker is not configured")
)

// WALBackend 标识 WAL 的持久化实现。
type WALBackend string

const (
	// WALBackendMongo 使用已经装配的 Mongo route table 和连接。
	WALBackendMongo WALBackend = "mongo"
)

// WALConfig 配置可选的预写日志。
type WALConfig struct {
	// Backend 选择 WAL 持久化实现。
	Backend WALBackend
	// Mongo 是 Mongo WAL backend 的路由和 collection 参数。
	Mongo WALMongoConfig
}

// WALMongoConfig 指定 WAL 使用的 Mongo routeType 和独立 collection。
type WALMongoConfig struct {
	// RouteType 引用 MongoRoutes 中已经配置的路由。
	RouteType string
	// Collection 是 WAL 记录使用的独立 MongoDB collection。
	Collection string
}

func isNil(value any) bool {
	if value == nil {
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
