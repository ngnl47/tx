package storage

import (
	"context"
	"reflect"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// ApplyOptions 描述变更落地时可选的通用写入语义。
type ApplyOptions struct {
	Upsert bool
	TTL    int64
}

// DeltaApplier 定义支持 DeltaSet 直连落地的适配器。
type DeltaApplier interface {
	ApplyDeltasWithOptions(context.Context, meta.TypeSpec, RecordKey, change.DeltaSet, change.FieldNameResolver, change.DeltaValueResolver, ApplyOptions) error
}

// DeltaStore 描述一个可由 storage 通用写入入口落地的变更 Store。
type DeltaStore interface {
	TypeSpec() meta.TypeSpec
	RecordKey() RecordKey
	Deltas() change.DeltaSet
	FieldNameResolver() change.FieldNameResolver
	DeltaValueResolver() change.DeltaValueResolver
	DefaultApplyOptions() ApplyOptions
	AllTouch()
	ClearDeltas()
}

// ReadOnlyDeltaStore 标记只可用于投影或下行、不可持久化的 Store。
// 生成的 viewStore 实现该接口；普通 Store 无需额外实现。
type ReadOnlyDeltaStore interface {
	IsViewStore() bool
}

// StoreProvider 描述能暴露生成 Store 的数据对象。
type StoreProvider[S DeltaStore] interface {
	Store() S
}

// Create 全字段创建一条记录。创建语义强制 Upsert=true，成功后清理 Store 变更。
func Create(ctx context.Context, applier DeltaApplier, store DeltaStore) error {
	return CreateWithOptions(ctx, applier, store, ApplyOptions{})
}

// CreateWithOptions 全字段创建一条记录，并允许框架调用方覆盖 TTL 等写入选项。
func CreateWithOptions(ctx context.Context, applier DeltaApplier, store DeltaStore, options ApplyOptions) error {
	if applier == nil {
		return ErrNilDeltaApplier
	}
	if isNilDeltaStore(store) {
		return ErrNilDeltaStore
	}
	if isReadOnlyDeltaStore(store) {
		return ErrReadOnlyStore
	}
	store.AllTouch()
	options = mergeApplyOptions(store.DefaultApplyOptions(), options)
	options.Upsert = true
	if err := applier.ApplyDeltasWithOptions(ctx, store.TypeSpec(), store.RecordKey(), store.Deltas(), store.FieldNameResolver(), store.DeltaValueResolver(), options); err != nil {
		return err
	}
	store.ClearDeltas()
	return nil
}

// Save 保存 Store 当前记录的 DeltaSet。保存语义强制 Upsert=false，成功后清理 Store 变更。
func Save(ctx context.Context, applier DeltaApplier, store DeltaStore) error {
	return SaveWithOptions(ctx, applier, store, ApplyOptions{})
}

// SaveWithOptions 保存 Store 当前记录的 DeltaSet，并允许框架调用方覆盖 TTL 等写入选项。
func SaveWithOptions(ctx context.Context, applier DeltaApplier, store DeltaStore, options ApplyOptions) error {
	if isNilDeltaStore(store) {
		return ErrNilDeltaStore
	}
	return SaveDeltasWithOptions(ctx, applier, store, store.Deltas(), options)
}

// SaveDeltas 保存已投影的 DeltaSet。它供状态调度等框架内部路径复用同一份
// 投影结果，避免在一次落盘中重复调用 Store.Deltas。空 DeltaSet 不触发 I/O。
func SaveDeltas(ctx context.Context, applier DeltaApplier, store DeltaStore, deltas change.DeltaSet) error {
	return SaveDeltasWithOptions(ctx, applier, store, deltas, ApplyOptions{})
}

// SaveDeltasWithOptions 保存已投影的 DeltaSet，并允许框架调用方覆盖 TTL 等写入选项。
func SaveDeltasWithOptions(ctx context.Context, applier DeltaApplier, store DeltaStore, deltas change.DeltaSet, options ApplyOptions) error {
	if applier == nil {
		return ErrNilDeltaApplier
	}
	if isNilDeltaStore(store) {
		return ErrNilDeltaStore
	}
	if isReadOnlyDeltaStore(store) {
		return ErrReadOnlyStore
	}
	if len(deltas) == 0 {
		return nil
	}
	options = mergeApplyOptions(store.DefaultApplyOptions(), options)
	options.Upsert = false
	if err := applier.ApplyDeltasWithOptions(ctx, store.TypeSpec(), store.RecordKey(), deltas, store.FieldNameResolver(), store.DeltaValueResolver(), options); err != nil {
		return err
	}
	store.ClearDeltas()
	return nil
}

func mergeApplyOptions(defaults ApplyOptions, explicit ApplyOptions) ApplyOptions {
	out := explicit
	if out.TTL == 0 {
		out.TTL = defaults.TTL
	}
	return out
}

func isNilDeltaStore(store DeltaStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isReadOnlyDeltaStore(store DeltaStore) bool {
	readOnly, ok := store.(ReadOnlyDeltaStore)
	return ok && readOnly.IsViewStore()
}

// DocumentLoader 定义按记录键读取单条文档的适配器。
type DocumentLoader interface {
	LoadDocument(context.Context, meta.TypeSpec, RecordKey, ...string) (map[string]any, bool, error)
}

// BatchDocumentLoader 定义按记录键批量读取文档的适配器。
type BatchDocumentLoader interface {
	LoadDocuments(context.Context, meta.TypeSpec, []RecordKey, ...string) ([]map[string]any, []bool, error)
}

// DocumentReplacerWithOptions 定义按记录键整文档替换且支持写入选项的适配器。
type DocumentReplacerWithOptions interface {
	ReplaceDocumentWithOptions(context.Context, meta.TypeSpec, RecordKey, map[string]any, ApplyOptions) error
}

// DocumentReplacer 定义按记录键整文档替换的适配器。
type DocumentReplacer interface {
	ReplaceDocument(context.Context, meta.TypeSpec, RecordKey, map[string]any) error
}

// LockOptions 描述记录级分布式锁的等待策略。
type LockOptions struct {
	TTL  time.Duration
	Wait time.Duration
}

// RecordLock 表示已持有的一条记录级锁。
type RecordLock interface {
	Unlock(context.Context) error
}

// RecordLocker 定义按记录键加锁的适配器。
type RecordLocker interface {
	LockRecord(context.Context, meta.TypeSpec, RecordKey, LockOptions) (RecordLock, error)
}

// DeleteApplier 定义按元数据和记录键删除存储记录的适配器。
type DeleteApplier interface {
	ApplyDelete(context.Context, meta.TypeSpec, RecordKey) error
}

// RevisionDeleteApplier 定义带 Redis 生命周期版本校验的删除适配器。
type RevisionDeleteApplier interface {
	ApplyDeleteWithRevision(context.Context, meta.TypeSpec, RecordKey, string) (bool, string, error)
}

// RecordRevisionReader 定义读取存储记录生命周期版本的适配器。
type RecordRevisionReader interface {
	ReadRecordRevision(context.Context, meta.TypeSpec, RecordKey) (string, error)
}
