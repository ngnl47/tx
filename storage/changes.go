package storage

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// ApplyOptions 描述变更落地时可选的通用写入语义。
type ApplyOptions struct {
	Upsert     bool
	TTLSeconds int64
}

// ChangeApplier 定义支持规范化变更流直连落地的适配器。
// TypeSpec 参数是只读元数据；适配器不得修改其中的 map、slice 或指针成员。
// nil Source 必须返回 change.ErrNilSource；只有有效 Source 明确报告无变更时
// 才能作为不执行 I/O 的成功 no-op。
// 适配器必须在第一次外部 I/O 前完整消费 Source 并处理遍历错误，不能把
// visitor 中途返回的错误变成部分写入。
type ChangeApplier interface {
	ApplyChangesWithOptions(context.Context, meta.TypeSpec, RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, ApplyOptions) error
}

// ChangeStore 描述一个可由 storage 通用写入入口落地的变更 Store。
type ChangeStore interface {
	change.Source
	change.ValueResolver
	TypeSpec() meta.TypeSpec
	RecordKey() RecordKey
	FieldNameResolver() change.FieldNameResolver
	DefaultApplyOptions() ApplyOptions
	AllTouch()
	ClearChanges()
}

// CollapsibleChangeStore 描述能把细粒度变更收敛为顶层字段替换的持久化 Store。
// txn.Runtime 配置单根变更容量上限时要求持久化 Store 实现该契约。
type CollapsibleChangeStore interface {
	ChangeStore
	CollapseChanges()
}

// StoreProvider 描述能暴露生成 Store 的数据对象。
type StoreProvider[S ChangeStore] interface {
	Store() S
}

// Create 全字段创建一条记录。创建语义强制 Upsert=true，成功后清理 Store 变更。
func Create(ctx context.Context, applier ChangeApplier, store ChangeStore) error {
	return CreateWithOptions(ctx, applier, store, ApplyOptions{})
}

// CreateWithOptions 全字段创建一条记录，并允许框架调用方覆盖 TTL 等写入选项。
func CreateWithOptions(ctx context.Context, applier ChangeApplier, store ChangeStore, options ApplyOptions) error {
	if isNilChangeApplier(applier) {
		return ErrNilChangeApplier
	}
	if isNilChangeStore(store) {
		return ErrNilChangeStore
	}
	key := store.RecordKey()
	if err := ValidateRecordKey(store.TypeSpec(), key); err != nil {
		return err
	}
	store.AllTouch()
	options = mergeApplyOptions(store.DefaultApplyOptions(), options)
	options.Upsert = true
	if err := applier.ApplyChangesWithOptions(ctx, store.TypeSpec(), key, store, store.FieldNameResolver(), store, options); err != nil {
		return err
	}
	store.ClearChanges()
	return nil
}

// Save 保存 Store 当前记录的规范化变更。保存语义强制 Upsert=false，成功后清理 Store 变更。
func Save(ctx context.Context, applier ChangeApplier, store ChangeStore) error {
	return SaveWithOptions(ctx, applier, store, ApplyOptions{})
}

// SaveWithOptions 保存 Store 当前记录的规范化变更，并允许框架调用方覆盖 TTL 等写入选项。
func SaveWithOptions(ctx context.Context, applier ChangeApplier, store ChangeStore, options ApplyOptions) error {
	if isNilChangeApplier(applier) {
		return ErrNilChangeApplier
	}
	if isNilChangeStore(store) {
		return ErrNilChangeStore
	}
	if !store.HasChanges() {
		return nil
	}
	key := store.RecordKey()
	if err := ValidateRecordKey(store.TypeSpec(), key); err != nil {
		return err
	}
	options = mergeApplyOptions(store.DefaultApplyOptions(), options)
	options.Upsert = false
	if err := applier.ApplyChangesWithOptions(ctx, store.TypeSpec(), key, store, store.FieldNameResolver(), store, options); err != nil {
		return err
	}
	store.ClearChanges()
	return nil
}

func mergeApplyOptions(defaults ApplyOptions, explicit ApplyOptions) ApplyOptions {
	out := explicit
	if out.TTLSeconds == 0 {
		out.TTLSeconds = defaults.TTLSeconds
	}
	return out
}

func isNilChangeStore(store ChangeStore) bool {
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

func isNilChangeApplier(applier ChangeApplier) bool {
	if applier == nil {
		return true
	}
	value := reflect.ValueOf(applier)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// DocumentLoader 定义按记录键读取单条文档的适配器。
//
// TypeSpec 参数是只读元数据；适配器不得修改其中的 map、slice 或指针成员。
// 返回值必须满足：exists=true 时 document 非 nil；exists=false 时 document 为 nil。
type DocumentLoader interface {
	LoadDocument(context.Context, meta.TypeSpec, RecordKey, ...string) (map[string]any, bool, error)
}

// BatchDocumentLoader 定义按记录键批量读取文档的适配器。
// 每个 documents[i] 与 exists[i] 都遵循 DocumentLoader 的结果约束。
type BatchDocumentLoader interface {
	LoadDocuments(context.Context, meta.TypeSpec, []RecordKey, ...string) ([]map[string]any, []bool, error)
}

// ValidateDocumentResult 校验 DocumentLoader 的单条结果是否满足接口约束。
func ValidateDocumentResult(document map[string]any, exists bool) error {
	if exists && document == nil {
		return fmt.Errorf("storage: document loader returned existing document without data")
	}
	if !exists && document != nil {
		return fmt.Errorf("storage: document loader returned missing document with data")
	}
	return nil
}

// ValidateDocumentResults 校验 BatchDocumentLoader 的批量结果是否满足接口约束。
func ValidateDocumentResults(documents []map[string]any, exists []bool) error {
	if len(documents) != len(exists) {
		return fmt.Errorf("storage: document loader returned documents=%d exists=%d", len(documents), len(exists))
	}
	for i, found := range exists {
		if err := ValidateDocumentResult(documents[i], found); err != nil {
			return fmt.Errorf("storage: document loader result at index %d: %w", i, err)
		}
	}
	return nil
}

// DocumentReplacerWithOptions 定义按记录键整文档替换且支持写入选项的适配器。
// document 必须非 nil；空 map 表示合法的空 JSON 对象。
type DocumentReplacerWithOptions interface {
	ReplaceDocumentWithOptions(context.Context, meta.TypeSpec, RecordKey, map[string]any, ApplyOptions) error
}

// DocumentReplacer 定义按记录键整文档替换的适配器。
// document 必须非 nil；空 map 表示合法的空 JSON 对象。
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
