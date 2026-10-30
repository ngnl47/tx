package entitycache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

// Descriptor 由生成器提供完整根加载与 Store 访问，不保存运行期对象。
type Descriptor struct {
	// TypeName 是生成类型名，仅用于管理元数据，不参与缓存身份。
	TypeName string
	Load     func(context.Context, storage.RecordKey) (any, bool, error)
	Store    func(any) storage.ChangeStore
	// Clone 只复制业务字段，不共享子对象，也不复制 Store、追踪器或事务状态。
	Clone func(any) (any, error)
}

// Described 由启用缓存的生成根实现，创建和删除与查询复用同一描述。
type Described interface{ EntityDescriptor() *Descriptor }

// Config 为本运行时全部实体设置统一回收预算，不按业务类型维护登记清单。
type Config struct {
	MaximumSize int
	IdleTimeout time.Duration
}

type identity struct {
	descriptor *Descriptor
	record     storage.RecordKey
}
type entity struct {
	id       identity
	value    any
	removing *txn.Tx
	removed  bool
	deleting bool
}

// Runtime 维护一次应用内的实体身份；后端与应用实例通过运行时边界隔离。
type Runtime struct {
	cache      *Cache[identity, *entity]
	management *managementGate
}

type runtimeKey struct{}

// Bind 将同一应用拥有的实体运行时放入操作上下文，不建立全局默认实例。
func Bind(ctx context.Context, runtime *Runtime) context.Context {
	return context.WithValue(ctx, runtimeKey{}, runtime)
}

// FromContext 只返回显式绑定的运行时。
func FromContext(ctx context.Context) *Runtime {
	r, _ := ctx.Value(runtimeKey{}).(*Runtime)
	return r
}

// NewRuntime 的 release 只负责持久化安全判定，不能代替业务并发保护。
func NewRuntime(config Config, release func(context.Context, any) (bool, error)) (*Runtime, error) {
	if config.MaximumSize == 0 {
		config.MaximumSize = 10000
	}
	runtime := &Runtime{management: new(managementGate)}
	cache, err := NewCache(func(id identity) string {
		return fmt.Sprintf("%p/%T:%v/%T:%v", id.descriptor, id.record.ID, id.record.ID, id.record.ShardKey, id.record.ShardKey)
	}, func(ctx context.Context, id identity) (*entity, bool, error) {
		value, exists, err := id.descriptor.Load(ctx, id.record)
		if err != nil || !exists {
			return nil, exists, err
		}
		lockable, ok := value.(txn.StoreProvider)
		if !ok {
			return nil, false, fmt.Errorf("entity cache: %T is not declared transactional", value)
		}
		// 生成 Store 会在首次获取锁目标时惰性初始化；发布后只能读取这个目标。
		lockable.StoreTarget()
		return &entity{id: id, value: value}, true, nil
	}, Options[*entity]{MaximumSize: config.MaximumSize, IdleTimeout: config.IdleTimeout, TryRelease: func(ctx context.Context, e *entity) (bool, error) {
		runtime.cache.mu.RLock()
		deleting := e.deleting
		runtime.cache.mu.RUnlock()
		if deleting {
			return false, nil
		}
		if release != nil {
			return release(ctx, e.value)
		}
		if e.id.descriptor.Store != nil {
			var clean bool
			err := txn.WithLocks(ctx, txn.Lockeys{e.value.(txn.StoreProvider).StoreTarget().Lock}, func(context.Context) error { clean = !e.id.descriptor.Store(e.value).HasChanges(); return nil })
			return clean, err
		}
		return true, nil
	}})
	if err != nil {
		return nil, err
	}
	runtime.cache = cache
	return runtime, nil
}

func (r *Runtime) Start(tasks lifecycle.Tasks) error {
	if tasks == nil {
		return errors.New("entity cache: lifecycle tasks are nil")
	}
	return tasks.Go(func(ctx context.Context) {
		defer r.CloseManagement(context.Background())
		r.cache.runReleaseWorker(ctx)
	})
}

// ReserveCreate 按生成描述自动找到对应缓存，不维护业务类型清单。
func (r *Runtime) ReserveCreate(ctx context.Context, value any) (txn.CreationReservation, error) {
	if _, err := requireProcedure(ctx); err != nil {
		return nil, err
	}
	described, ok := value.(Described)
	if !ok {
		return nil, fmt.Errorf("entity cache: %T has no generated descriptor", value)
	}
	d := described.EntityDescriptor()
	key := d.Store(value).RecordKey()
	if err := storage.ValidateRecordKey(d.Store(value).TypeSpec(), key); err != nil {
		return nil, err
	}
	e := &entity{id: identity{descriptor: d, record: key}, value: value}
	return r.cache.Creation(func(e *entity) (identity, error) {
		return identity{descriptor: e.id.descriptor, record: e.id.descriptor.Store(e.value).RecordKey()}, nil
	}).ReserveCreate(ctx, e)
}

// Find 只在有效 Procedure 内返回共享可变对象；使用权和记录锁保留到事务结束。
func (r *Runtime) Find(ctx context.Context, descriptor *Descriptor, key storage.RecordKey) (any, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("entity cache: runtime is not bound")
	}
	tx, err := requireProcedure(ctx)
	if err != nil {
		return nil, false, err
	}
	entry, exists, err := r.cache.Find(ctx, identity{descriptor: descriptor, record: key})
	if err != nil || !exists {
		return nil, exists, err
	}
	r.cache.mu.RLock()
	removedBefore := entry.removed
	r.cache.mu.RUnlock()
	if removedBefore {
		return nil, false, nil
	}
	if err := tx.Enlist(entry.value.(txn.StoreProvider).StoreTarget()); err != nil {
		return nil, false, err
	}
	return r.visible(ctx, entry)
}

// ErrProcedureRequired 表示调用方缺少共享可变 GD 所需的有效事务范围。
var ErrProcedureRequired = errors.New("entity cache: shared mutable GD requires an active Procedure")

func requireProcedure(ctx context.Context) (*txn.Tx, error) {
	tx, active := txn.FromContext(ctx)
	if tx == nil {
		return nil, ErrProcedureRequired
	}
	if !active {
		return nil, txn.ErrInactive
	}
	return tx, nil
}

// visible 在持有使用权和记录锁时检查删除状态；不将缓存互斥锁带入业务数据操作。
func (r *Runtime) visible(ctx context.Context, entry *entity) (any, bool, error) {
	r.cache.mu.RLock()
	removed := entry.removed
	owner := entry.removing
	busy := entry.deleting
	r.cache.mu.RUnlock()
	if removed {
		return nil, false, nil
	}
	if busy {
		tx, _ := txn.FromContext(ctx)
		if owner == tx && owner != nil {
			return nil, false, nil
		}
		return nil, false, storage.ErrDeletionPending
	}
	return entry.value, true, nil
}

// Snapshot 返回当前缓存状态的独立深拷贝，不能用于保存或删除共享实体。
// 普通读取先取得使用权再等待记录锁；事务内复用 Find，可读到本事务尚未提交的修改。
func (r *Runtime) Snapshot(ctx context.Context, descriptor *Descriptor, key storage.RecordKey) (any, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("entity cache: runtime is not bound")
	}
	if tx, active := txn.FromContext(ctx); tx != nil {
		if !active {
			return nil, false, txn.ErrInactive
		}
		value, found, err := r.Find(ctx, descriptor, key)
		if err != nil || !found {
			return nil, found, err
		}
		copy, err := descriptor.Clone(value)
		return copy, err == nil, err
	}
	var copy any
	var found bool
	_, err := r.cache.Use(ctx, identity{descriptor, key}, func(entry *entity) error {
		return txn.WithLocks(ctx, txn.Lockeys{entry.value.(txn.StoreProvider).StoreTarget().Lock}, func(ctx context.Context) error {
			value, exists, err := r.visible(ctx, entry)
			if err != nil || !exists {
				return err
			}
			copy, err = descriptor.Clone(value)
			found = err == nil
			return err
		})
	})
	return copy, found, err
}
