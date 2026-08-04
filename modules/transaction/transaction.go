// Package transaction 提供可选的单机内存事务。
//
// 它只协调当前进程内、由生成 Store 受控的修改；不提供分布式锁或持久化层 ACID。
package transaction

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/ngnl47/tx/change"
)

const lockStripeCount = 4096

var lockStripes [lockStripeCount]sync.Mutex

type contextKey struct{}

// Key 是单机事务锁的稳定身份。Scope 表示一致性域，Value 表示域内对象。
type Key struct {
	Scope string
	Value string
}

// NewKey 创建一个规范化锁键。生成代码通常使用它把基础字段转换为锁键。
func NewKey(scope string, value any) Key {
	return Key{Scope: scope, Value: fmt.Sprint(value)}
}

func (key Key) valid() bool {
	return key.Scope != "" && key.Value != ""
}

func (key Key) String() string {
	return key.Scope + ":" + key.Value
}

// Target 将一条锁键和可绑定 undo recorder 的根 Store 组合为一个事务目标。
type Target struct {
	Key   Key
	Store change.UndoBindable
}

// NewTarget 创建一个事务目标。
func NewTarget(key Key, store change.UndoBindable) Target {
	return Target{Key: key, Store: store}
}

// Targets 便于在调用 Run 时组织多个事务目标。
func Targets(targets ...Target) []Target {
	return targets
}

// Procedure 是可复用的单机事务业务单元。
//
// Targets 必须在进入根事务前给出完整锁键集合。子 Procedure 只能声明根事务
// 已持有锁键的子集；需要更多目标时，应由根 Procedure 在调用前一并声明。
type Procedure interface {
	Targets(context.Context) ([]Target, error)
	Process(*Tx) error
}

// Tx 是一次正在执行的单机事务。
type Tx struct {
	ctx        context.Context
	active     bool
	targets    map[Key]change.UndoBindable
	procedures []string

	undos         []change.UndoFunc
	afterCommit   []func(context.Context) error
	afterRollback []func(context.Context)
}

// Context 返回本次事务派生的 context，供事务内的后续框架调用传递。
func (tx *Tx) Context() context.Context {
	if tx == nil {
		return nil
	}
	return tx.ctx
}

// FromContext 返回当前调用链绑定的活跃单机事务。
func FromContext(ctx context.Context) (*Tx, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(contextKey{}).(*Tx)
	return tx, ok && tx != nil && tx.active
}

// RecordUndo 实现 change.UndoRecorder。生成 Store 只会在绑定到 Tx 后调用它。
func (tx *Tx) RecordUndo(undo change.UndoFunc) {
	if tx == nil || undo == nil {
		return
	}
	tx.undos = append(tx.undos, undo)
}

// Do 在当前事务内建立 savepoint。返回错误时只回滚该调用之后的修改，调用方可自行决定是否继续外层事务。
func (tx *Tx) Do(fn func(*Tx) error) error {
	if tx == nil {
		return errors.New("transaction: nil transaction")
	}
	if fn == nil {
		return nil
	}
	savepoint := tx.savepoint()
	if err := fn(tx); err != nil {
		tx.rollbackTo(savepoint, true)
		return err
	}
	return nil
}

// Call 在当前事务中调用子 Procedure，并为它建立 savepoint。
// 子 Procedure 不会重新加锁，且只能使用根事务已声明的目标。
func (tx *Tx) Call(procedure Procedure) error {
	if tx == nil || !tx.active {
		return errors.New("transaction: inactive transaction")
	}
	if procedure == nil {
		return errors.New("transaction: nil procedure")
	}
	targets, err := procedure.Targets(tx.ctx)
	if err != nil {
		return fmt.Errorf("transaction: procedure %s targets: %w", procedureName(procedure), err)
	}
	if len(targets) > 0 {
		prepared, err := prepareTargets(targets)
		if err != nil {
			return fmt.Errorf("transaction: procedure %s targets: %w", procedureName(procedure), err)
		}
		for _, target := range prepared {
			rootStore, exists := tx.targets[target.Key]
			if !exists {
				return fmt.Errorf("transaction: procedure %s introduces undeclared target %s", tx.procedureTrace(procedure), target.Key.String())
			}
			if !sameStore(rootStore, target.Store) {
				return fmt.Errorf("transaction: procedure %s uses different Store for declared target %s", tx.procedureTrace(procedure), target.Key.String())
			}
		}
	}
	return tx.call(procedure)
}

// AfterCommit 注册根事务成功后的动作。动作在锁释放后执行，失败不会回滚已提交的内存修改。
func (tx *Tx) AfterCommit(fn func(context.Context) error) {
	if tx != nil && fn != nil {
		tx.afterCommit = append(tx.afterCommit, fn)
	}
}

// AfterRollback 注册回滚动作。动作在锁释放后执行。
func (tx *Tx) AfterRollback(fn func(context.Context)) {
	if tx != nil && fn != nil {
		tx.afterRollback = append(tx.afterRollback, fn)
	}
}

type savepoint struct {
	undos         int
	afterCommit   int
	afterRollback int
}

func (tx *Tx) savepoint() savepoint {
	return savepoint{
		undos:         len(tx.undos),
		afterCommit:   len(tx.afterCommit),
		afterRollback: len(tx.afterRollback),
	}
}

func (tx *Tx) rollbackTo(point savepoint, runCallbacks bool) {
	for index := len(tx.undos) - 1; index >= point.undos; index-- {
		tx.undos[index]()
	}
	tx.undos = tx.undos[:point.undos]
	if runCallbacks {
		for _, fn := range tx.afterRollback[point.afterRollback:] {
			fn(tx.ctx)
		}
	}
	tx.afterCommit = tx.afterCommit[:point.afterCommit]
	tx.afterRollback = tx.afterRollback[:point.afterRollback]
}

func (tx *Tx) call(procedure Procedure) error {
	tx.procedures = append(tx.procedures, procedureName(procedure))
	defer func() {
		tx.procedures = tx.procedures[:len(tx.procedures)-1]
	}()
	return tx.Do(procedure.Process)
}

func (tx *Tx) procedureTrace(next Procedure) string {
	trace := make([]string, 0, len(tx.procedures)+1)
	trace = append(trace, tx.procedures...)
	trace = append(trace, procedureName(next))
	return strings.Join(trace, " -> ")
}

func procedureName(procedure Procedure) string {
	typ := reflect.TypeOf(procedure)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if name := typ.Name(); name != "" {
		return name
	}
	return typ.String()
}

func sameStore(left, right change.UndoBindable) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if leftValue.Type() != rightValue.Type() || !leftValue.Type().Comparable() {
		return false
	}
	return leftValue.Interface() == rightValue.Interface()
}

// Run 按稳定锁顺序执行一次单机事务。只有传入的 Store 会在回调期间绑定 undo recorder。
func Run(ctx context.Context, targets []Target, fn func(*Tx) error) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: nested Run is not allowed; use tx.Call")
	}
	prepared, err := prepareTargets(targets)
	if err != nil {
		return err
	}
	stripes := lockTargets(prepared)

	tx := &Tx{ctx: context.WithValue(ctx, contextKey{}, (*Tx)(nil)), active: true, targets: make(map[Key]change.UndoBindable, len(prepared))}
	tx.ctx = context.WithValue(ctx, contextKey{}, tx)
	for _, target := range prepared {
		tx.targets[target.Key] = target.Store
	}
	for _, target := range prepared {
		target.Store.SetUndoRecorder(tx)
	}
	defer func() {
		tx.active = false
		if recovered := recover(); recovered != nil {
			rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
			tx.rollbackTo(savepoint{}, false)
			for _, target := range prepared {
				target.Store.SetUndoRecorder(nil)
			}
			unlockStripes(stripes)
			for _, callback := range rollbackCallbacks {
				callback(ctx)
			}
			panic(recovered)
		}
	}()
	if fn != nil {
		err = fn(tx)
	}
	if err != nil {
		rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
		tx.rollbackTo(savepoint{}, false)
		for _, target := range prepared {
			target.Store.SetUndoRecorder(nil)
		}
		unlockStripes(stripes)
		tx.active = false
		for _, callback := range rollbackCallbacks {
			callback(ctx)
		}
		return err
	}

	// 成功时不再需要反向操作。提交回调不能占用数据锁，也不能继续修改已解绑 Store。
	tx.undos = nil
	for _, target := range prepared {
		target.Store.SetUndoRecorder(nil)
	}
	unlockStripes(stripes)
	tx.active = false
	return runAfterCommit(ctx, tx.afterCommit)
}

// Call 使用 Procedure 声明的完整目标集合创建根事务。
// 已在事务内时必须改用 tx.Call，以避免动态扩张锁集。
func Call(ctx context.Context, procedure Procedure) error {
	if procedure == nil {
		return errors.New("transaction: nil procedure")
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: nested Call is not allowed; use tx.Call")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	targets, err := procedure.Targets(ctx)
	if err != nil {
		return fmt.Errorf("transaction: procedure %s targets: %w", procedureName(procedure), err)
	}
	return Run(ctx, targets, func(tx *Tx) error {
		return tx.call(procedure)
	})
}

func prepareTargets(targets []Target) ([]Target, error) {
	if len(targets) == 0 {
		return nil, errors.New("transaction: no targets")
	}
	prepared := append([]Target(nil), targets...)
	sort.Slice(prepared, func(i, j int) bool {
		if prepared[i].Key.Scope != prepared[j].Key.Scope {
			return prepared[i].Key.Scope < prepared[j].Key.Scope
		}
		return prepared[i].Key.Value < prepared[j].Key.Value
	})
	write := 0
	for _, target := range prepared {
		if !target.Key.valid() {
			return nil, fmt.Errorf("transaction: invalid lock key %q", target.Key.String())
		}
		if target.Store == nil {
			return nil, fmt.Errorf("transaction: target %s has nil Store", target.Key.String())
		}
		if write > 0 && target.Key == prepared[write-1].Key {
			if !sameStore(target.Store, prepared[write-1].Store) {
				return nil, fmt.Errorf("transaction: duplicate key %s binds different Store", target.Key.String())
			}
			continue
		}
		prepared[write] = target
		write++
	}
	return prepared[:write], nil
}

func lockTargets(targets []Target) []int {
	stripes := make([]int, 0, len(targets))
	for _, target := range targets {
		stripes = append(stripes, lockStripe(target.Key))
	}
	sort.Ints(stripes)
	write := 0
	for _, stripe := range stripes {
		if write > 0 && stripe == stripes[write-1] {
			continue
		}
		stripes[write] = stripe
		write++
	}
	stripes = stripes[:write]
	for _, stripe := range stripes {
		lockStripes[stripe].Lock()
	}
	return stripes
}

func unlockStripes(stripes []int) {
	for index := len(stripes) - 1; index >= 0; index-- {
		lockStripes[stripes[index]].Unlock()
	}
}

func lockStripe(key Key) int {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	hash := uint64(offset64)
	for _, value := range []string{key.Scope, "\x00", key.Value} {
		for index := 0; index < len(value); index++ {
			hash ^= uint64(value[index])
			hash *= prime64
		}
	}
	return int(hash % lockStripeCount)
}

func runAfterCommit(ctx context.Context, callbacks []func(context.Context) error) error {
	var errs []error
	for _, callback := range callbacks {
		if err := callback(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
