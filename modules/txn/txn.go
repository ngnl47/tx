// Package txn 提供可选的单机内存事务。
//
// 它只协调当前进程内、由生成 Store 受控的修改；不提供分布式锁或持久化层 ACID。
package txn

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/ngnl47/tx/change"
	txlog "github.com/ngnl47/tx/log"
)

type contextKey struct{}

// LockKey 是单机事务锁的稳定身份。Scope 表示一致性域，Value 表示域内对象。
type LockKey struct {
	Scope string
	Value string
}

// NewLockKey 创建一个规范化锁键。生成代码通常使用它把基础字段转换为锁键。
func NewLockKey(scope string, value any) LockKey {
	return LockKey{Scope: scope, Value: fmt.Sprint(value)}
}

func (key LockKey) valid() bool {
	return key.Scope != "" && key.Value != ""
}

func (key LockKey) String() string {
	return key.Scope + ":" + key.Value
}

// LockTarget 将一条锁键、锁获取顺序和可绑定 undo recorder 的根 Store 组合为一个锁对象。
// Order 越小越先获取；相同逻辑 Key 必须使用相同 Order。
type LockTarget struct {
	Key   LockKey
	Order int
	Store change.UndoBindable
}

// NewLockTarget 创建默认 Order 为 0 的锁对象。
func NewLockTarget(key LockKey, store change.UndoBindable) LockTarget {
	return LockTarget{Key: key, Store: store}
}

// NewOrderedLockTarget 创建带锁层级的锁对象。较小的 order 先获取，
// 例如一致性域对象使用 10，域内角色对象使用 20。
func NewOrderedLockTarget(order int, key LockKey, store change.UndoBindable) LockTarget {
	return LockTarget{Key: key, Order: order, Store: store}
}

// Lockable 表示可提供本机事务锁对象的生成数据或 Store。
type Lockable interface {
	LockTarget() LockTarget
}

// Locks 将生成数据或 Store 转换为 Run 所需的锁对象列表。
func Locks(values ...Lockable) []LockTarget {
	return NewLockSet().Add(values...).Build()
}

// LockSet 用于在进入事务前收集一批已经确定的锁对象。
type LockSet struct {
	locks []LockTarget
}

// NewLockSet 创建一个空锁集。
func NewLockSet() *LockSet {
	return &LockSet{}
}

// Add 向锁集追加生成数据或 Store。重复锁和非法锁由 Run 统一校验。
func (set *LockSet) Add(values ...Lockable) *LockSet {
	if set == nil {
		return nil
	}
	for _, value := range values {
		if isNil(value) {
			set.locks = append(set.locks, LockTarget{})
			continue
		}
		set.locks = append(set.locks, value.LockTarget())
	}
	return set
}

// Build 返回当前锁集的副本，供 Run 或 Procedure 使用。
func (set *LockSet) Build() []LockTarget {
	if set == nil {
		return nil
	}
	return append([]LockTarget(nil), set.locks...)
}

// Procedure 是可复用的单机事务业务单元。
//
// Locks 只声明当前 Procedure 直接使用的锁。子 Procedure 通过 Tx.Call 同步执行时，
// 框架会复用已持有锁并获取新锁；所有锁直到根事务结束才释放。
type Procedure interface {
	Locks(context.Context) ([]LockTarget, error)
	Process(*Tx) error
}

// CallOptions 描述根 Procedure 的执行选项。
type CallOptions struct {
	// AllowPendingChanges 允许锁定对象在进入事务前已保留待落盘变更。
	// 有状态运行时会在多次事务之间合并 Store 脏状态，因此需要显式开启。
	AllowPendingChanges bool
	// DeadlockRetries 是根 Procedure 被选为死锁受害者后的最大重试次数。
	// 零表示不重试；每次重试都会从干净根状态重新执行 Process。
	DeadlockRetries int
}

type callHooks struct {
	reserveDynamicTargets func([]LockTarget) error
	afterAttemptFailure   func()
}

const defaultDeadlockRetries = 3

type deadlockVictimError struct {
	err error
}

func (err *deadlockVictimError) Error() string {
	return err.err.Error()
}

func (err *deadlockVictimError) Unwrap() error {
	return err.err
}

// Tx 是一次正在执行的单机事务。
type Tx struct {
	id         TransactionID
	ctx        context.Context
	active     bool
	locks      map[LockKey]change.UndoBindable
	targets    []LockTarget
	heldLocks  heldLockSet
	dirty      map[LockKey]struct{}
	procedures []string
	deadlock   error
	options    CallOptions
	hooks      callHooks

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

// DirtyTargets 返回本次事务经受控 Store 修改且当前仍有待落盘变更的根目标。
//
// 返回顺序与根事务实际取得目标的顺序一致。该结果适合业务 facade 在提交后选择持久化对象；
// 它不包含未经事务 Store API 修改的裸字段变更。
func (tx *Tx) DirtyTargets() []LockTarget {
	if tx == nil || len(tx.dirty) == 0 {
		return nil
	}
	targets := make([]LockTarget, 0, len(tx.dirty))
	for _, target := range tx.targets {
		if _, ok := tx.dirty[target.Key]; !ok {
			continue
		}
		if store, ok := target.Store.(change.Source); ok && !store.HasChanges() {
			continue
		}
		targets = append(targets, target)
	}
	return targets
}

func (tx *Tx) markDirty(target LockTarget) {
	if tx != nil {
		tx.dirty[target.Key] = struct{}{}
	}
}

type targetUndoRecorder struct {
	tx     *Tx
	target LockTarget
}

func (recorder targetUndoRecorder) RecordUndo(undo change.UndoFunc) {
	if recorder.tx == nil || undo == nil {
		return
	}
	recorder.tx.markDirty(recorder.target)
	recorder.tx.RecordUndo(undo)
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
// 已持有锁会直接复用；新锁按当前 Procedure 的直接锁集排序后获取，并持有到根事务结束。
func (tx *Tx) Call(procedure Procedure) error {
	if tx == nil || !tx.active {
		return errors.New("transaction: inactive transaction")
	}
	if tx.deadlock != nil {
		return tx.deadlock
	}
	if isNil(procedure) {
		return errors.New("transaction: nil procedure")
	}
	locks, err := procedure.Locks(tx.ctx)
	if err != nil {
		return fmt.Errorf("transaction: procedure %s locks: %w", procedureName(procedure), err)
	}
	if len(locks) > 0 {
		prepared, err := prepareLocks(locks)
		if err != nil {
			return fmt.Errorf("transaction: procedure %s locks: %w", procedureName(procedure), err)
		}
		var newLocks []LockTarget
		for _, lock := range prepared {
			rootStore, exists := tx.locks[lock.Key]
			if !exists {
				newLocks = append(newLocks, lock)
				continue
			}
			if !sameStore(rootStore, lock.Store) {
				return fmt.Errorf("transaction: procedure %s uses different Store for declared lock %s", tx.procedureTrace(procedure), lock.Key.String())
			}
		}
		if len(newLocks) > 0 {
			trace := tx.procedureTrace(procedure)
			heldLocks, err := acquireLocks(tx.ctx, tx.id, newLocks, trace)
			if err != nil {
				if errors.Is(err, ErrDeadlock) {
					tx.deadlock = err
				}
				return err
			}
			if err := validateCleanLockTargets(newLocks, tx.options); err != nil {
				releaseLocks(tx.id, &heldLocks)
				return err
			}
			if tx.hooks.reserveDynamicTargets != nil {
				if err := tx.hooks.reserveDynamicTargets(newLocks); err != nil {
					releaseLocks(tx.id, &heldLocks)
					return err
				}
			}
			tx.heldLocks.appendSet(&heldLocks)
			for _, lock := range newLocks {
				tx.locks[lock.Key] = lock.Store
				tx.targets = append(tx.targets, lock)
				lock.Store.SetUndoRecorder(targetUndoRecorder{tx: tx, target: lock})
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

type procedureNameProvider interface {
	transactionProcedureName() string
}

func procedureName(procedure Procedure) string {
	if provider, ok := procedure.(procedureNameProvider); ok {
		return provider.transactionProcedureName()
	}
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

// Run 按稳定锁顺序执行一次单机事务。只有传入锁对象绑定的 Store 会在回调期间绑定 undo recorder。
func Run(ctx context.Context, locks []LockTarget, fn func(*Tx) error) (err error) {
	return unwrapDeadlockVictim(run(ctx, locks, fn, CallOptions{}, callHooks{}, "txn.Run"))
}

func run(ctx context.Context, locks []LockTarget, fn func(*Tx) error, options CallOptions, hooks callHooks, procedurePath string) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: nested Run is not allowed; use tx.Call")
	}
	prepared, err := prepareLocks(locks)
	if err != nil {
		return err
	}
	transaction := nextTransactionID()
	heldLocks, err := acquireLocks(ctx, transaction, prepared, procedurePath)
	if err != nil {
		if errors.Is(err, ErrDeadlock) {
			return &deadlockVictimError{err: err}
		}
		return err
	}
	if err := validateCleanLockTargets(prepared, options); err != nil {
		releaseLocks(transaction, &heldLocks)
		return err
	}

	tx := &Tx{
		id:        transaction,
		active:    true,
		locks:     make(map[LockKey]change.UndoBindable, len(prepared)),
		targets:   append([]LockTarget(nil), prepared...),
		heldLocks: heldLocks,
		dirty:     make(map[LockKey]struct{}, len(prepared)),
		options:   options,
		hooks:     hooks,
	}
	tx.ctx = context.WithValue(ctx, contextKey{}, tx)
	defer func() {
		tx.active = false
		if recovered := recover(); recovered != nil {
			rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
			tx.rollbackTo(savepoint{}, false)
			for _, lock := range tx.targets {
				lock.Store.SetUndoRecorder(nil)
			}
			releaseLocks(transaction, &tx.heldLocks)
			for _, callback := range rollbackCallbacks {
				callback(ctx)
			}
			panic(recovered)
		}
	}()
	for _, lock := range prepared {
		tx.locks[lock.Key] = lock.Store
	}
	for _, lock := range prepared {
		lock.Store.SetUndoRecorder(targetUndoRecorder{tx: tx, target: lock})
	}
	if fn != nil {
		err = fn(tx)
	}
	if err == nil && tx.deadlock != nil {
		err = tx.deadlock
	}
	if err != nil {
		rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
		tx.rollbackTo(savepoint{}, false)
		for _, lock := range tx.targets {
			lock.Store.SetUndoRecorder(nil)
		}
		releaseLocks(transaction, &tx.heldLocks)
		tx.active = false
		for _, callback := range rollbackCallbacks {
			callback(ctx)
		}
		if tx.deadlock != nil {
			return &deadlockVictimError{err: tx.deadlock}
		}
		return err
	}

	// 成功时不再需要反向操作。提交回调不能占用数据锁，也不能继续修改已解绑 Store。
	tx.undos = nil
	for _, lock := range tx.targets {
		lock.Store.SetUndoRecorder(nil)
	}
	releaseLocks(transaction, &tx.heldLocks)
	tx.active = false
	return runAfterCommit(ctx, tx.afterCommit)
}

// WithLocks 在不创建事务、undo log 或回调的前提下持有一组本机锁执行 fn。
// 它适合状态运行时在落盘前稳定读取并清理 Store 的当前脏状态。
func WithLocks(ctx context.Context, locks []LockTarget, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: WithLocks cannot run inside active transaction")
	}
	prepared, err := prepareLocks(locks)
	if err != nil {
		return err
	}
	transaction := nextTransactionID()
	heldLocks, err := acquireLocks(ctx, transaction, prepared, "txn.WithLocks")
	if err != nil {
		return err
	}
	defer releaseLocks(transaction, &heldLocks)
	if fn == nil {
		return nil
	}
	return fn(ctx)
}

func validateCleanLockTargets(locks []LockTarget, options CallOptions) error {
	if options.AllowPendingChanges || !CurrentConfig().Debug.CheckCleanLockTargets {
		return nil
	}
	for _, lock := range locks {
		store, ok := lock.Store.(change.Source)
		if !ok || !store.HasChanges() {
			continue
		}
		return fmt.Errorf("transaction: lock target %s has pending changes before transaction", lock.Key.String())
	}
	return nil
}

// Call 使用 Procedure 声明的直接锁集创建根事务。
// 已在事务内时必须改用 tx.Call，以复用当前事务身份和 savepoint。
func Call(ctx context.Context, procedure Procedure) error {
	return CallWithOptions(ctx, procedure, CallOptions{DeadlockRetries: defaultDeadlockRetries})
}

// CallWithOptions 使用 Procedure 声明的直接锁集创建根事务，并应用执行选项。
func CallWithOptions(ctx context.Context, procedure Procedure, options CallOptions) error {
	return callWithHooks(ctx, procedure, options, callHooks{})
}

func callWithHooks(ctx context.Context, procedure Procedure, options CallOptions, hooks callHooks) error {
	if isNil(procedure) {
		return errors.New("transaction: nil procedure")
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: nested Call is not allowed; use tx.Call")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if options.DeadlockRetries < 0 {
		return errors.New("transaction: deadlock retries cannot be negative")
	}
	for attempt := 0; ; attempt++ {
		locks, err := procedure.Locks(ctx)
		if err != nil {
			return fmt.Errorf("transaction: procedure %s locks: %w", procedureName(procedure), err)
		}
		err = run(ctx, locks, func(tx *Tx) error {
			return tx.call(procedure)
		}, options, hooks, procedureName(procedure))
		if err != nil && hooks.afterAttemptFailure != nil {
			hooks.afterAttemptFailure()
		}
		var victim *deadlockVictimError
		if !errors.As(err, &victim) {
			return err
		}
		if attempt >= options.DeadlockRetries || ctx.Err() != nil {
			return victim.err
		}
		txlog.WarnContext(
			ctx,
			"transaction deadlock detected, retry scheduled",
			"attempt", attempt+1,
			"maxRetries", options.DeadlockRetries,
			"err", victim.err,
		)
	}
}

func unwrapDeadlockVictim(err error) error {
	var victim *deadlockVictimError
	if errors.As(err, &victim) {
		return victim.err
	}
	return err
}

func prepareLocks(locks []LockTarget) ([]LockTarget, error) {
	if len(locks) == 0 {
		return nil, errors.New("transaction: no locks")
	}
	if len(locks) == 1 {
		if err := validateLockTarget(locks[0]); err != nil {
			return nil, err
		}
		return locks, nil
	}
	prepared := append([]LockTarget(nil), locks...)
	sort.Slice(prepared, func(i, j int) bool {
		if prepared[i].Order != prepared[j].Order {
			return prepared[i].Order < prepared[j].Order
		}
		return prepared[i].Key.Value < prepared[j].Key.Value
	})
	seen := make(map[LockKey]LockTarget, len(prepared))
	write := 0
	for _, lock := range prepared {
		if err := validateLockTarget(lock); err != nil {
			return nil, err
		}
		if previous, exists := seen[lock.Key]; exists {
			if !sameStore(lock.Store, previous.Store) {
				return nil, fmt.Errorf("transaction: duplicate key %s binds different Store", lock.Key.String())
			}
			if lock.Order != previous.Order {
				return nil, fmt.Errorf("transaction: duplicate key %s has different lock orders %d and %d", lock.Key.String(), lock.Order, previous.Order)
			}
			continue
		}
		seen[lock.Key] = lock
		prepared[write] = lock
		write++
	}
	return prepared[:write], nil
}

func validateLockTarget(lock LockTarget) error {
	if !lock.Key.valid() {
		return fmt.Errorf("transaction: invalid lock key %q", lock.Key.String())
	}
	if lock.Order < 0 {
		return fmt.Errorf("transaction: lock %s has negative lock order %d", lock.Key.String(), lock.Order)
	}
	if isNil(lock.Store) {
		return fmt.Errorf("transaction: lock %s has nil Store", lock.Key.String())
	}
	return nil
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

func runAfterCommit(ctx context.Context, callbacks []func(context.Context) error) error {
	for _, callback := range callbacks {
		if err := callback(ctx); err != nil {
			return err
		}
	}
	return nil
}
