// Package txn 提供可选的单机内存事务。
//
// 它只协调当前进程内、由生成 Store 受控的修改；不提供分布式锁或持久化层 ACID。
package txn

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/ngnl47/tx/change"
	txlog "github.com/ngnl47/tx/log"
)

type contextKey struct{}

// StoreKey 是事务参与对象的身份，与保护它的一致性域锁无关。
type StoreKey struct {
	Type string
	Key  string
}

func (key StoreKey) valid() bool    { return key.Type != "" && key.Key != "" }
func (key StoreKey) String() string { return key.Type + ":" + key.Key }

// StoreTarget 将对象登记与保护它的 Lockey 关联，多个 Store 可共用同一 Lockey。
type StoreTarget struct {
	Key   StoreKey
	Lock  Lockey
	Store change.UndoBindable
}

// NewStoreTarget 创建事务对象登记；对象身份与一致性域锁分别管理。
func NewStoreTarget(lock Lockey, store change.UndoBindable) StoreTarget {
	return StoreTarget{Key: storeIdentity(store), Lock: lock, Store: store}
}

// StoreProvider 由生成根对象及 Store 实现。
type StoreProvider interface{ StoreTarget() StoreTarget }

// Locks 从已加载的对象获取锁声明，不登记 Store；修改前须由受管查询或 Enlist 登记。
func Locks(values ...StoreProvider) Lockeys {
	result := make(Lockeys, 0, len(values))
	for _, value := range values {
		result = append(result, value.StoreTarget().Lock)
	}
	return result
}

// Procedure 是可复用的单机事务业务单元。
//
// Locks 只声明当前 Procedure 直接使用的锁。子 Procedure 通过 Tx.Call 同步执行时，
// 框架会复用已持有锁并获取新锁；所有锁直到根事务结束才释放。
type Procedure interface {
	Locks(context.Context) (Lockeys, error)
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
	allowEmptyLocks       bool
	reserveDynamicTargets func([]StoreTarget) error
	afterAttemptFailure   func()
	commitLocked          func([]StoreTarget)
	callAfterCommit       func(context.Context, Procedure) error
	catchCommittedPanic   bool
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
	locks      map[Lockey]int
	stores     map[StoreKey]change.UndoBindable
	targets    []StoreTarget
	heldLocks  heldLockSet
	dirty      map[StoreKey]struct{}
	procedures []string
	deadlock   error
	options    CallOptions
	hooks      callHooks

	remover       RemovalParticipant
	removals      []removal
	creator       CreationParticipant
	creations     []creation
	undos         []change.UndoFunc
	afterCommit   []func(context.Context) error
	afterRollback []func(context.Context)
	finish        []func()
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

func (tx *Tx) dirtyTargets() []StoreTarget {
	if tx == nil || len(tx.dirty) == 0 {
		return nil
	}
	targets := make([]StoreTarget, 0, len(tx.dirty))
	for _, target := range tx.targets {
		if _, ok := tx.dirty[target.Key]; !ok {
			continue
		}
		if tx.active {
			if store, ok := target.Store.(change.Source); ok && !store.HasChanges() {
				continue
			}
		}
		targets = append(targets, target)
	}
	return targets
}

func (tx *Tx) finalizeDirtyTargets() {
	for _, target := range tx.targets {
		if _, ok := tx.dirty[target.Key]; !ok {
			continue
		}
		if store, ok := target.Store.(change.Source); ok && !store.HasChanges() {
			delete(tx.dirty, target.Key)
		}
	}
}

func (tx *Tx) markDirty(target StoreTarget) {
	if tx != nil {
		tx.dirty[target.Key] = struct{}{}
	}
}

type targetUndoRecorder struct {
	tx     *Tx
	target StoreTarget
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
	if tx == nil || !tx.active {
		return ErrInactive
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
	if err := tx.acquire(locks, tx.procedureTrace(procedure)); err != nil {
		return err
	}
	return tx.call(procedure)
}

// AfterCommit 注册根事务成功后的动作。动作在锁释放后执行，失败不会回滚已提交的内存修改。
func (tx *Tx) AfterCommit(fn func(context.Context) error) {
	if tx != nil && fn != nil {
		tx.afterCommit = append(tx.afterCommit, fn)
	}
}

// CallAfterCommit 登记根提交并解锁后的独立 Procedure，按登记顺序同步执行。
// 登记随 savepoint 回滚撤销；沿用当前 Runtime、操作期限和已接纳工作，不回投邮箱。
func (tx *Tx) CallAfterCommit(procedure Procedure) error {
	if tx == nil || !tx.active {
		return ErrInactive
	}
	if isNil(procedure) {
		return errors.New("transaction: nil procedure")
	}
	call := tx.hooks.callAfterCommit
	if call == nil {
		options := tx.options
		call = func(ctx context.Context, next Procedure) error {
			return CallWithOptions(ctx, next, options)
		}
	}
	tx.AfterCommit(func(ctx context.Context) error { return call(ctx, procedure) })
	return nil
}

// AfterCommitError 表示原事务已提交，但提交后的动作失败；不可据此重投原事务。
type AfterCommitError struct{ Err error }

func (err *AfterCommitError) Error() string { return "transaction: after commit: " + err.Err.Error() }
func (err *AfterCommitError) Unwrap() error { return err.Err }

// AfterRollback 注册回滚动作。动作在锁释放后执行。
func (tx *Tx) AfterRollback(fn func(context.Context)) {
	if tx != nil && fn != nil {
		tx.afterRollback = append(tx.afterRollback, fn)
	}
}

type savepoint struct {
	removals      int
	creations     int
	undos         int
	afterCommit   int
	afterRollback int
}

func (tx *Tx) savepoint() savepoint {
	return savepoint{
		creations:     len(tx.creations),
		removals:      len(tx.removals),
		undos:         len(tx.undos),
		afterCommit:   len(tx.afterCommit),
		afterRollback: len(tx.afterRollback),
	}
}

func (tx *Tx) rollbackTo(point savepoint, runCallbacks bool) {
	for i := len(tx.removals) - 1; i >= point.removals; i-- {
		tx.removals[i].reservation.Abort()
	}
	tx.removals = tx.removals[:point.removals]
	for index := len(tx.undos) - 1; index >= point.undos; index-- {
		tx.undos[index]()
	}
	tx.undos = tx.undos[:point.undos]
	for i := len(tx.creations) - 1; i >= point.creations; i-- {
		entry := tx.creations[i]
		entry.reservation.Abort()
		delete(tx.dirty, entry.target.Key)
		entry.target.Store.SetUndoRecorder(nil)
		delete(tx.stores, entry.target.Key)
		for i, target := range tx.targets {
			if target.Key == entry.target.Key {
				tx.targets = append(tx.targets[:i], tx.targets[i+1:]...)
				break
			}
		}
	}
	tx.creations = tx.creations[:point.creations]
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
func Run(ctx context.Context, targets []StoreTarget, fn func(*Tx) error) error {
	locks := make(Lockeys, 0, len(targets))
	for _, target := range targets {
		locks = append(locks, target.Lock)
	}
	return unwrapDeadlockVictim(run(ctx, locks, func(tx *Tx) error {
		for _, target := range targets {
			if err := tx.Enlist(target); err != nil {
				return err
			}
		}
		if fn != nil {
			return fn(tx)
		}
		return nil
	}, CallOptions{}, callHooks{}, "txn.Run"))
}

func run(ctx context.Context, locks Lockeys, fn func(*Tx) error, options CallOptions, hooks callHooks, procedurePath string) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: nested Run is not allowed; use tx.Call")
	}
	var prepared Lockeys
	if len(locks) > 0 || !hooks.allowEmptyLocks {
		prepared, err = prepareLockeys(locks)
		if err != nil {
			return err
		}
	}
	transaction := nextTransactionID()
	heldLocks, err := acquireLocks(ctx, transaction, prepared, procedurePath)
	if err != nil {
		if errors.Is(err, ErrDeadlock) {
			return &deadlockVictimError{err: err}
		}
		return err
	}

	tx := &Tx{
		id:        transaction,
		active:    true,
		locks:     make(map[Lockey]int, len(prepared)),
		stores:    make(map[StoreKey]change.UndoBindable),
		heldLocks: heldLocks,
		dirty:     make(map[StoreKey]struct{}, len(prepared)),
		options:   options,
		hooks:     hooks,
	}
	tx.ctx = context.WithValue(ctx, contextKey{}, tx)
	committed := false
	defer func() {
		// 一个释放回调异常不应阻断其他实体使用权释放。
		var failure any
		for i := len(tx.finish) - 1; i >= 0; i-- {
			func() {
				defer func() {
					if recovered := recover(); recovered != nil && failure == nil {
						failure = recovered
					}
				}()
				tx.finish[i]()
			}()
		}
		if failure != nil {
			if committed && hooks.catchCommittedPanic {
				err = errors.Join(err, &AfterCommitError{Err: newPanicError(failure)})
			} else {
				panic(failure)
			}
		}
	}()
	defer func() {
		tx.active = false
		if recovered := recover(); recovered != nil {
			if committed {
				if hooks.catchCommittedPanic {
					err = &AfterCommitError{Err: newPanicError(recovered)}
					return
				}
				panic(recovered)
			}
			rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
			tx.rollbackTo(savepoint{}, false)
			clear(tx.dirty)
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
		tx.locks[lock.identity()] = lock.Order
	}
	if fn != nil {
		err = fn(tx)
	}
	if err == nil && tx.deadlock != nil {
		err = tx.deadlock
	}
	if err == nil {
		err = tx.validateCreations()
		if err == nil {
			for _, r := range tx.removals {
				if err = r.reservation.Validate(); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
		tx.rollbackTo(savepoint{}, false)
		clear(tx.dirty)
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

	// 内部提交在持锁时完成登记；业务 AfterCommit 在解绑和释放锁后执行。
	tx.finalizeDirtyTargets()
	for _, entry := range tx.creations {
		entry.reservation.Commit()
	}
	tx.creations = nil
	for _, r := range tx.removals {
		r.reservation.Commit()
		delete(tx.dirty, r.target.Key)
	}
	tx.removals = nil
	if tx.hooks.commitLocked != nil {
		tx.hooks.commitLocked(tx.dirtyTargets())
	}
	tx.undos = nil
	for _, lock := range tx.targets {
		lock.Store.SetUndoRecorder(nil)
	}
	releaseLocks(transaction, &tx.heldLocks)
	tx.active = false
	committed = true
	return runAfterCommit(ctx, tx.afterCommit)
}

// WithLocks 在不创建事务、undo log 或回调的前提下持有一组本机锁执行 fn。
// 它适合状态运行时在落盘前稳定读取并清理 Store 的当前脏状态。
func WithLocks(ctx context.Context, locks Lockeys, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: WithLocks cannot run inside active transaction")
	}
	prepared, err := prepareLockeys(locks)
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

func validateCleanStoreTargets(locks []StoreTarget, options CallOptions) error {
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
		var committed *AfterCommitError
		if errors.As(err, &committed) {
			return err
		}
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
			return &AfterCommitError{Err: err}
		}
	}
	return nil
}
