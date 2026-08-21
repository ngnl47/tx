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
	"sync"

	"github.com/ngnl47/tx/change"
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

// LockSet 用于在进入事务前动态收集完整锁集。
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
		if value == nil {
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
// Locks 必须在进入根事务前给出完整锁键集合。子 Procedure 只能声明根事务
// 已持有锁键的子集；需要更多锁时，应由根 Procedure 在调用前一并声明。
type Procedure interface {
	Locks(context.Context) ([]LockTarget, error)
	Process(*Tx) error
}

// CallOptions 描述根 Procedure 的执行选项。
type CallOptions struct {
	// AllowPendingDeltas 允许锁定对象在进入事务前已保留待落盘 delta。
	// 有状态运行时会在多次事务之间合并 Store 脏状态，因此需要显式开启。
	AllowPendingDeltas bool
}

// Tx 是一次正在执行的单机事务。
type Tx struct {
	ctx        context.Context
	active     bool
	locks      map[LockKey]change.UndoBindable
	targets    []LockTarget
	dirty      map[LockKey]struct{}
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

// DirtyTargets 返回本次事务经受控 Store 修改且当前仍有 delta 的根目标。
//
// 返回顺序与事务锁顺序一致。该结果适合业务 facade 在提交后选择持久化对象；
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
		if store, ok := target.Store.(change.DeltaInspectable); ok && len(store.Deltas()) == 0 {
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
// 子 Procedure 不会重新加锁，且只能使用根事务已声明的锁。
func (tx *Tx) Call(procedure Procedure) error {
	if tx == nil || !tx.active {
		return errors.New("transaction: inactive transaction")
	}
	if procedure == nil {
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
		for _, lock := range prepared {
			rootStore, exists := tx.locks[lock.Key]
			if !exists {
				return fmt.Errorf("transaction: procedure %s introduces undeclared lock %s", tx.procedureTrace(procedure), lock.Key.String())
			}
			if !sameStore(rootStore, lock.Store) {
				return fmt.Errorf("transaction: procedure %s uses different Store for declared lock %s", tx.procedureTrace(procedure), lock.Key.String())
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

// Run 按稳定锁顺序执行一次单机事务。只有传入锁对象绑定的 Store 会在回调期间绑定 undo recorder。
func Run(ctx context.Context, locks []LockTarget, fn func(*Tx) error) (err error) {
	return run(ctx, locks, fn, CallOptions{})
}

func run(ctx context.Context, locks []LockTarget, fn func(*Tx) error, options CallOptions) (err error) {
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
	heldLocks, err := acquireLocks(ctx, prepared)
	if err != nil {
		return err
	}
	if err := validateCleanLockTargets(prepared, options); err != nil {
		releaseLocks(&heldLocks)
		return err
	}

	tx := &Tx{
		active:  true,
		locks:   make(map[LockKey]change.UndoBindable, len(prepared)),
		targets: append([]LockTarget(nil), prepared...),
		dirty:   make(map[LockKey]struct{}, len(prepared)),
	}
	tx.ctx = context.WithValue(ctx, contextKey{}, tx)
	defer func() {
		tx.active = false
		if recovered := recover(); recovered != nil {
			rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
			tx.rollbackTo(savepoint{}, false)
			for _, lock := range prepared {
				lock.Store.SetUndoRecorder(nil)
			}
			releaseLocks(&heldLocks)
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
	if err != nil {
		rollbackCallbacks := append([]func(context.Context){}, tx.afterRollback...)
		tx.rollbackTo(savepoint{}, false)
		for _, lock := range prepared {
			lock.Store.SetUndoRecorder(nil)
		}
		releaseLocks(&heldLocks)
		tx.active = false
		for _, callback := range rollbackCallbacks {
			callback(ctx)
		}
		return err
	}

	// 成功时不再需要反向操作。提交回调不能占用数据锁，也不能继续修改已解绑 Store。
	tx.undos = nil
	for _, lock := range prepared {
		lock.Store.SetUndoRecorder(nil)
	}
	releaseLocks(&heldLocks)
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
	heldLocks, err := acquireLocks(ctx, prepared)
	if err != nil {
		return err
	}
	defer releaseLocks(&heldLocks)
	if fn == nil {
		return nil
	}
	return fn(ctx)
}

func validateCleanLockTargets(locks []LockTarget, options CallOptions) error {
	if options.AllowPendingDeltas || !CurrentConfig().Debug.CheckCleanLockTargets {
		return nil
	}
	for _, lock := range locks {
		store, ok := lock.Store.(change.DeltaInspectable)
		if !ok || len(store.Deltas()) == 0 {
			continue
		}
		return fmt.Errorf("transaction: lock target %s has pending deltas before transaction", lock.Key.String())
	}
	return nil
}

// Call 使用 Procedure 声明的完整锁集创建根事务。
// 已在事务内时必须改用 tx.Call，以避免动态扩张锁集。
func Call(ctx context.Context, procedure Procedure) error {
	return CallWithOptions(ctx, procedure, CallOptions{})
}

// CallWithOptions 使用 Procedure 声明的完整锁集创建根事务，并应用执行选项。
func CallWithOptions(ctx context.Context, procedure Procedure, options CallOptions) error {
	if procedure == nil {
		return errors.New("transaction: nil procedure")
	}
	if _, ok := FromContext(ctx); ok {
		return errors.New("transaction: nested Call is not allowed; use tx.Call")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	locks, err := procedure.Locks(ctx)
	if err != nil {
		return fmt.Errorf("transaction: procedure %s locks: %w", procedureName(procedure), err)
	}
	return run(ctx, locks, func(tx *Tx) error {
		return tx.call(procedure)
	}, options)
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
		if prepared[i].Key.Scope != prepared[j].Key.Scope {
			return prepared[i].Key.Scope < prepared[j].Key.Scope
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
	if lock.Store == nil {
		return fmt.Errorf("transaction: lock %s has nil Store", lock.Key.String())
	}
	return nil
}

type contextMutex struct {
	permit chan struct{}
}

func newContextMutex() contextMutex {
	mutex := contextMutex{permit: make(chan struct{}, 1)}
	mutex.permit <- struct{}{}
	return mutex
}

func (mutex *contextMutex) Lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-mutex.permit:
		return nil
	}
}

func (mutex *contextMutex) Unlock() {
	mutex.permit <- struct{}{}
}

type lockEntry struct {
	mu   contextMutex
	refs int
}

const (
	lockShardCount       = 64
	inlineHeldLockCount  = 4
	lockKeyHashOffset64  = uint64(14695981039346656037)
	lockKeyHashPrime64   = uint64(1099511628211)
	lockKeyHashSeparator = byte(0xff)
)

type lockShard struct {
	mu      sync.Mutex
	entries map[LockKey]*lockEntry
}

type lockTable struct {
	shards [lockShardCount]lockShard
	pool   sync.Pool
}

var transactionLocks lockTable

type heldLock struct {
	key   LockKey
	entry *lockEntry
	shard *lockShard
}

type heldLockSet struct {
	inline   [inlineHeldLockCount]heldLock
	overflow []heldLock
	count    int
}

func (set *heldLockSet) append(lock heldLock) {
	if set.count < len(set.inline) {
		set.inline[set.count] = lock
	} else {
		set.overflow = append(set.overflow, lock)
	}
	set.count++
}

func (set *heldLockSet) at(index int) *heldLock {
	if index < len(set.inline) {
		return &set.inline[index]
	}
	return &set.overflow[index-len(set.inline)]
}

func acquireLocks(ctx context.Context, locks []LockTarget) (heldLockSet, error) {
	var heldLocks heldLockSet
	for _, lock := range locks {
		held := transactionLocks.reserve(lock.Key)
		if err := acquireLock(ctx, lock.Key, held.entry); err != nil {
			transactionLocks.release(held)
			releaseLocks(&heldLocks)
			return heldLockSet{}, err
		}
		heldLocks.append(held)
	}
	if err := ctx.Err(); err != nil {
		releaseLocks(&heldLocks)
		return heldLockSet{}, fmt.Errorf("transaction: acquire locks: %w", err)
	}
	return heldLocks, nil
}

func acquireLock(ctx context.Context, key LockKey, entry *lockEntry) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), err)
	}
	if err := entry.mu.Lock(ctx); err != nil {
		return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), err)
	}
	if err := ctx.Err(); err != nil {
		entry.mu.Unlock()
		return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), err)
	}
	return nil
}

func releaseLocks(locks *heldLockSet) {
	if locks == nil {
		return
	}
	for index := locks.count - 1; index >= 0; index-- {
		held := locks.at(index)
		held.entry.mu.Unlock()
		transactionLocks.release(*held)
		*held = heldLock{}
	}
	clear(locks.overflow)
	locks.overflow = nil
	locks.count = 0
}

func (table *lockTable) reserve(key LockKey) heldLock {
	shard := table.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.entries == nil {
		shard.entries = make(map[LockKey]*lockEntry)
	}
	entry := shard.entries[key]
	if entry == nil {
		entry = table.acquireEntry()
		shard.entries[key] = entry
	}
	entry.refs++
	return heldLock{key: key, entry: entry, shard: shard}
}

func (table *lockTable) release(held heldLock) {
	if held.shard == nil || held.entry == nil {
		return
	}
	held.shard.mu.Lock()
	if held.shard.entries[held.key] != held.entry {
		held.shard.mu.Unlock()
		return
	}
	held.entry.refs--
	if held.entry.refs != 0 {
		held.shard.mu.Unlock()
		return
	}
	delete(held.shard.entries, held.key)
	held.shard.mu.Unlock()
	table.pool.Put(held.entry)
}

func (table *lockTable) shard(key LockKey) *lockShard {
	return &table.shards[lockKeyHash(key)&(lockShardCount-1)]
}

func (table *lockTable) acquireEntry() *lockEntry {
	if pooled := table.pool.Get(); pooled != nil {
		return pooled.(*lockEntry)
	}
	return &lockEntry{mu: newContextMutex()}
}

func lockKeyHash(key LockKey) uint64 {
	hash := lockKeyHashOffset64
	for index := 0; index < len(key.Scope); index++ {
		hash ^= uint64(key.Scope[index])
		hash *= lockKeyHashPrime64
	}
	hash ^= uint64(lockKeyHashSeparator)
	hash *= lockKeyHashPrime64
	for index := 0; index < len(key.Value); index++ {
		hash ^= uint64(key.Value[index])
		hash *= lockKeyHashPrime64
	}
	return hash
}

func runAfterCommit(ctx context.Context, callbacks []func(context.Context) error) error {
	for _, callback := range callbacks {
		if err := callback(ctx); err != nil {
			return err
		}
	}
	return nil
}
