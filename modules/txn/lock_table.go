package txn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	lockShardCount       = 64
	inlineHeldLockCount  = 4
	lockKeyHashOffset64  = uint64(14695981039346656037)
	lockKeyHashPrime64   = uint64(1099511628211)
	lockKeyHashSeparator = byte(0xff)
)

var (
	transactionLocks lockTable
	transactionSeq   atomic.Uint64
)

type lockWaiter struct {
	transaction TransactionID
	signal      chan struct{}
	previous    *lockWaiter
	next        *lockWaiter
	granted     bool
}

type lockEntry struct {
	mu      sync.Mutex
	owner   TransactionID
	waiters *lockWaiter
	tail    *lockWaiter
	refs    int
}

type lockShard struct {
	mu      sync.Mutex
	entries map[LockKey]*lockEntry
}

type lockTable struct {
	shards     [lockShardCount]lockShard
	entryPool  sync.Pool
	waiterPool sync.Pool
	waitsMu    sync.Mutex
	waits      waitGraph
}

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

func nextTransactionID() TransactionID {
	id := TransactionID(transactionSeq.Add(1))
	if id == 0 {
		id = TransactionID(transactionSeq.Add(1))
	}
	return id
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

func (set *heldLockSet) appendSet(other *heldLockSet) {
	if set == nil || other == nil {
		return
	}
	for index := 0; index < other.count; index++ {
		set.append(*other.at(index))
		*other.at(index) = heldLock{}
	}
	clear(other.overflow)
	other.overflow = nil
	other.count = 0
}

func acquireLocks(ctx context.Context, transaction TransactionID, locks []LockTarget, procedurePath string) (heldLockSet, error) {
	var heldLocks heldLockSet
	for _, lock := range locks {
		held := transactionLocks.reserve(lock.Key)
		if err := transactionLocks.acquire(ctx, transaction, lock.Key, held.entry, procedurePath); err != nil {
			transactionLocks.release(held)
			releaseLocks(transaction, &heldLocks)
			return heldLockSet{}, err
		}
		heldLocks.append(held)
	}
	if err := ctx.Err(); err != nil {
		releaseLocks(transaction, &heldLocks)
		return heldLockSet{}, fmt.Errorf("transaction: acquire locks: %w", err)
	}
	return heldLocks, nil
}

func releaseLocks(transaction TransactionID, locks *heldLockSet) {
	if locks == nil {
		return
	}
	for index := locks.count - 1; index >= 0; index-- {
		held := locks.at(index)
		transactionLocks.unlock(transaction, held.entry)
		transactionLocks.release(*held)
		*held = heldLock{}
	}
	clear(locks.overflow)
	locks.overflow = nil
	locks.count = 0
}

func (table *lockTable) acquire(ctx context.Context, transaction TransactionID, key LockKey, entry *lockEntry, procedurePath string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), err)
	}

	entry.mu.Lock()
	if entry.owner == 0 {
		entry.owner = transaction
		entry.mu.Unlock()
		if err := ctx.Err(); err != nil {
			table.unlock(transaction, entry)
			return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), err)
		}
		return nil
	}

	waiter := table.acquireWaiter(transaction)
	blocker := entry.owner
	if entry.tail != nil {
		blocker = entry.tail.transaction
	}
	table.waitsMu.Lock()
	if err := table.waits.add(transaction, blocker, key, procedurePath); err != nil {
		table.waitsMu.Unlock()
		entry.mu.Unlock()
		table.releaseWaiter(waiter)
		return err
	}
	entry.append(waiter)
	table.waitsMu.Unlock()
	entry.mu.Unlock()

	select {
	case <-waiter.signal:
		if err := ctx.Err(); err != nil {
			table.unlock(transaction, entry)
			table.releaseWaiter(waiter)
			return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), err)
		}
		table.releaseWaiter(waiter)
		return nil
	case <-ctx.Done():
		entry.mu.Lock()
		if !waiter.granted {
			blocker := entry.owner
			if waiter.previous != nil {
				blocker = waiter.previous.transaction
			}
			successor := waiter.next
			entry.remove(waiter)
			table.waitsMu.Lock()
			table.waits.remove(transaction)
			if successor != nil {
				table.waits.reassignBlocker(successor.transaction, blocker)
			}
			table.waitsMu.Unlock()
			entry.mu.Unlock()
			table.releaseWaiter(waiter)
			return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), ctx.Err())
		}
		entry.mu.Unlock()
		<-waiter.signal
		table.unlock(transaction, entry)
		table.releaseWaiter(waiter)
		return fmt.Errorf("transaction: acquire lock %s: %w", key.String(), ctx.Err())
	}
}

func (table *lockTable) unlock(transaction TransactionID, entry *lockEntry) {
	entry.mu.Lock()
	if entry.owner != transaction {
		owner := entry.owner
		entry.mu.Unlock()
		panic(fmt.Sprintf("transaction: transaction %d cannot release lock owned by transaction %d", transaction, owner))
	}
	waiter := entry.pop()
	if waiter == nil {
		entry.owner = 0
		entry.mu.Unlock()
		return
	}
	table.waitsMu.Lock()
	table.waits.remove(waiter.transaction)
	table.waitsMu.Unlock()
	entry.owner = waiter.transaction
	waiter.granted = true
	waiter.signal <- struct{}{}
	entry.mu.Unlock()
}

func (entry *lockEntry) append(waiter *lockWaiter) {
	waiter.previous = entry.tail
	if entry.tail == nil {
		entry.waiters = waiter
	} else {
		entry.tail.next = waiter
	}
	entry.tail = waiter
}

func (entry *lockEntry) pop() *lockWaiter {
	waiter := entry.waiters
	if waiter != nil {
		entry.remove(waiter)
	}
	return waiter
}

func (entry *lockEntry) remove(waiter *lockWaiter) {
	if waiter.previous == nil {
		entry.waiters = waiter.next
	} else {
		waiter.previous.next = waiter.next
	}
	if waiter.next == nil {
		entry.tail = waiter.previous
	} else {
		waiter.next.previous = waiter.previous
	}
	waiter.previous = nil
	waiter.next = nil
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
	table.entryPool.Put(held.entry)
}

func (table *lockTable) shard(key LockKey) *lockShard {
	return &table.shards[lockKeyHash(key)&(lockShardCount-1)]
}

func (table *lockTable) acquireEntry() *lockEntry {
	if pooled := table.entryPool.Get(); pooled != nil {
		return pooled.(*lockEntry)
	}
	return &lockEntry{}
}

func (table *lockTable) acquireWaiter(transaction TransactionID) *lockWaiter {
	if pooled := table.waiterPool.Get(); pooled != nil {
		waiter := pooled.(*lockWaiter)
		waiter.transaction = transaction
		return waiter
	}
	return &lockWaiter{transaction: transaction, signal: make(chan struct{}, 1)}
}

func (table *lockTable) releaseWaiter(waiter *lockWaiter) {
	waiter.transaction = 0
	waiter.previous = nil
	waiter.next = nil
	waiter.granted = false
	table.waiterPool.Put(waiter)
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
