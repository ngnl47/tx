package txn

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLockTableReusedEntryKeepsKeysIndependent(t *testing.T) {
	var table lockTable
	ctx := context.Background()
	firstKey := LockKey{Scope: "role", Value: "470001"}
	secondKey := LockKey{Scope: "role", Value: "470002"}

	first := table.reserve(firstKey)
	if err := acquireLock(ctx, firstKey, first.entry); err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	first.entry.mu.Unlock()
	table.release(first)

	second := table.reserve(secondKey)
	if err := acquireLock(ctx, secondKey, second.entry); err != nil {
		t.Fatalf("acquire second lock: %v", err)
	}

	firstAgain := table.reserve(firstKey)
	if err := acquireLock(ctx, firstKey, firstAgain.entry); err != nil {
		t.Fatalf("reacquire first lock while second is held: %v", err)
	}

	firstAgain.entry.mu.Unlock()
	table.release(firstAgain)
	second.entry.mu.Unlock()
	table.release(second)
	assertLockTableEmpty(t, &table)
}

func TestLockTableCanceledWaiterLeavesReusableEntry(t *testing.T) {
	var table lockTable
	key := LockKey{Scope: "role", Value: "470001"}
	holder := table.reserve(key)
	if err := acquireLock(context.Background(), key, holder.entry); err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waiter := table.reserve(key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := acquireLock(ctx, key, waiter.entry); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v, want context deadline exceeded", err)
	}
	table.release(waiter)

	holder.entry.mu.Unlock()
	table.release(holder)
	assertLockTableEmpty(t, &table)

	reused := table.reserve(key)
	if err := acquireLock(context.Background(), key, reused.entry); err != nil {
		t.Fatalf("acquire reused lock: %v", err)
	}
	reused.entry.mu.Unlock()
	table.release(reused)
	assertLockTableEmpty(t, &table)
}

func assertLockTableEmpty(t *testing.T, table *lockTable) {
	t.Helper()
	for index := range table.shards {
		shard := &table.shards[index]
		shard.mu.Lock()
		entryCount := len(shard.entries)
		shard.mu.Unlock()
		if entryCount != 0 {
			t.Fatalf("lock shard %d retained %d entries", index, entryCount)
		}
	}
}
