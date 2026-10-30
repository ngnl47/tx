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
	firstKey := Lockey{Type: "role", Key: "470001"}
	secondKey := Lockey{Type: "role", Key: "470002"}

	first := table.reserve(firstKey)
	if err := table.acquire(ctx, 1, firstKey, first.entry, "PFirst"); err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	table.unlock(1, first.entry)
	table.release(first)

	second := table.reserve(secondKey)
	if err := table.acquire(ctx, 2, secondKey, second.entry, "PSecond"); err != nil {
		t.Fatalf("acquire second lock: %v", err)
	}

	firstAgain := table.reserve(firstKey)
	if err := table.acquire(ctx, 3, firstKey, firstAgain.entry, "PFirstAgain"); err != nil {
		t.Fatalf("reacquire first lock while second is held: %v", err)
	}

	table.unlock(3, firstAgain.entry)
	table.release(firstAgain)
	table.unlock(2, second.entry)
	table.release(second)
	assertLockTableEmpty(t, &table)
}

func TestLockTableCanceledWaiterLeavesReusableEntry(t *testing.T) {
	var table lockTable
	key := Lockey{Type: "role", Key: "470001"}
	holder := table.reserve(key)
	if err := table.acquire(context.Background(), 1, key, holder.entry, "PHolder"); err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waiter := table.reserve(key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := table.acquire(ctx, 2, key, waiter.entry, "PWaiter"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v, want context deadline exceeded", err)
	}
	table.release(waiter)
	if lockTableWaiting(&table, 2) {
		t.Fatal("canceled waiter remains in wait graph")
	}

	table.unlock(1, holder.entry)
	table.release(holder)
	assertLockTableEmpty(t, &table)

	reused := table.reserve(key)
	if err := table.acquire(context.Background(), 3, key, reused.entry, "PReused"); err != nil {
		t.Fatalf("acquire reused lock: %v", err)
	}
	table.unlock(3, reused.entry)
	table.release(reused)
	assertLockTableEmpty(t, &table)
}

func TestLockTableTransfersOwnershipToWaitingTransaction(t *testing.T) {
	var table lockTable
	key := NewLockey("role", 0, 470001)
	holder := table.reserve(key)
	if err := table.acquire(context.Background(), 1, key, holder.entry, "PHolder"); err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waiter := table.reserve(key)
	done := make(chan error, 1)
	go func() {
		done <- table.acquire(context.Background(), 2, key, waiter.entry, "PWaiter")
	}()
	waitForWaitEdge(t, &table, 2)

	table.unlock(1, holder.entry)
	if err := <-done; err != nil {
		t.Fatalf("acquire waiter: %v", err)
	}
	waiter.entry.mu.Lock()
	owner := waiter.entry.owner
	waiter.entry.mu.Unlock()
	if owner != 2 {
		t.Fatalf("lock owner = %d, want 2", owner)
	}
	if lockTableWaiting(&table, 2) {
		t.Fatal("granted waiter remains in wait graph")
	}

	table.unlock(2, waiter.entry)
	table.release(waiter)
	table.release(holder)
	assertLockTableEmpty(t, &table)
}

func TestLockTableReconnectsWaitChainAfterMiddleWaiterCancellation(t *testing.T) {
	var table lockTable
	key := NewLockey("role", 0, 470001)
	holder := table.reserve(key)
	if err := table.acquire(context.Background(), 1, key, holder.entry, "PHolder"); err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	second := table.reserve(key)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- table.acquire(context.Background(), 2, key, second.entry, "PSecond")
	}()
	waitForWaitEdge(t, &table, 2)

	third := table.reserve(key)
	thirdContext, cancelThird := context.WithCancel(context.Background())
	thirdDone := make(chan error, 1)
	go func() {
		thirdDone <- table.acquire(thirdContext, 3, key, third.entry, "PThird")
	}()
	waitForWaitEdge(t, &table, 3)

	fourth := table.reserve(key)
	fourthDone := make(chan error, 1)
	go func() {
		fourthDone <- table.acquire(context.Background(), 4, key, fourth.entry, "PFourth")
	}()
	waitForWaitEdge(t, &table, 4)

	cancelThird()
	if err := <-thirdDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("transaction 3 error = %v, want context canceled", err)
	}
	if blocker, ok := lockTableWaitBlocker(&table, 4); !ok || blocker != 2 {
		t.Fatalf("transaction 4 waits for %d, %v; want transaction 2", blocker, ok)
	}

	table.unlock(1, holder.entry)
	if err := <-secondDone; err != nil {
		t.Fatalf("transaction 2 acquire lock: %v", err)
	}
	table.unlock(2, second.entry)
	if err := <-fourthDone; err != nil {
		t.Fatalf("transaction 4 acquire lock: %v", err)
	}
	table.unlock(4, fourth.entry)

	table.release(fourth)
	table.release(third)
	table.release(second)
	table.release(holder)
	assertLockTableEmpty(t, &table)
}

func TestLockTableRejectsDeadlockWithLockAndProcedureChain(t *testing.T) {
	var table lockTable
	firstKey := NewLockey("role", 0, 1001)
	secondKey := NewLockey("role", 0, 1002)
	firstForOne := table.reserve(firstKey)
	secondForTwo := table.reserve(secondKey)
	if err := table.acquire(context.Background(), 1, firstKey, firstForOne.entry, "PFirst"); err != nil {
		t.Fatalf("transaction 1 acquire first: %v", err)
	}
	if err := table.acquire(context.Background(), 2, secondKey, secondForTwo.entry, "PSecond"); err != nil {
		t.Fatalf("transaction 2 acquire second: %v", err)
	}

	secondForOne := table.reserve(secondKey)
	firstWait := make(chan error, 1)
	go func() {
		firstWait <- table.acquire(context.Background(), 1, secondKey, secondForOne.entry, "PFirst -> PNeedsSecond")
	}()
	waitForWaitEdge(t, &table, 1)

	firstForTwo := table.reserve(firstKey)
	err := table.acquire(context.Background(), 2, firstKey, firstForTwo.entry, "PSecond -> PNeedsFirst")
	var deadlock *DeadlockError
	if !errors.As(err, &deadlock) {
		t.Fatalf("transaction 2 acquire first error = %v, want DeadlockError", err)
	}
	if len(deadlock.Cycle) != 2 || deadlock.Cycle[0].ProcedurePath != "PSecond -> PNeedsFirst" || deadlock.Cycle[1].ProcedurePath != "PFirst -> PNeedsSecond" {
		t.Fatalf("deadlock cycle = %#v, want both Procedure paths", deadlock.Cycle)
	}
	table.release(firstForTwo)

	table.unlock(2, secondForTwo.entry)
	if err := <-firstWait; err != nil {
		t.Fatalf("transaction 1 acquire second: %v", err)
	}
	table.unlock(1, secondForOne.entry)
	table.unlock(1, firstForOne.entry)
	table.release(secondForOne)
	table.release(secondForTwo)
	table.release(firstForOne)
	assertLockTableEmpty(t, &table)
}

func TestLockTableReassignsQueuedWaitersWhenOwnershipChanges(t *testing.T) {
	var table lockTable
	sharedKey := NewLockey("role", 0, 1001)
	thirdKey := NewLockey("role", 0, 1003)
	sharedForOne := table.reserve(sharedKey)
	thirdForThree := table.reserve(thirdKey)
	if err := table.acquire(context.Background(), 1, sharedKey, sharedForOne.entry, "POwner"); err != nil {
		t.Fatalf("transaction 1 acquire shared lock: %v", err)
	}
	if err := table.acquire(context.Background(), 3, thirdKey, thirdForThree.entry, "PThirdOwner"); err != nil {
		t.Fatalf("transaction 3 acquire third lock: %v", err)
	}

	sharedForTwo := table.reserve(sharedKey)
	secondWait := make(chan error, 1)
	go func() {
		secondWait <- table.acquire(context.Background(), 2, sharedKey, sharedForTwo.entry, "PSecond -> PShared")
	}()
	waitForWaitEdge(t, &table, 2)

	sharedForThree := table.reserve(sharedKey)
	thirdWait := make(chan error, 1)
	go func() {
		thirdWait <- table.acquire(context.Background(), 3, sharedKey, sharedForThree.entry, "PThird -> PShared")
	}()
	waitForWaitEdge(t, &table, 3)

	table.unlock(1, sharedForOne.entry)
	if err := <-secondWait; err != nil {
		t.Fatalf("transaction 2 acquire shared lock: %v", err)
	}

	thirdForTwo := table.reserve(thirdKey)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := table.acquire(ctx, 2, thirdKey, thirdForTwo.entry, "PSecond -> PThird")
	var deadlock *DeadlockError
	if !errors.As(err, &deadlock) {
		t.Fatalf("transaction 2 acquire third lock error = %v, want DeadlockError after owner transfer", err)
	}
	if len(deadlock.Cycle) != 2 || deadlock.Cycle[0].Waiter != 2 || deadlock.Cycle[1].Waiter != 3 {
		t.Fatalf("deadlock cycle = %#v, want transaction 2 -> 3 -> 2", deadlock.Cycle)
	}
	table.release(thirdForTwo)

	table.unlock(2, sharedForTwo.entry)
	if err := <-thirdWait; err != nil {
		t.Fatalf("transaction 3 acquire shared lock: %v", err)
	}
	table.unlock(3, sharedForThree.entry)
	table.unlock(3, thirdForThree.entry)
	table.release(sharedForThree)
	table.release(sharedForTwo)
	table.release(thirdForThree)
	table.release(sharedForOne)
	assertLockTableEmpty(t, &table)
}

func waitForWaitEdge(t *testing.T, table *lockTable, transaction TransactionID) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !lockTableWaiting(table, transaction) {
		if time.Now().After(deadline) {
			t.Fatalf("transaction %d did not enter wait graph", transaction)
		}
		time.Sleep(time.Millisecond)
	}
}

func lockTableWaiting(table *lockTable, transaction TransactionID) bool {
	table.waitsMu.Lock()
	_, ok := table.waits.waitsFor[transaction]
	table.waitsMu.Unlock()
	return ok
}

func lockTableWaitBlocker(table *lockTable, transaction TransactionID) (TransactionID, bool) {
	table.waitsMu.Lock()
	edge, ok := table.waits.waitsFor[transaction]
	table.waitsMu.Unlock()
	return edge.blocker, ok
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
