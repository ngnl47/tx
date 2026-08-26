package txn

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestWaitGraphAddsAndRemovesAcyclicWait(t *testing.T) {
	var graph waitGraph
	lock := NewLockKey("role", 1002)

	if err := graph.add(1, 2, lock, "PTrade -> PGrantItem"); err != nil {
		t.Fatalf("add returned error: %v", err)
	}
	if edge, ok := graph.waitsFor[1]; !ok || edge.blocker != 2 || edge.lock != lock || edge.procedurePath != "PTrade -> PGrantItem" {
		t.Fatalf("wait edge = %#v, want owner 2 and complete diagnostic context", edge)
	}

	graph.remove(1)
	if _, ok := graph.waitsFor[1]; ok {
		t.Fatal("wait edge remains after remove")
	}
}

func TestWaitGraphRejectsCycleWithCompleteProcedureChain(t *testing.T) {
	var graph waitGraph
	roleLock := NewLockKey("role", 1002)
	guildLock := NewLockKey("guild", 7)

	if err := graph.add(1, 2, roleLock, "PGuildTrade -> PGrantItem"); err != nil {
		t.Fatalf("add first edge: %v", err)
	}
	err := graph.add(2, 1, guildLock, "PRefreshGuild -> PLoadOwner")
	var deadlock *DeadlockError
	if !errors.Is(err, ErrDeadlock) || !errors.As(err, &deadlock) {
		t.Fatalf("add cycle error = %v, want DeadlockError", err)
	}
	want := []DeadlockStep{
		{
			Waiter:        2,
			Blocker:       1,
			Lock:          guildLock,
			ProcedurePath: "PRefreshGuild -> PLoadOwner",
		},
		{
			Waiter:        1,
			Blocker:       2,
			Lock:          roleLock,
			ProcedurePath: "PGuildTrade -> PGrantItem",
		},
	}
	if !reflect.DeepEqual(deadlock.Cycle, want) {
		t.Fatalf("deadlock cycle = %#v, want %#v", deadlock.Cycle, want)
	}
	for _, part := range []string{
		"transaction 2",
		"PRefreshGuild -> PLoadOwner",
		"guild:7",
		"transaction 1",
		"PGuildTrade -> PGrantItem",
		"role:1002",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("deadlock error %q does not contain %q", err, part)
		}
	}
	if _, ok := graph.waitsFor[2]; ok {
		t.Fatal("rejected cycle edge was stored in wait graph")
	}
}

func TestWaitGraphDetectsLongCycle(t *testing.T) {
	var graph waitGraph
	if err := graph.add(1, 2, NewLockKey("role", 2), "PFirst -> PSecond"); err != nil {
		t.Fatalf("add 1 -> 2: %v", err)
	}
	if err := graph.add(2, 3, NewLockKey("role", 3), "PSecond -> PThird"); err != nil {
		t.Fatalf("add 2 -> 3: %v", err)
	}
	err := graph.add(3, 1, NewLockKey("role", 1), "PThird -> PFirst")
	var deadlock *DeadlockError
	if !errors.As(err, &deadlock) {
		t.Fatalf("add 3 -> 1 error = %v, want DeadlockError", err)
	}
	if len(deadlock.Cycle) != 3 {
		t.Fatalf("cycle length = %d, want 3", len(deadlock.Cycle))
	}
	for index, waiter := range []TransactionID{3, 1, 2} {
		if deadlock.Cycle[index].Waiter != waiter {
			t.Fatalf("cycle[%d].Waiter = %d, want %d", index, deadlock.Cycle[index].Waiter, waiter)
		}
	}
}
