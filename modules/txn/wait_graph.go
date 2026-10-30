package txn

import (
	"errors"
	"fmt"
	"strings"
)

// TransactionID 是进程内根事务的诊断身份。
// 它只用于本机锁所有权和等待链，不是持久化版本或分布式事务编号。
type TransactionID uint64

// ErrDeadlock 表示当前锁请求会形成循环等待。
var ErrDeadlock = errors.New("transaction: deadlock detected")

// DeadlockStep 描述等待环中的一条边。
type DeadlockStep struct {
	// Waiter 是正在等待推进的根事务。
	Waiter TransactionID
	// Blocker 是 Waiter 的直接阻塞事务，可能是锁 owner 或 FIFO 前驱。
	Blocker TransactionID
	// Lock 是 Waiter 本次请求的精确业务锁键。
	Lock Lockey
	// ProcedurePath 是 Waiter 发起锁请求时的完整 Procedure 调用路径。
	ProcedurePath string
}

// DeadlockError 保存一次死锁检测得到的完整等待环。
// Cycle 从本次被拒绝的锁请求开始，按等待方向最终回到 Waiter。
type DeadlockError struct {
	Cycle []DeadlockStep
}

func (err *DeadlockError) Error() string {
	if err == nil || len(err.Cycle) == 0 {
		return ErrDeadlock.Error()
	}
	var message strings.Builder
	message.WriteString(ErrDeadlock.Error())
	message.WriteString(": ")
	for index, step := range err.Cycle {
		if index > 0 {
			message.WriteString(" -> ")
		}
		fmt.Fprintf(
			&message,
			"transaction %d [%s] waits for %s blocked by transaction %d",
			step.Waiter,
			step.ProcedurePath,
			step.Lock.String(),
			step.Blocker,
		)
	}
	return message.String()
}

// Is 允许调用方通过 errors.Is(err, ErrDeadlock) 判断死锁受害者错误。
func (err *DeadlockError) Is(target error) bool {
	return target == ErrDeadlock
}

type waitEdge struct {
	blocker       TransactionID
	lock          Lockey
	procedurePath string
}

// waitGraph 只保存正在阻塞的根事务。同步 Procedure 一次只能等待一把锁，
// 因此每个事务至多有一条出边，成环检测只需沿 blocker 链前进。
// 并发控制由锁管理器负责，本结构本身不加锁。
type waitGraph struct {
	waitsFor map[TransactionID]waitEdge
}

func (graph *waitGraph) add(waiter, blocker TransactionID, lock Lockey, procedurePath string) error {
	newEdge := waitEdge{blocker: blocker, lock: lock, procedurePath: procedurePath}
	if waiter == blocker {
		return &DeadlockError{Cycle: []DeadlockStep{deadlockStep(waiter, newEdge)}}
	}
	for current := blocker; current != 0; {
		if current == waiter {
			return graph.deadlock(waiter, newEdge)
		}
		edge, ok := graph.waitsFor[current]
		if !ok {
			break
		}
		current = edge.blocker
	}
	if graph.waitsFor == nil {
		graph.waitsFor = make(map[TransactionID]waitEdge)
	}
	graph.waitsFor[waiter] = newEdge
	return nil
}

func (graph *waitGraph) remove(waiter TransactionID) {
	delete(graph.waitsFor, waiter)
}

func (graph *waitGraph) reassignBlocker(waiter, blocker TransactionID) {
	edge, ok := graph.waitsFor[waiter]
	if !ok {
		return
	}
	edge.blocker = blocker
	graph.waitsFor[waiter] = edge
}

func (graph *waitGraph) deadlock(waiter TransactionID, newEdge waitEdge) *DeadlockError {
	cycle := []DeadlockStep{deadlockStep(waiter, newEdge)}
	for current := newEdge.blocker; current != waiter; {
		edge := graph.waitsFor[current]
		cycle = append(cycle, deadlockStep(current, edge))
		current = edge.blocker
	}
	return &DeadlockError{Cycle: cycle}
}

func deadlockStep(waiter TransactionID, edge waitEdge) DeadlockStep {
	return DeadlockStep{
		Waiter:        waiter,
		Blocker:       edge.blocker,
		Lock:          edge.lock,
		ProcedurePath: edge.procedurePath,
	}
}
