package txn

import (
	"context"
	"errors"
	"sync"
)

// CommitParticipant 是 Procedure 与有状态缓存运行时之间的提交边界。
// Prepare 必须在业务修改前完成全部可失败校验和资源预留。
type CommitParticipant interface {
	Prepare(context.Context, []LockTarget) (CommitReservation, error)
}

// CommitReservation 保存一次提交前预留。Commit 在内存事务成功且仍持锁时调用，不能失败或进行 I/O；
// Abort 在事务失败或 panic 回滚后调用。
type CommitReservation interface {
	Commit([]LockTarget)
	Abort()
}

// Runtime 执行单机 Procedure，并把成功提交的根对象变更交给可选 CommitParticipant。
// 它不持有业务缓存、存储 writer 或后台 Flush 调度器。
type Runtime struct {
	mu                sync.Mutex
	commitParticipant CommitParticipant
	started           bool
}

// Executor 是面向应用的事务契约。提交参与者绑定和生命周期仍由框架负责。
type Executor interface {
	Call(context.Context, Procedure) error
}

var _ Executor = (*Runtime)(nil)

// NewRuntime 创建纯内存 Procedure 运行时。未绑定 CommitParticipant 时仍可执行不需要
// 延迟持久化的事务，适合纯内存对象和单元测试。
func NewRuntime() *Runtime {
	return &Runtime{}
}

// BindCommitParticipant 绑定 Procedure 的提交参与者。同一 Runtime 只能在首次 Call 前绑定一次。
// 该入口供框架 CMod 组装使用，普通业务不应直接调用。
func (runtime *Runtime) BindCommitParticipant(participant CommitParticipant) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if isNil(participant) {
		return errors.New("txn runtime: commit participant is nil")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.started {
		return errors.New("txn runtime: cannot bind commit participant after Call")
	}
	if runtime.commitParticipant != nil {
		return errors.New("txn runtime: commit participant is already bound")
	}
	runtime.commitParticipant = participant
	return nil
}

// Call 执行一个单机 Procedure。成功只表示内存事务已经提交；是否登记延迟刷盘
// 由框架绑定的 CommitParticipant 决定。
func (runtime *Runtime) Call(ctx context.Context, procedure Procedure) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	if isNil(procedure) {
		return errors.New("txn runtime: procedure is nil")
	}
	runtime.mu.Lock()
	runtime.started = true
	participant := runtime.commitParticipant
	runtime.mu.Unlock()

	managed := &managedProcedure{
		procedure:         procedure,
		commitParticipant: participant,
		ctx:               ctx,
	}
	err := callWithHooks(ctx, managed, CallOptions{
		AllowPendingChanges: true,
		DeadlockRetries:     defaultDeadlockRetries,
	}, callHooks{
		reserveDynamicTargets: managed.reserveAdditional,
		afterAttemptFailure:   managed.abortCommitReservations,
		commitLocked:          managed.commitPrepared,
		allowEmptyLocks:       true,
	})
	if err != nil {
		managed.abortCommitReservations()
	}
	return err
}

type managedProcedure struct {
	procedure          Procedure
	commitParticipant  CommitParticipant
	ctx                context.Context
	commitReservations []CommitReservation
}

func (procedure *managedProcedure) transactionProcedureName() string {
	return procedureName(procedure.procedure)
}

func (procedure *managedProcedure) Locks(ctx context.Context) ([]LockTarget, error) {
	procedure.ctx = ctx
	targets, err := procedure.procedure.Locks(ctx)
	if err != nil {
		return nil, err
	}
	if len(targets) > 0 {
		if _, err := prepareLocks(targets); err != nil {
			return nil, err
		}
	}
	if err := procedure.prepareCommit(ctx, targets); err != nil {
		return nil, err
	}
	return targets, nil
}

func (procedure *managedProcedure) Process(tx *Tx) error {
	tx.AfterRollback(func(context.Context) {
		procedure.abortCommitReservations()
	})
	tx.creator, _ = procedure.commitParticipant.(CreationParticipant)
	tx.remover, _ = procedure.commitParticipant.(RemovalParticipant)
	return procedure.procedure.Process(tx)
}

func (procedure *managedProcedure) reserveAdditional(targets []LockTarget) error {
	return procedure.prepareCommit(procedure.ctx, targets)
}

func (procedure *managedProcedure) prepareCommit(ctx context.Context, targets []LockTarget) error {
	if procedure == nil || procedure.commitParticipant == nil {
		return nil
	}
	reservation, err := procedure.commitParticipant.Prepare(ctx, targets)
	if err != nil {
		return err
	}
	if isNil(reservation) {
		return errors.New("txn runtime: commit participant returned nil reservation")
	}
	procedure.commitReservations = append(procedure.commitReservations, reservation)
	return nil
}

func (procedure *managedProcedure) commitPrepared(targets []LockTarget) {
	if procedure == nil {
		return
	}
	for _, reservation := range procedure.commitReservations {
		reservation.Commit(targets)
	}
	procedure.commitReservations = nil
}

func (procedure *managedProcedure) abortCommitReservations() {
	if procedure == nil {
		return
	}
	for _, reservation := range procedure.commitReservations {
		reservation.Abort()
	}
	procedure.commitReservations = nil
}
