package txn

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
)

// CommitParticipant 连接 Procedure 与写回运行时，由宿主装配，不由业务过程绑定。
// 对象经 Tx.Enlist 首次登记时，在取得其锁后、允许修改该对象前调用 Prepare。
// 一次根事务可以多次预留；后加入对象的预留不表示前面对象尚未修改。
type CommitParticipant interface {
	// Prepare 校验本批对象并预留提交所需资源，成功返回非空预留。
	// 返回错误前须清理本次未交出的资源；创建、删除另由对应参与者处理。
	Prepare(context.Context, []StoreTarget) (CommitReservation, error)
}

// CommitReservation 保存一批对象的预留，由 Runtime 在根提交或失败时收尾。
type CommitReservation interface {
	// Commit 在根提交仍持锁时接收本轮待写目标，只处理本预留负责的对象，
	// 并释放未产生修改的预留。只能进行不可失败的内存登记，不能执行 I/O。
	Commit([]StoreTarget)
	// Abort 在事务失败清理时撤销本预留尚未提交的资源，不进行持久化。
	Abort()
}

// Runtime 执行单机 Procedure，并把成功提交的根对象变更交给可选 CommitParticipant。
// 它不持有业务缓存、存储 writer 或后台 Flush 调度器。
type Runtime struct {
	mu                sync.Mutex
	commitParticipant CommitParticipant
	started           bool
	work              *lifecycle.Ingress
	asyncConfig       AsyncConfig
	asyncRunning      int
	asyncQueue        list.List
}

// Executor 是面向应用的事务契约。提交参与者绑定和生命周期仍由框架负责。
type Executor interface {
	// Call 同步执行根事务、提交后动作和使用权清理；成功不代表最终落盘。
	// 返回 AfterCommitError 时原事务已经提交，不能据此重投；嵌套过程使用 Tx.Call。
	Call(context.Context, Procedure) error
	// Submit 接纳独立异步根事务，不投递 Actor 邮箱；成功仅表示接纳，结果由 Future 获取。
	// 总接纳额度满时返回 ErrAsyncFull；接纳后不继承调用取消，期限包含排队。
	Submit(context.Context, Procedure) (*Future, error)
}

var _ Executor = (*Runtime)(nil)

// NewRuntime 创建纯内存 Procedure 运行时。未绑定 CommitParticipant 时仍可执行不需要
// 延迟持久化的事务，适合纯内存对象和单元测试。
func NewRuntime() *Runtime {
	return &Runtime{}
}

// BindCommitParticipant 绑定 Procedure 的提交参与者。同一 Runtime 只能在首次 Call/Submit 前绑定一次。
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
		return errors.New("txn runtime: cannot bind commit participant after Call/Submit")
	}
	if runtime.commitParticipant != nil {
		return errors.New("txn runtime: commit participant is already bound")
	}
	runtime.commitParticipant = participant
	return nil
}

// Call 同步执行根事务、提交后动作及使用权清理，纳入 Runtime 的排水计数。
// 成功不表示最终落盘；是否登记延迟写回由宿主绑定的 CommitParticipant 决定。
// AfterCommitError 表示原事务已提交但后续动作失败，不能据此重投原事务。
// 死锁受害者有限重试；ctx 取消不强制终止业务函数，返回前仍需完成收尾。
func (runtime *Runtime) Call(ctx context.Context, procedure Procedure) error {
	if runtime == nil {
		return errors.New("txn runtime: runtime is nil")
	}
	work := runtime.workIngress()
	if err := work.BeginWork(); err != nil {
		return err
	}
	defer work.EndWork()
	return runtime.call(ctx, procedure)
}

// Drain 关闭新根事务准入并等待已接纳 Call/Submit 及其同步后续过程结束；取消只结束等待。
// 框架应在业务入口和后台任务排空后调用，成功后才可最终写回和关闭资源。
func (runtime *Runtime) Drain(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	work := runtime.workIngress()
	work.BeginDrain()
	return work.WaitContext(ctx)
}

func (runtime *Runtime) workIngress() *lifecycle.Ingress {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.work == nil {
		runtime.work = lifecycle.NewIngress(nil)
	}
	return runtime.work
}

// call 供已登记的根工作及其同步提交后续过程复用，不重新申请外部准入。
func (runtime *Runtime) call(ctx context.Context, procedure Procedure) error {
	return runtime.execute(ctx, procedure, false)
}

func (runtime *Runtime) callAsync(ctx context.Context, procedure Procedure) error {
	return runtime.execute(ctx, procedure, true)
}

func (runtime *Runtime) execute(ctx context.Context, procedure Procedure, async bool) error {
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
	hooks := callHooks{
		reserveDynamicTargets: managed.reserveAdditional,
		afterAttemptFailure:   managed.abortCommitReservations,
		commitLocked:          managed.commitPrepared,
		allowEmptyLocks:       true,
		callAfterCommit:       runtime.call,
		catchCommittedPanic:   async,
	}
	if async {
		hooks.callAfterCommit = runtime.callAsync
	}
	err := callWithHooks(ctx, managed, CallOptions{
		AllowPendingChanges: true,
		DeadlockRetries:     defaultDeadlockRetries,
	}, hooks)
	if err != nil {
		managed.abortCommitReservations()
	}
	return err
}

type managedProcedure struct {
	procedure         Procedure
	commitParticipant CommitParticipant

	ctx                context.Context
	commitReservations []CommitReservation
}

func (procedure *managedProcedure) transactionProcedureName() string {
	return procedureName(procedure.procedure)
}

func (procedure *managedProcedure) Locks(ctx context.Context) (Lockeys, error) {
	procedure.ctx = ctx
	targets, err := procedure.procedure.Locks(ctx)
	if err != nil {
		return nil, err
	}
	if len(targets) > 0 {
		if _, err := prepareLockeys(targets); err != nil {
			return nil, err
		}
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

func (procedure *managedProcedure) reserveAdditional(targets []StoreTarget) error {
	return procedure.prepareCommit(procedure.ctx, targets)
}

func (procedure *managedProcedure) prepareCommit(ctx context.Context, targets []StoreTarget) error {
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

func (procedure *managedProcedure) commitPrepared(targets []StoreTarget) {
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
