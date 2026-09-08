package actor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	actorEtcd "github.com/asynkron/protoactor-go/cluster/clusterproviders/etcd"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/disthash"
	"github.com/asynkron/protoactor-go/remote"
	clientv3 "go.etcd.io/etcd/client/v3"

	txlog "github.com/ngnl47/tx/log"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

var (
	ErrRuntimeNotStarted        = errors.New("actor: runtime is not started")
	ErrRuntimeDraining          = errors.New("actor: runtime is draining")
	ErrRuntimeStopped           = errors.New("actor: runtime is stopped")
	ErrLifecycleContextRequired = errors.New("actor: lifecycle WaitGroupContext is required")
	ErrInvalidPID               = errors.New("actor: invalid pid")
)

// Messenger 是面向服务的 Actor 契约。集群启动、关闭和底层 ActorSystem 访问仍由 runtime 管理。
type Messenger interface {
	VirtualPID(kind, identity string) (*protoactor.PID, error)
	Tell(pid *protoactor.PID, delivery Delivery) error
	TellAccepted(ctx context.Context, pid *protoactor.PID, delivery Delivery) error
	Call(ctx context.Context, pid *protoactor.PID, delivery Delivery) (Result, error)
	CallFuture(ctx context.Context, pid *protoactor.PID, delivery Delivery) (*Future, error)
}

var _ Messenger = (*Runtime)(nil)

// Runtime 管理一个集群成员，并将 Actor 生命周期、成员发现和虚拟 Actor 放置交给 ProtoActor。
type Runtime struct {
	config    Config
	kinds     []*cluster.Kind
	fixed     []fixedKind
	lifecycle *mailboxLifecycle

	mu        sync.RWMutex
	cluster   *cluster.Cluster
	responder *protoactor.PID
	pending   *pendingCalls
	started   bool
	draining  bool
	stopped   bool
}

// New 创建尚未启动的 Actor Cluster 运行时。
func New(config Config, kinds ...Kind) (*Runtime, error) {
	config, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	lifecycle := &mailboxLifecycle{}
	configuredKinds, fixedKinds, err := configureKinds(config, kinds, lifecycle)
	if err != nil {
		return nil, err
	}
	return &Runtime{
		config:    config,
		kinds:     configuredKinds,
		fixed:     fixedKinds,
		lifecycle: lifecycle,
		pending:   newPendingCalls(config.MaxPendingCalls),
	}, nil
}

// Start 校验 etcd 可达性，并将当前进程启动为集群成员。停止由调用方在完成
// 请求排空和最终持久化后显式调用 Shutdown，避免成员过早离开集群。
func (r *Runtime) Start(ctx context.Context) (err error) {
	if ctx == nil {
		return ErrInvalidConfig
	}
	root, ok := actorLifecycleRoot(ctx)
	if !ok {
		return ErrLifecycleContextRequired
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrRuntimeStopped
	}
	if r.started {
		return nil
	}
	if err := r.checkEtcd(ctx); err != nil {
		return err
	}
	r.lifecycle.bind(root)

	provider, err := actorEtcd.NewWithConfig(r.config.Etcd.Prefix, clientv3.Config{
		Endpoints:   r.config.Etcd.Endpoints,
		DialTimeout: r.config.Etcd.DialTimeout,
	})
	if err != nil {
		return fmt.Errorf("actor: create etcd provider: %w", err)
	}
	system := newActorSystem(r.config.LogLevel)
	system.Extensions.Register(&actorLifecycleExtension{context: ctx})
	remoteConfig := remote.Configure(r.config.Host, r.config.Port)
	clusterConfig := cluster.Configure(
		r.config.ClusterName,
		provider,
		disthash.New(),
		remoteConfig,
		cluster.WithKinds(r.kinds...),
	)
	r.cluster = cluster.New(system, clusterConfig)
	if err := startMember(r.cluster); err != nil {
		r.stopLocked()
		return err
	}
	if err := r.startFixedHosts(system); err != nil {
		r.stopLocked()
		return err
	}
	responder, err := system.Root.SpawnNamed(protoactor.PropsFromProducer(func() protoactor.Actor {
		return responseActor{pending: r.pending}
	}), "tx-call-responses")
	if err != nil {
		r.stopLocked()
		return fmt.Errorf("actor: start response receiver: %w", err)
	}
	r.responder = responder
	r.started = true
	return nil
}

func newActorSystem(level LogLevel) *protoactor.ActorSystem {
	return protoactor.NewActorSystem(protoactor.WithLoggerFactory(func(system *protoactor.ActorSystem) *slog.Logger {
		return newRuntimeLogger(system.ID, level)
	}))
}

// Drain 停止接受新的 tx Delivery，并等待已经进入 Actor 邮箱的业务消息处理完成。
// 它不会取消根 context，也不会关闭 ProtoActor 的监督和停止控制消息。
func (r *Runtime) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.BeginDrain(); err != nil {
		return err
	}
	if err := r.lifecycle.waitDrained(ctx); err != nil {
		pending := r.lifecycle.pending()
		txlog.Error("actor runtime drain failed", "pending", pending, "err", err)
		return fmt.Errorf("actor: drain %d pending deliveries: %w", pending, err)
	}
	return nil
}

// BeginDrain 立即关闭新业务准入，不等待邮箱排空，也不停止控制消息。
func (r *Runtime) BeginDrain() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrRuntimeStopped
	}
	if !r.started || r.cluster == nil {
		return ErrRuntimeNotStarted
	}
	r.draining = true
	r.lifecycle.beginDrain()
	return nil
}

// Shutdown 将成员从集群移除。项目应先完成自身请求排空和最终持久化，再取消生命周期上下文。
func (r *Runtime) Shutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil
	}
	r.stopped = true
	r.stopLocked()
	return nil
}

// VirtualPID 按 kind 和 identity 解析可恢复的虚拟 Actor。
func (r *Runtime) VirtualPID(kind, identity string) (*protoactor.PID, error) {
	if strings.TrimSpace(kind) == "" || strings.TrimSpace(identity) == "" {
		return nil, ErrInvalidPID
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.stopped {
		return nil, ErrRuntimeStopped
	}
	if !r.started || r.cluster == nil {
		return nil, ErrRuntimeNotStarted
	}
	pid := r.cluster.Get(identity, kind)
	if pid == nil {
		return nil, fmt.Errorf("actor: resolve virtual pid kind=%q identity=%q", kind, identity)
	}
	return pid, nil
}

// ActorSystem 返回底层 ActorSystem，是注册本地 Actor 等定制能力的高级扩展入口。
// 普通业务投递和请求应使用 Messenger 的四个稳定方法。
func (r *Runtime) ActorSystem() (*protoactor.ActorSystem, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrRuntimeStopped
	}
	if !r.started || r.cluster == nil {
		return nil, ErrRuntimeNotStarted
	}
	return r.cluster.ActorSystem, nil
}

// Endpoint 返回当前 Actor Remote 对外发布的实际地址。
func (r *Runtime) Endpoint() (string, error) {
	system, err := r.ActorSystem()
	if err != nil {
		return "", err
	}
	endpoint := strings.TrimSpace(system.Address())
	if endpoint == "" {
		return "", ErrRuntimeNotStarted
	}
	return endpoint, nil
}

// Tell 将一条单向 Delivery 投递到 Virtual 或固定归属的 Actor PID。
//
// 返回 nil 仅表示本地 Runtime 已接受投递；需要接收确认或业务结果时分别使用
// TellAccepted、CallFuture 或 Call。
func (r *Runtime) Tell(pid *protoactor.PID, delivery Delivery) error {
	if pid == nil {
		return ErrInvalidPID
	}
	system, err := r.deliveryActorSystem()
	if err != nil {
		return err
	}
	message, err := delivery.ToProto()
	if err != nil {
		return err
	}
	system.Root.Send(pid, message)
	return nil
}

func (r *Runtime) deliveryActorSystem() (*protoactor.ActorSystem, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrRuntimeStopped
	}
	if r.draining {
		return nil, ErrRuntimeDraining
	}
	if !r.started || r.cluster == nil {
		return nil, ErrRuntimeNotStarted
	}
	return r.cluster.ActorSystem, nil
}

// FixedPID 返回底层直连 Remote PID。普通固定实体应使用 FixedKindPID；该入口保留给
// 已经自行管理本地 Actor ID 和生命周期的高级扩展。
func FixedPID(endpoint, actorID string) (*protoactor.PID, error) {
	endpoint = strings.TrimSpace(endpoint)
	actorID = strings.TrimSpace(actorID)
	if endpoint == "" || actorID == "" {
		return nil, ErrInvalidPID
	}
	if _, _, err := net.SplitHostPort(endpoint); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPID, err)
	}
	return protoactor.NewPID(endpoint, actorID), nil
}

func (r *Runtime) checkEtcd(ctx context.Context) error {
	client, err := clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   r.config.Etcd.Endpoints,
		DialTimeout: r.config.Etcd.DialTimeout,
	})
	if err != nil {
		return fmt.Errorf("actor: create etcd client: %w", err)
	}
	defer client.Close()
	probeCtx, cancel := context.WithTimeout(ctx, r.config.Etcd.DialTimeout)
	defer cancel()
	if _, err := client.Get(probeCtx, r.config.Etcd.Prefix); err != nil {
		return fmt.Errorf("actor: check etcd: %w", err)
	}
	return nil
}

func (r *Runtime) stopLocked() {
	if r.pending != nil {
		r.pending.failAll(ErrRuntimeStopped)
	}
	r.responder = nil
	if r.cluster == nil {
		return
	}
	cluster := r.cluster
	r.cluster = nil
	r.started = false
	defer func() {
		if recovered := recover(); recovered != nil {
			txlog.Error("actor runtime forced shutdown failed", "panic", recovered)
		}
	}()
	cluster.Shutdown(true)
}

func startMember(cluster *cluster.Cluster) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("actor: start cluster member: %v", recovered)
		}
	}()
	cluster.StartMember()
	return nil
}

func configureKinds(config Config, kinds []Kind, lifecycle *mailboxLifecycle) ([]*cluster.Kind, []fixedKind, error) {
	if err := validateKinds(kinds); err != nil {
		return nil, nil, err
	}
	configured := make([]*cluster.Kind, 0, len(kinds))
	fixed := make([]fixedKind, 0, len(kinds))
	for _, kind := range kinds {
		clusterKind := kind.cluster
		baseProps := clusterKind.Props
		if kind.isFixed() {
			baseProps = kind.props
		}
		if kind.idlePassivation == 0 {
			if timeout := config.IdlePassivation[clusterKind.Kind]; timeout > 0 {
				if kind.producer == nil {
					return nil, nil, fmt.Errorf("%w: adapted Actor kind %q cannot apply runtime idle passivation", ErrInvalidConfig, clusterKind.Kind)
				}
				baseProps = protoactor.PropsFromProducer(idlePassivationProducer(kind.producer, timeout))
			}
		}
		props := baseProps.Clone(
			protoactor.WithMailbox(observedBoundedMailbox(config, clusterKind.Kind, lifecycle)),
			protoactor.WithReceiverMiddleware(acceptanceReceiverMiddleware),
		)
		if kind.isFixed() {
			fixed = append(fixed, fixedKind{name: clusterKind.Kind, props: props})
			continue
		}
		configured = append(configured, &cluster.Kind{
			Kind:            clusterKind.Kind,
			Props:           props,
			StrategyBuilder: clusterKind.StrategyBuilder,
		})
	}
	return configured, fixed, nil
}

func (r *Runtime) startFixedHosts(system *protoactor.ActorSystem) error {
	for _, kind := range r.fixed {
		props, err := newFixedHostProps(kind.name, kind.props)
		if err != nil {
			return fmt.Errorf("actor: create fixed kind host %q: %w", kind.name, err)
		}
		props = props.Clone(protoactor.WithMailbox(observedBoundedMailbox(r.config, kind.name+".host", r.lifecycle)))
		hostID, err := fixedKindHostID(kind.name)
		if err != nil {
			return fmt.Errorf("actor: resolve fixed kind host %q: %w", kind.name, err)
		}
		if _, err := system.Root.SpawnNamed(props, hostID); err != nil {
			return fmt.Errorf("actor: start fixed kind host %q: %w", kind.name, err)
		}
	}
	return nil
}

func acceptanceReceiverMiddleware(next protoactor.ReceiverFunc) protoactor.ReceiverFunc {
	return func(ctx protoactor.ReceiverContext, envelope *protoactor.MessageEnvelope) {
		if delivery, ok := envelope.Message.(*actorpb.Delivery); ok && delivery.GetMode() == actorpb.DeliveryMode_DELIVERY_MODE_ACCEPTED && envelope.Sender != nil {
			ctx.ActorSystem().Root.Send(envelope.Sender, &actorpb.Acceptance{
				CommandId: delivery.GetCommandId(),
				RequestId: delivery.GetRequestId(),
			})
		}
		next(ctx, envelope)
	}
}
