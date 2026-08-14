package actor

import (
	"context"
	"errors"
	"fmt"
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
)

var (
	ErrRuntimeNotStarted = errors.New("actor: runtime is not started")
	ErrRuntimeStopped    = errors.New("actor: runtime is stopped")
	ErrInvalidPID        = errors.New("actor: invalid pid")
)

// Runtime 管理一个集群成员，并将 Actor 生命周期、成员发现和虚拟 Actor 放置交给 ProtoActor。
type Runtime struct {
	config Config
	kinds  []*cluster.Kind

	mu      sync.Mutex
	cluster *cluster.Cluster
	started bool
	stopped bool
}

// New 创建尚未启动的 Actor Cluster 运行时。
func New(config Config, kinds ...*cluster.Kind) (*Runtime, error) {
	config, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	return &Runtime{config: config, kinds: append([]*cluster.Kind(nil), kinds...)}, nil
}

// Start 校验 etcd 可达性，并将当前进程启动为集群成员。停止由调用方在完成
// 请求排空和最终持久化后显式调用 Shutdown，避免成员过早离开集群。
func (r *Runtime) Start(ctx context.Context) (err error) {
	if ctx == nil {
		return ErrInvalidConfig
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

	provider, err := actorEtcd.NewWithConfig(r.config.Etcd.Prefix, clientv3.Config{
		Endpoints:   r.config.Etcd.Endpoints,
		DialTimeout: r.config.Etcd.DialTimeout,
	})
	if err != nil {
		return fmt.Errorf("actor: create etcd provider: %w", err)
	}
	system := protoactor.NewActorSystem()
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
	r.started = true
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
	r.mu.Lock()
	defer r.mu.Unlock()
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

// ActorSystem 返回底层 ActorSystem，供项目注册固定归属的本地 Actor 或向已解析 PID 发起请求。
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

// Tell 将一条单向 Delivery 投递到 Virtual 或固定归属的 Actor PID。
//
// 返回 nil 仅表示本地 Runtime 已接受投递；远端业务处理结果、超时和重试由消息契约负责。
// 需要显式响应时，调用方应通过 ActorSystem 使用 RequestFuture。
func (r *Runtime) Tell(pid *protoactor.PID, delivery Delivery) error {
	if pid == nil {
		return ErrInvalidPID
	}
	system, err := r.ActorSystem()
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

// FixedPID 返回固定归属 Actor 的直连 Remote PID。实体归属由业务维护，不参与集群重平衡。
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
