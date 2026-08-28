package actor

import (
	"fmt"
	"strings"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
)

// Kind 是业务 Actor 类型的声明。普通项目只需要提供名称和 Actor 构造函数，
// Cluster 放置策略等底层细节由 Runtime 统一装配。
type Kind struct {
	cluster         *cluster.Kind
	props           *protoactor.Props
	producer        func() protoactor.Actor
	placement       kindPlacement
	idlePassivation time.Duration
}

// KindOption 配置单个 Actor Kind 的生命周期策略。
// 具体选项由本包提供，业务不能实现私有配置协议。
type KindOption interface {
	applyKind(*kindOptions) error
}

type kindOptions struct {
	idlePassivation time.Duration
}

type kindOptionFunc func(*kindOptions) error

func (option kindOptionFunc) applyKind(options *kindOptions) error {
	if option == nil || options == nil {
		return ErrInvalidConfig
	}
	return option(options)
}

// WithIdlePassivation 让该 Kind 的实体 Actor 在连续无业务消息达到 timeout 后停止。
// 后续消息仍可通过 Virtual Actor placement 或固定 Kind 宿主惰性重建实体。
func WithIdlePassivation(timeout time.Duration) KindOption {
	return kindOptionFunc(func(options *kindOptions) error {
		if timeout < time.Millisecond {
			return fmt.Errorf("%w: idle passivation must be at least 1ms", ErrInvalidConfig)
		}
		options.idlePassivation = timeout
		return nil
	})
}

type kindPlacement uint8

const (
	kindPlacementVirtual kindPlacement = iota + 1
	kindPlacementFixed
)

// NewKind 创建一个使用默认集群放置策略的 Actor Kind。
func NewKind(name string, producer func() protoactor.Actor, options ...KindOption) (Kind, error) {
	return newKind(name, producer, kindPlacementVirtual, options)
}

// NewFixedKind 创建固定归属的 Actor Kind。固定 Kind 不参与集群重平衡，
// 业务通过稳定逻辑槽位选择宿主实例，Runtime 在该实例内按 RouteKey 惰性激活实体 Actor。
func NewFixedKind(name string, producer func() protoactor.Actor, options ...KindOption) (Kind, error) {
	return newKind(name, producer, kindPlacementFixed, options)
}

func newKind(name string, producer func() protoactor.Actor, placement kindPlacement, values []KindOption) (Kind, error) {
	name = strings.TrimSpace(name)
	if name == "" || producer == nil {
		return Kind{}, ErrInvalidConfig
	}
	var options kindOptions
	for _, value := range values {
		if value == nil {
			return Kind{}, ErrInvalidConfig
		}
		if err := value.applyKind(&options); err != nil {
			return Kind{}, err
		}
	}
	originalProducer := producer
	if options.idlePassivation > 0 {
		producer = idlePassivationProducer(producer, options.idlePassivation)
	}
	props := protoactor.PropsFromProducer(producer)
	return Kind{
		cluster:         cluster.NewKind(name, props),
		props:           props,
		producer:        originalProducer,
		placement:       placement,
		idlePassivation: options.idlePassivation,
	}, nil
}

// MustKind 创建静态 Actor Kind，配置错误时立即终止启动。
func MustKind(name string, producer func() protoactor.Actor, options ...KindOption) Kind {
	kind, err := NewKind(name, producer, options...)
	if err != nil {
		panic(fmt.Sprintf("actor: create kind %q: %v", name, err))
	}
	return kind
}

// MustFixedKind 创建静态固定归属 Actor Kind，配置错误时立即终止启动。
func MustFixedKind(name string, producer func() protoactor.Actor, options ...KindOption) Kind {
	kind, err := NewFixedKind(name, producer, options...)
	if err != nil {
		panic(fmt.Sprintf("actor: create fixed kind %q: %v", name, err))
	}
	return kind
}

// AdaptKind 将 ProtoActor 原生 Kind 接入 tx。它是高级扩展入口，普通项目应使用 NewKind。
func AdaptKind(kind *cluster.Kind) (Kind, error) {
	if kind == nil || strings.TrimSpace(kind.Kind) == "" || kind.Props == nil {
		return Kind{}, ErrInvalidConfig
	}
	return Kind{cluster: kind, props: kind.Props, placement: kindPlacementVirtual}, nil
}

func (kind Kind) isFixed() bool {
	return kind.placement == kindPlacementFixed
}

// IsFixed 报告该 Kind 是否由稳定逻辑槽位固定到指定进程，不参与虚拟 Actor 重平衡。
func (kind Kind) IsFixed() bool {
	return kind.isFixed()
}

// Name 返回 Kind 的稳定名称。
func (kind Kind) Name() string {
	if kind.cluster == nil {
		return ""
	}
	return kind.cluster.Kind
}

func validateKinds(kinds []Kind) error {
	names := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		name := kind.Name()
		if name == "" || kind.cluster == nil || kind.cluster.Props == nil || kind.props == nil ||
			(kind.placement != kindPlacementVirtual && kind.placement != kindPlacementFixed) {
			return ErrInvalidConfig
		}
		if kind.idlePassivation > 0 && kind.idlePassivation < time.Millisecond {
			return ErrInvalidConfig
		}
		if _, exists := names[name]; exists {
			return fmt.Errorf("%w: duplicate Actor kind %q", ErrInvalidConfig, name)
		}
		names[name] = struct{}{}
	}
	return nil
}
