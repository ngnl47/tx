package actor

import (
	"fmt"
	"strings"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
)

// Kind 是业务 Actor 类型的声明。普通项目只需要提供名称和 Actor 构造函数，
// Cluster 放置策略等底层细节由 Runtime 统一装配。
type Kind struct {
	cluster *cluster.Kind
}

// NewKind 创建一个使用默认集群放置策略的 Actor Kind。
func NewKind(name string, producer func() protoactor.Actor) (Kind, error) {
	name = strings.TrimSpace(name)
	if name == "" || producer == nil {
		return Kind{}, ErrInvalidConfig
	}
	return Kind{cluster: cluster.NewKind(name, protoactor.PropsFromProducer(producer))}, nil
}

// MustKind 创建静态 Actor Kind，配置错误时立即终止启动。
func MustKind(name string, producer func() protoactor.Actor) Kind {
	kind, err := NewKind(name, producer)
	if err != nil {
		panic(fmt.Sprintf("actor: create kind %q: %v", name, err))
	}
	return kind
}

// AdaptKind 将 ProtoActor 原生 Kind 接入 tx。它是高级扩展入口，普通项目应使用 NewKind。
func AdaptKind(kind *cluster.Kind) (Kind, error) {
	if kind == nil || strings.TrimSpace(kind.Kind) == "" || kind.Props == nil {
		return Kind{}, ErrInvalidConfig
	}
	return Kind{cluster: kind}, nil
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
		if name == "" || kind.cluster == nil || kind.cluster.Props == nil {
			return ErrInvalidConfig
		}
		if _, exists := names[name]; exists {
			return fmt.Errorf("%w: duplicate Actor kind %q", ErrInvalidConfig, name)
		}
		names[name] = struct{}{}
	}
	return nil
}
