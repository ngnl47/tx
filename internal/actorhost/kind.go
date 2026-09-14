package actorhost

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidKind = errors.New("actor: invalid kind")

// Kind 声明实体行为与稳定归属；构造声明不会初始化业务实体。
type Kind struct {
	name    string
	factory BehaviorFactory
	slot    SlotMapping
	idle    time.Duration
}

// Name 返回应用内唯一的实体类型名。
func (k Kind) Name() string { return k.name }

// KindOption 配置框架支持的归属和生命周期策略。
type KindOption interface{ apply(*Kind) error }
type kindOption func(*Kind) error

func (f kindOption) apply(k *Kind) error { return f(k) }

func NewKind(name string, factory BehaviorFactory, options ...KindOption) (Kind, error) {
	k := Kind{name: strings.TrimSpace(name), factory: factory}
	if k.name == "" || factory == nil {
		return Kind{}, ErrInvalidKind
	}
	for _, option := range options {
		if option == nil {
			return Kind{}, ErrInvalidKind
		}
		if err := option.apply(&k); err != nil {
			return Kind{}, err
		}
	}
	return k, nil
}
func MustKind(name string, factory BehaviorFactory, options ...KindOption) Kind {
	k, err := NewKind(name, factory, options...)
	if err != nil {
		panic(fmt.Sprintf("actor: create kind %q: %v", name, err))
	}
	return k
}
func WithSlotMapping(mapping SlotMapping) KindOption {
	return kindOption(func(k *Kind) error {
		if mapping == nil {
			return ErrInvalidKind
		}
		k.slot = mapping
		return nil
	})
}
func WithIdlePassivation(timeout time.Duration) KindOption {
	return kindOption(func(k *Kind) error {
		if timeout < time.Millisecond {
			return ErrInvalidKind
		}
		k.idle = timeout
		return nil
	})
}

// SlotFor 供组合根把同一声明绑定到寻址、首次分配和权威加载校验。
func SlotFor(k Kind, id Identity) string { return k.slot.Resolve(id) }

// KindFactory 只装配选中的本地工厂；运行时策略不修改共享的应用规格。
func KindFactory(kinds []Kind, idle map[string]time.Duration) (BehaviorFactory, error) {
	byName := make(map[string]Kind, len(kinds))
	for _, k := range kinds {
		if k.name == "" || k.factory == nil {
			return nil, ErrInvalidKind
		}
		if _, exists := byName[k.name]; exists {
			return nil, fmt.Errorf("%w: duplicate %q", ErrInvalidKind, k.name)
		}
		byName[k.name] = k
	}
	return func(c Context) (*Behavior, error) {
		k, ok := byName[c.Identity.Kind]
		if !ok {
			return nil, ErrActivationDenied
		}
		behavior, err := k.factory(c)
		if behavior != nil {
			timeout := k.idle
			if timeout == 0 {
				timeout = idle[k.name]
			}
			if timeout > 0 {
				behavior.IdlePassivation = timeout
			}
		}
		return behavior, err
	}, nil
}

// HasIndependentSlots 表示业务显式提供槽位映射；一实体一槽位的唯一性由业务映射保证。
func HasIndependentSlots(k Kind) bool { return k.slot != nil }
