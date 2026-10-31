package actorhost

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidType = errors.New("actor: invalid type")

// Type 声明实体行为与稳定归属；构造声明不会初始化业务实体。
type Type struct {
	name               string
	factory            BehaviorFactory
	slot               SlotResolver
	placement          string
	idle               time.Duration
	idleConfigured     bool
	explicitAssignment bool
}

// Name 返回应用内唯一的实体类型名。
func (k Type) Name() string { return k.name }

// TypeOption 配置框架支持的归属和生命周期策略。
type TypeOption interface{ apply(*Type) error }
type typeOption func(*Type) error

func (f typeOption) apply(k *Type) error { return f(k) }

func NewType(name string, factory BehaviorFactory, options ...TypeOption) (Type, error) {
	k := Type{name: strings.TrimSpace(name), factory: factory}
	if k.name == "" || factory == nil {
		return Type{}, ErrInvalidType
	}
	for _, option := range options {
		if option == nil {
			return Type{}, ErrInvalidType
		}
		if err := option.apply(&k); err != nil {
			return Type{}, err
		}
	}
	return k, nil
}
func MustType(name string, factory BehaviorFactory, options ...TypeOption) Type {
	k, err := NewType(name, factory, options...)
	if err != nil {
		panic(fmt.Sprintf("actor: create type %q: %v", name, err))
	}
	return k
}
func WithSlotMapping(mapping SlotMapping) TypeOption {
	return typeOption(func(k *Type) error {
		if mapping == nil || k.slot != nil || k.placement != "" {
			return ErrInvalidType
		}
		k.slot = mapping.Resolver()
		return nil
	})
}

// RequiresExplicitAssignment 表示归属只能由部署或控制面登记，消息不触发首次选址。
func (k Type) RequiresExplicitAssignment() bool { return k.explicitAssignment }

// WithExplicitAssignment 禁止缺失槽位时自动选址，不改变已有归属的激活与路由。
func WithExplicitAssignment() TypeOption {
	return typeOption(func(k *Type) error { k.explicitAssignment = true; return nil })
}

func WithIdlePassivation(timeout time.Duration) TypeOption {
	return typeOption(func(k *Type) error {
		if timeout != 0 && timeout < time.Millisecond {
			return ErrInvalidType
		}
		k.idle = timeout
		k.idleConfigured = true
		return nil
	})
}

// SlotFor 供组合根把同一声明绑定到寻址、首次分配和权威加载校验。
func SlotFor(ctx context.Context, k Type, id Identity) (string, error) {
	if k.placement != "" {
		return "", fmt.Errorf("actor: placement %q requires runtime resolution", k.placement)
	}
	return k.slot.Resolve(ctx, id)
}

// TypeFactory 只装配选中的本地工厂；运行时策略不修改共享的应用规格。
func TypeFactory(actorTypes []Type, idle map[string]time.Duration) (BehaviorFactory, error) {
	byName := make(map[string]Type, len(actorTypes))
	for _, k := range actorTypes {
		if k.name == "" || k.factory == nil {
			return nil, ErrInvalidType
		}
		if _, exists := byName[k.name]; exists {
			return nil, fmt.Errorf("%w: duplicate %q", ErrInvalidType, k.name)
		}
		byName[k.name] = k
	}
	return func(c Context) (*Behavior, error) {
		k, ok := byName[c.Identity.Type]
		if !ok {
			return nil, ErrActivationDenied
		}
		behavior, err := k.factory(c)
		if behavior != nil {
			if k.idleConfigured {
				behavior.IdlePassivation = k.idle
			} else if timeout := idle[k.name]; timeout > 0 {
				behavior.IdlePassivation = timeout
			}
		}
		return behavior, err
	}, nil
}

// HasIndependentSlots 表示业务显式提供槽位映射；一实体一槽位的唯一性由业务映射保证。
func HasIndependentSlots(k Type) bool { return k.slot != nil }
