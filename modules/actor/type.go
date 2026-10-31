package actor

import (
	"github.com/ngnl47/tx/internal/actorhost"
	"time"
)

// Type 是实体行为、稳定归属及生命周期策略的声明。
type Type = actorhost.Type

// TypeOption 配置单个 Type，业务不需要实现配置协议。
type TypeOption = actorhost.TypeOption

// NewType 声明行为工厂；默认使用 1024 个稳定分区，工厂只在实体激活时执行。
func NewType(name string, factory BehaviorFactory, options ...TypeOption) (Type, error) {
	return actorhost.NewType(name, factory, options...)
}

// MustType 用于静态声明，声明错误时 panic；运行时加载错误仍正常返回。
func MustType(name string, factory BehaviorFactory, options ...TypeOption) Type {
	return actorhost.MustType(name, factory, options...)
}

// WithSlotMapping 声明独立世界或稳定分服映射；空结果不会回退到默认分区。
func WithSlotMapping(mapping SlotMapping) TypeOption { return actorhost.WithSlotMapping(mapping) }

// WithIdlePassivation 设置类型的空闲卸载策略，零值显式禁用并覆盖运行时及 Behavior 配置。
// 未指定时沿用运行时及 Behavior 策略；业务仍可通过 Context 自定义定时或显式卸载。
func WithIdlePassivation(timeout time.Duration) TypeOption {
	return actorhost.WithIdlePassivation(timeout)
}

// GroupResolver 只返回业务分组 ID，实例和槽位编码由框架处理。
type GroupResolver = actorhost.GroupResolver

// WithPlacement 引用服务声明中的命名放置策略，调用方无需绑定。
func WithPlacement(name string) TypeOption { return actorhost.WithPlacement(name) }

// PlacementSlot 供运维工具构造分组槽位；业务调用使用 Actor Identity。
func PlacementSlot(service, policy, group string) string {
	return actorhost.PlacementSlot(service, policy, group)
}

// WithExplicitAssignment 要求先由部署或控制面登记归属；普通调用及 Ensure/Retry 均不自动选址。
func WithExplicitAssignment() TypeOption { return actorhost.WithExplicitAssignment() }
