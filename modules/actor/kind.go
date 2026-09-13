package actor

import (
	"github.com/ngnl47/tx/internal/actorhost"
	"time"
)

// Kind 是实体行为、稳定归属及生命周期策略的声明。
type Kind = actorhost.Kind

// KindOption 配置单个 Kind，业务不需要实现配置协议。
type KindOption = actorhost.KindOption

// NewKind 声明行为工厂；默认使用 1024 个稳定分区，工厂只在实体激活时执行。
func NewKind(name string, factory BehaviorFactory, options ...KindOption) (Kind, error) {
	return actorhost.NewKind(name, factory, options...)
}

// MustKind 用于静态声明，声明错误时 panic；运行时加载错误仍正常返回。
func MustKind(name string, factory BehaviorFactory, options ...KindOption) Kind {
	return actorhost.MustKind(name, factory, options...)
}

// WithSlotMapping 声明独立世界或稳定分服映射；空结果不会回退到默认分区。
func WithSlotMapping(mapping SlotMapping) KindOption { return actorhost.WithSlotMapping(mapping) }

// WithIdlePassivation 设置空闲卸载策略；业务仍可通过 Context 自定义定时或显式卸载。
func WithIdlePassivation(timeout time.Duration) KindOption {
	return actorhost.WithIdlePassivation(timeout)
}
