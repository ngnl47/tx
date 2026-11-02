package discovery

import (
	"context"
	"errors"
	"slices"
	"strings"
)

var (
	ErrInvalidSlotAssignment    = errors.New("discovery: invalid slot assignment")
	ErrInvalidSlotPromotion     = errors.New("discovery: invalid slot promotion")
	ErrSlotReleased             = errors.New("discovery: slot is released")
	ErrSlotNotFound             = errors.New("discovery: slot assignment not found")
	ErrSlotConflict             = errors.New("discovery: slot assignment conflict")
	ErrSlotStoreNil             = errors.New("discovery: slot store is nil")
	ErrSlotDirectoryNil         = errors.New("discovery: slot directory is nil")
	ErrSlotDirectoryUnavailable = errors.New("discovery: slot directory is unavailable")
	ErrSlotRuntimeUnavailable   = errors.New("discovery: slot runtime is unavailable")
	ErrSlotRouteStale           = errors.New("discovery: cached slot route is stale")
)

// SlotAssignment 将稳定逻辑槽位绑定到一个特定运行实例 generation。SlotID 是业务
// 归属键，例如 game-slot-1；RuntimeInstanceID 必须是每次进程启动唯一的实例 ID，
// 不能复用或重复编码部署名称与逻辑槽位名称。空 RuntimeInstanceID 表示正常释放，
// 仍保留非零 Generation；普通解析返回 ErrSlotReleased，不把它当可调用目标。
type SlotAssignment struct {
	SlotID            string `json:"slotId"`
	RuntimeInstanceID string `json:"runtimeInstanceId"`
	Generation        uint64 `json:"generation"`
}

func (a SlotAssignment) Validate() error {
	if strings.TrimSpace(a.SlotID) == "" ||
		a.SlotID != strings.TrimSpace(a.SlotID) || a.RuntimeInstanceID != strings.TrimSpace(a.RuntimeInstanceID) ||
		strings.Contains(a.SlotID, "/") || strings.Contains(a.RuntimeInstanceID, "/") || a.Generation == 0 {
		return ErrInvalidSlotAssignment
	}
	return nil
}

// Released 表示正常收尾后无 owner 的持久记录；Generation 仍保留。
func (a SlotAssignment) Released() bool { return a.RuntimeInstanceID == "" }

// ValidateSlotPromotion 只验证与存储无关的交接规则。实际 Compare-And-Swap 由持久化
// SlotStore 保证，调用方仍须先确认 next runtime 已完成启动和依赖预检。
func ValidateSlotPromotion(current, next SlotAssignment) error {
	if current.Validate() != nil || next.Validate() != nil || current.SlotID != next.SlotID || next.Released() || current.Generation == ^uint64(0) ||
		next.RuntimeInstanceID == current.RuntimeInstanceID || next.Generation != current.Generation+1 {
		return ErrInvalidSlotPromotion
	}
	return nil
}

// SlotReader 只读取槽位归属，供业务路由和加载前校验使用。
type SlotReader interface {
	ResolveSlot(context.Context, string) (SlotAssignment, error)
}

// VersionedSlotReader 返回线性读所在的权威快照版本，供主动查询与 Watch 合并。
// 版本必须与 ReplaceSnapshot 使用同一序列；不是某个键最后修改时的版本。
type VersionedSlotReader interface {
	ResolveSlotAtRevision(context.Context, string) (SlotAssignment, int64, error)
}

// SlotStore 保存稳定逻辑槽位的当前归属。写入必须原子复核目标成员及容量，
// Promote 必须比较当前完整 assignment；它不启动实例、不观察 Actor、不决定业务 SlotID。
type SlotStore interface {
	SlotReader
	CreateSlot(ctx context.Context, assignment SlotAssignment) error
	PromoteSlot(ctx context.Context, current, next SlotAssignment) error
}

// SlotTarget 是一次安全解析后的固定归属目标。业务使用 Assignment 中的 generation
// 防止缓存旧路由；gRPC 使用 Instance.Endpoint，Core NATS 使用归属实例身份定向投递。
type SlotTarget struct {
	Assignment SlotAssignment
	Instance   RouteInstance
}

// SlotResolver 将 Watch assignment 和当前服务成员视图组合为可调用目标。它只接受
// running 实例，draining/stopping/丢失实例一律返回明确错误，避免静默改派。
type SlotResolver struct {
	refresh        *slotRefresh
	store          SlotReader
	directory      Directory
	routeDirectory RouteDirectory
}

// NewSlotResolver 创建只读路由解析器，不要求调用方提供槽位写入能力。
func NewSlotResolver(store SlotReader, directory Directory) (*SlotResolver, error) {
	if store == nil {
		return nil, ErrSlotStoreNil
	}
	if directory == nil {
		return nil, ErrSlotDirectoryNil
	}
	router := &SlotResolver{store: store, directory: directory}
	if routeDirectory, ok := directory.(RouteDirectory); ok {
		router.routeDirectory = routeDirectory
	}
	return router, nil
}

func (r *SlotResolver) ResolveSlot(ctx context.Context, slotID string) (SlotTarget, error) {
	return r.resolveSlot(ctx, "", slotID)
}

// ResolveServiceSlot 同时校验目标实例承载指定服务；不因服务不匹配改派归属。
func (r *SlotResolver) ResolveServiceSlot(ctx context.Context, service, slotID string) (SlotTarget, error) {
	if service == "" {
		return SlotTarget{}, ErrSlotRuntimeUnavailable
	}
	target, err := r.resolveSlot(ctx, service, slotID)
	if err == nil || r.refresh == nil || (!errors.Is(err, ErrSlotNotFound) && !errors.Is(err, ErrSlotRouteStale) && !errors.Is(err, ErrSlotRuntimeUnavailable)) {
		return target, err
	}
	if _, bounded := ctx.Deadline(); !bounded && errors.Is(err, ErrSlotRuntimeUnavailable) {
		return target, err
	}
	return r.refresh.resolve(ctx, service, slotID)
}

func (r *SlotResolver) resolveSlot(ctx context.Context, service, slotID string) (SlotTarget, error) {
	if r == nil || r.store == nil {
		return SlotTarget{}, ErrSlotStoreNil
	}
	if r.directory == nil {
		return SlotTarget{}, ErrSlotDirectoryNil
	}
	assignment, err := r.store.ResolveSlot(ctx, slotID)
	if err != nil {
		return SlotTarget{}, err
	}
	if assignment.Released() {
		return SlotTarget{Assignment: assignment}, ErrSlotReleased
	}
	instance, ok := r.lookupRouteInstance(service, assignment.RuntimeInstanceID)
	if !ok || instance.Status != StatusRunning {
		return SlotTarget{Assignment: assignment}, ErrSlotRuntimeUnavailable
	}
	return SlotTarget{Assignment: assignment, Instance: instance}, nil
}

func (r *SlotResolver) lookupRouteInstance(service, instanceID string) (RouteInstance, bool) {
	if service != "" {
		if directory, ok := r.directory.(interface {
			LookupServiceRouteInstance(string, string) (RouteInstance, bool)
		}); ok {
			return directory.LookupServiceRouteInstance(service, instanceID)
		}
	} else if r.routeDirectory != nil {
		return r.routeDirectory.LookupRouteInstance(instanceID)
	}
	instance, ok := r.directory.LookupInstance(instanceID)
	if !ok || (service != "" && !slices.Contains(instance.Services, service)) {
		return RouteInstance{}, false
	}
	return routeInstance(instance), true
}

// SlotRouter 是高级槽位路由与管理 API，保留目标就绪检查和完整 SlotStore 写入能力。
// 普通业务读取使用 SlotResolver；项目控制面或高级组件可显式注入 SlotStore 使用本类型。
type SlotRouter struct {
	resolver SlotResolver
	store    SlotStore
}

func NewSlotRouter(store SlotStore, directory Directory) (*SlotRouter, error) {
	resolver, err := NewSlotResolver(store, directory)
	if err != nil {
		return nil, err
	}
	return &SlotRouter{resolver: *resolver, store: store}, nil
}

// ResolveSlot 复用只读解析逻辑，不执行创建或晋升。
func (r *SlotRouter) ResolveSlot(ctx context.Context, slotID string) (SlotTarget, error) {
	if r == nil {
		return SlotTarget{}, ErrSlotStoreNil
	}
	return r.resolver.ResolveSlot(ctx, slotID)
}

// CreateSlot 在写入初始 assignment 前确认目标实例已经进入 running。
func (r *SlotRouter) CreateSlot(ctx context.Context, assignment SlotAssignment) error {
	if r == nil || r.store == nil {
		return ErrSlotStoreNil
	}
	if r.resolver.directory == nil {
		return ErrSlotDirectoryNil
	}
	if err := assignment.Validate(); err != nil || assignment.Generation != 1 || assignment.Released() {
		return ErrInvalidSlotAssignment
	}
	instance, ok := r.resolver.directory.LookupInstance(assignment.RuntimeInstanceID)
	if !ok || instance.Status != StatusRunning {
		return ErrSlotRuntimeUnavailable
	}
	return r.store.CreateSlot(ctx, assignment)
}

// PromoteSlot 在写入持久 assignment 前确认目标实例已进入 running。它不主动将旧实例
// 切为 draining；发布控制面必须先关闭旧槽位的新命令准入并完成最终 Flush。
func (r *SlotRouter) PromoteSlot(ctx context.Context, current, next SlotAssignment) error {
	if r == nil || r.store == nil {
		return ErrSlotStoreNil
	}
	if r.resolver.directory == nil {
		return ErrSlotDirectoryNil
	}
	if err := ValidateSlotPromotion(current, next); err != nil {
		return err
	}
	instance, ok := r.resolver.directory.LookupInstance(next.RuntimeInstanceID)
	if !ok || instance.Status != StatusRunning {
		return ErrSlotRuntimeUnavailable
	}
	return r.store.PromoteSlot(ctx, current, next)
}
