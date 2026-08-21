package discovery

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrInvalidSlotAssignment  = errors.New("discovery: invalid slot assignment")
	ErrInvalidSlotPromotion   = errors.New("discovery: invalid slot promotion")
	ErrSlotNotFound           = errors.New("discovery: slot assignment not found")
	ErrSlotConflict           = errors.New("discovery: slot assignment conflict")
	ErrSlotStoreNil           = errors.New("discovery: slot store is nil")
	ErrSlotDirectoryNil       = errors.New("discovery: slot directory is nil")
	ErrSlotRuntimeUnavailable = errors.New("discovery: slot runtime is unavailable")
)

// SlotAssignment 将稳定逻辑槽位绑定到一个特定运行实例 generation。SlotID 是业务
// 归属键，例如 game-slot-1；RuntimeInstanceID 必须是每次进程启动唯一的实例 ID，
// 不能复用部署名称或逻辑槽位名称。
type SlotAssignment struct {
	SlotID            string `json:"slotId"`
	RuntimeInstanceID string `json:"runtimeInstanceId"`
	Generation        uint64 `json:"generation"`
}

func (a SlotAssignment) Validate() error {
	if strings.TrimSpace(a.SlotID) == "" || strings.TrimSpace(a.RuntimeInstanceID) == "" ||
		a.SlotID != strings.TrimSpace(a.SlotID) || a.RuntimeInstanceID != strings.TrimSpace(a.RuntimeInstanceID) ||
		strings.Contains(a.SlotID, "/") || strings.Contains(a.RuntimeInstanceID, "/") || a.Generation == 0 {
		return ErrInvalidSlotAssignment
	}
	return nil
}

// ValidateSlotPromotion 只验证与存储无关的交接规则。实际 Compare-And-Swap 由持久化
// SlotStore 保证，调用方仍须先确认 next runtime 已完成启动和依赖预检。
func ValidateSlotPromotion(current, next SlotAssignment) error {
	if current.Validate() != nil || next.Validate() != nil || current.SlotID != next.SlotID ||
		next.RuntimeInstanceID == current.RuntimeInstanceID || next.Generation != current.Generation+1 {
		return ErrInvalidSlotPromotion
	}
	return nil
}

// SlotStore 保存稳定逻辑槽位的当前归属。它不负责启动实例、不观察 Actor，也不替
// 业务决定 SlotID；Promote 必须使用当前完整 assignment 执行 Compare-And-Swap。
type SlotStore interface {
	ResolveSlot(ctx context.Context, slotID string) (SlotAssignment, error)
	CreateSlot(ctx context.Context, assignment SlotAssignment) error
	PromoteSlot(ctx context.Context, current, next SlotAssignment) error
}

// SlotTarget 是一次安全解析后的固定归属目标。业务使用 Assignment 中的 generation
// 防止缓存旧路由；实际 Actor PID 或 gRPC client 由业务按 Instance.Endpoint 创建。
type SlotTarget struct {
	Assignment SlotAssignment
	Instance   Instance
}

// SlotRouter 将持久 assignment 和当前服务成员视图组合为可调用目标。它只接受
// running 实例，draining/stopping/丢失实例一律返回明确错误，避免静默改派。
type SlotRouter struct {
	store     SlotStore
	directory *ServiceDirectory
}

func NewSlotRouter(store SlotStore, directory *ServiceDirectory) (*SlotRouter, error) {
	if store == nil {
		return nil, ErrSlotStoreNil
	}
	if directory == nil {
		return nil, ErrSlotDirectoryNil
	}
	return &SlotRouter{store: store, directory: directory}, nil
}

func (r *SlotRouter) ResolveSlot(ctx context.Context, slotID string) (SlotTarget, error) {
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
	instance, ok := r.directory.LookupInstance(assignment.RuntimeInstanceID)
	if !ok || instance.Status != StatusRunning {
		return SlotTarget{}, ErrSlotRuntimeUnavailable
	}
	return SlotTarget{Assignment: assignment, Instance: instance}, nil
}

// PromoteSlot 在写入持久 assignment 前确认目标实例已进入 running。它不主动将旧实例
// 切为 draining；发布控制面必须先关闭旧槽位的新命令准入并完成最终 Flush。
func (r *SlotRouter) PromoteSlot(ctx context.Context, current, next SlotAssignment) error {
	if r == nil || r.store == nil {
		return ErrSlotStoreNil
	}
	if r.directory == nil {
		return ErrSlotDirectoryNil
	}
	if err := ValidateSlotPromotion(current, next); err != nil {
		return err
	}
	instance, ok := r.directory.LookupInstance(next.RuntimeInstanceID)
	if !ok || instance.Status != StatusRunning {
		return ErrSlotRuntimeUnavailable
	}
	return r.store.PromoteSlot(ctx, current, next)
}
