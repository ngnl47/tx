package actor

import (
	"context"
	"fmt"
	"time"
)

// Controller 提供低频的激活请求和状态查询；控制请求成功不等于初始化完成。
// 查询失败或超时不证明远端操作未发生，调用方不能据此自动重试或改派。
type Controller interface {
	// Probe 只读查询，不分配归属、不加载；Unloaded 不代表业务实体不存在。
	Probe(context.Context, Identity) (ActivationStatus, error)
	// EnsureActivation 发起或共享加载，返回当前状态，不等待 Ready。
	// 已缓存的失败通过状态返回，不自动重新执行工厂。
	EnsureActivation(context.Context, Identity) (ActivationStatus, error)
	// RetryActivation 清除已结束的失败并尝试加载；无失败时等同 EnsureActivation。
	// 正在初始化或关闭时返回当前状态，不替换旧激活，不重投业务消息。
	RetryActivation(context.Context, Identity) (ActivationStatus, error)
}

const activationPollInterval = 20 * time.Millisecond

// WaitReady 只通过 Probe 等待身份就绪，不激活、不自动重试，也不提交业务消息。
// 仅 Initializing 继续等待；Unloaded、Closing、Failed 或查询错误立即返回错误。
// 调用方应设置等待期限；取消只结束等待，不取消已经开始的加载或清理。
// 成功仅表示观察到 Ready，不锁定激活或归属；普通投递仍可能遭遇后续卸载。
// 不得在目标自身的初始化回调中等待自身 Ready，否则会形成自等待。
func WaitReady(ctx context.Context, control Controller, id Identity) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := control.Probe(ctx, id)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		switch state.State {
		case Ready:
			return nil
		case Initializing:
		case Failed:
			if state.Failure != nil {
				return state.Failure
			}
			return fmt.Errorf("actor: %s/%s activation failed", id.Type, id.ID)
		case Closing:
			return fmt.Errorf("actor: %s/%s is closing: %w", id.Type, id.ID, ErrDeactivated)
		default:
			return fmt.Errorf("actor: %s/%s cannot wait for ready in state %q", id.Type, id.ID, state.State)
		}
		timer := time.NewTimer(activationPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
