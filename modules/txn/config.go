package txn

import (
	"sync/atomic"
	"time"
)

// Config 控制可选单机事务模块的运行时行为。
//
// 项目应在服务启动阶段完成配置。零值关闭调试检查，并使用异步执行默认额度与期限。
type Config struct {
	// Debug 是仅建议在开发、测试或灰度环境启用的一致性检查。
	Debug DebugConfig
	// Async 配置本 Runtime 的异步执行额度与独立操作期限。
	Async AsyncConfig
}

// AsyncConfig 只限制 Submit，不改变同步 Call；零值使用默认值。
type AsyncConfig struct {
	MaxConcurrent int
	// MaxPending 包含执行和排队的根任务；等于 MaxConcurrent 时不排队。
	MaxPending int
	Timeout    time.Duration
}

// DebugConfig 是仅建议在开发、测试或灰度环境开启的事务校验项。
type DebugConfig struct {
	// CheckCleanLockTargets 在 Store 登记时检查是否有事务外遗留变更。
	// 它用于发现事务外 Store 修改混入本次提交边界的问题。
	CheckCleanLockTargets bool
}

var runtimeConfig atomic.Pointer[Config]

// Configure 发布进程级诊断配置；Async 由框架另外传给对应 Runtime.ConfigureAsync，
// 不用进程全局配置改变已启动 Runtime 的额度。
func Configure(config Config) {
	runtimeConfig.Store(&config)
}

// CurrentConfig 返回当前事务模块配置的副本。
func CurrentConfig() Config {
	config := runtimeConfig.Load()
	if config == nil {
		return Config{}
	}
	return *config
}

// Validate 在启动阶段检查异步额度关系，不修改配置。
func (config AsyncConfig) Validate() error {
	_, err := normalizeAsyncConfig(config)
	return err
}
