package txn

import "sync/atomic"

// Config 控制可选单机事务模块的运行时行为。
//
// 项目应在服务启动阶段完成配置。未配置时使用零值，即不启用调试校验。
type Config struct {
	Debug DebugConfig
}

// DebugConfig 是仅建议在开发、测试或灰度环境开启的事务校验项。
type DebugConfig struct {
	// CheckCleanLockTargets 要求事务入口的每个可检查 Store 均没有待落地变更。
	// 它用于发现事务外 Store 修改混入本次提交边界的问题。
	CheckCleanLockTargets bool
}

var runtimeConfig atomic.Pointer[Config]

// Configure 设置事务模块的运行时配置。
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
