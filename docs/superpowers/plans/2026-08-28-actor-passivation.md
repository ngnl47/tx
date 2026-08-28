# Actor 空闲钝化与常驻缓存淘汰实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development for every behavior change.

**Goal:** 为 Virtual/Fixed Actor 增加 Kind 级空闲钝化，并让 txgameslg 常驻 GD 缓存在持久化安全检查后自动淘汰。

**Architecture:** Core Actor Runtime 仅管理实体 Actor 生命周期；txgameslg 的 resident cache 独立管理 GD 引用，并通过 `cachemgr.Release` 接入 CacheManager 的同锁安全检查。心跳保持普通业务命令，不新增传输协议。

**Spec:** `/Users/a47/workspace/tx/docs/superpowers/specs/2026-08-28-actor-passivation-design.md`

## Task 1：Kind 级空闲钝化

- [x] 先写构造参数校验、消息刷新超时、Fixed Actor 钝化后重建测试并确认失败。
- [x] 新增 `KindOption` 与 `WithIdlePassivation`，使用独立计时器和私有钝化消息实现通用生命周期包装。
- [x] 保持未配置 Kind 的现有行为，并运行 Actor 单测与热点 benchmark。

## Task 2：常驻缓存安全淘汰

- [x] 先写空闲候选淘汰、访问续期、`ErrPendingRoot` 保留和并发访问代次保护测试并确认失败。
- [x] 为 resident cache 增加单扫描任务、访问时间和 generation；释放器在缓存锁外执行。
- [x] 将 `cachemgr.Release` 注入 slggame 常驻角色缓存，并接入 Ant `lifecycle.Tasks`。

## Task 3：模板配置与心跳

- [x] 在 txgame/txgameslg 的具体服务配置中增加 Actor/缓存生命周期参数及严格解析测试。
- [x] txgame 的现有 `System_Ping` 继续经默认 RoleActor 路由；txgameslg 增加携带 `roleId` 的 RoleActor 心跳命令。
- [x] 重新运行 txgen 生成脚本并更新真实生成产物、Handler 示例和命令注册测试。

## Task 4：文档与回归

- [x] 更新 tx README/design、txgame README、txgameslg README 和当前实施计划。
- [x] 执行 tx、txgen、txgame、txgameslg、txclient 受影响测试以及 `git diff --check`。
- [x] 对 Actor 投递和 resident cache 命中/扫描运行 benchmark，确认未使普通消息和缓存命中路径明显退化。
