# Stateless Multi-process Cluster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 使用真实 etcd、Redis、Mongo 和 txclient，闭环验证独立 agent、msgq、game-a、game-b 组成的无状态集群。

**Architecture:** txgame 项目提供可重复的集群配置、业务侧 MsgQ 推送入口和 Go 进程编排器；txclient 提供 TCP/WebSocket 协议驱动。编排器构建一次 server 二进制，使用固定端口启动四个独立进程，以控制面/Discovery/存储查询作为 ready 屏障，并保留失败 artifacts。

**Tech Stack:** Go 1.26、TypeScript、Node.js、TCP、WebSocket、gRPC、etcd、Redis 验收专用 DB 12、MongoDB。

**Spec:** `/Users/a47/workspace/tx/docs/superpowers/specs/2026-08-29-multiprocess-cluster-closure-design.md`

**Depends On:** `2026-08-29-multiprocess-cluster-foundation.md`

## Global Constraints

- 不提交 Git，由用户统一提交工作区。
- 真实依赖不可用时验收命令硬失败，不 skip。
- 只清理确认归属 `txgame-cluster-e2e` 的 Redis 验收专用 DB 12、Mongo database 和 etcd prefix。
- Ready 判断不使用固定 sleep；所有轮询有超时和失败快照。
- `game-a`、`game-b` 的主动下行直接调用 MsgQ Sender，不绕回 Agent Dispatcher。
- 每个任务先运行新增测试并确认失败，再实现最小代码使其通过。

---

### Task 1: 分离 txgame debug Redis database

**Files:**
- Modify: `/Users/a47/workspace/txgame/server/config/debug/redis-alias.yml`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/loader_test.go`
- Modify: `/Users/a47/workspace/txgame/README.md`

- [x] 增加配置测试，断言 debug 所有 Redis alias 都解析到 DB 14，且不会混用其他 logical database。
- [x] 运行 `go test ./internal/config -count=1`，确认当前 DB 15 使测试失败。
- [x] 把 debug 普通存储、Fusion、delay、MsgQ 路由 URI 全部改为 DB 14。
- [x] 文档说明 DB 14 是示例默认值，可由集群模板覆盖，功能 key 不增加项目名前缀。
- [x] 运行定向测试和 `git diff --check`。

### Task 2: 提供 txgame 业务主动下行组件

**Files:**
- Create: `/Users/a47/workspace/txgame/server/internal/push/push.go`
- Create: `/Users/a47/workspace/txgame/server/internal/push/push_test.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/app/app_test.go`
- Modify: `/Users/a47/workspace/txgame/server/services/game/service.go`
- Modify: `/Users/a47/workspace/txgame/server/services/game/service_test.go`
- Modify: `/Users/a47/workspace/txgame/server/services/game/handler.go`
- Modify: `/Users/a47/workspace/txgame/server/services/game/handler_test.go`

**Interfaces:**
- Produces project facade: `push.Install(*msgq.Sender)`、`push.Current() (*msgq.Sender, bool)`、`push.ResetForTest()`。
- Produces business commands that trigger direct `Sender.Pub` and local `Sender.BroadcastTopic` using generated txgame messages; response contains handling instance ID and broadcast counters.
- Consumes CMods: Discovery and Redis-backed MsgQ session routes.

- [ ] 为 facade 增加未安装、单次安装、测试重置和并发读取测试。
- [ ] 为 game handler 增加单播、JoinTopics、本地广播、MsgQ 失败转 System_Error、返回当前 instance ID 测试。
- [ ] 运行 game 定向测试，确认缺少 facade/handler 时失败。
- [ ] 在 app setup 中只为选择 `game` 的进程构造并安装 Sender；释放阶段关闭连接并清空 facade。
- [ ] 让 game service 显式声明所需 Discovery/Redis CMods，注册验收 handler。
- [ ] 使用 txgame 已有通知协议；若现有命令无法表达 trigger/result，只在 txgame/gen 输入中增加最小 E2E 命令并重新生成 txgame/server 与 txclient 产物。
- [ ] 运行 txgame/gen 生成测试、server 定向测试和全量测试。

### Task 3: 扩展 txclient 无状态集群驱动

**Files:**
- Create: `/Users/a47/workspace/txclient/src/cli/cluster-e2e.ts`
- Create: `/Users/a47/workspace/txclient/tests/cluster-e2e.test.ts`
- Modify: `/Users/a47/workspace/txclient/package.json`
- Modify: `/Users/a47/workspace/txclient/README.md`
- Modify generated files only when Task 2 changes txgame protocol: `/Users/a47/workspace/txclient/src/gen/cmd.ts`
- Modify generated files only when Task 2 changes txgame protocol: `/Users/a47/workspace/txclient/src/gen/schema.ts`

**CLI Contract:**
- `npm run cluster:e2e -- --tcp 127.0.0.1:9000 --ws ws://127.0.0.1:9001 --artifact <path>`。
- 输出 JSON Lines 事件：transport、sessionID、UpMsg seq、DownMsg seq/ack、command、instanceID、notification。
- 同时保持至少两个在线会话以验证 JoinTopics 和本地广播。

- [ ] 使用 fake transport/server 为 CLI 场景增加 TCP/WS Ping、业务错误不断线、未确认 UpMsg 原 Seq 重放、主动 Pub、两个会话广播测试。
- [ ] 运行 `npm test -- --test-name-pattern=cluster`，确认 CLI 缺失导致失败。
- [ ] 实现 cluster CLI，复用 AgentClient，不复制 Frame/Session/Seq/Ack 算法。
- [ ] 确认客户端只断言协议观察值，不断言 Redis/Mongo 内部状态。
- [ ] 运行 `npm test` 与 `npm run build`。

### Task 4: 建立 txgame 可复用进程编排内核

**Files:**
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/config.go`
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/config_test.go`
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/process.go`
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/process_test.go`
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/dependencies.go`
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/dependencies_test.go`
- Create: `/Users/a47/workspace/txgame/server/tests/cluster/config/debug/app.yml.tmpl`
- Modify: `/Users/a47/workspace/txgame/.gitignore`

**Fixed Ports:**
- agent: RPC 19210, Actor 19211, Control 19212, TCP 19000, WebSocket 19001。
- msgq: RPC 19220, Control 19222。
- game-a: RPC 19230, Actor 19231, Control 19232。
- game-b: RPC 19240, Actor 19241, Control 19242。

- [ ] 增加模板完整渲染、残留 token 失败、端口冲突、超时、子进程退出、SIGTERM 顺序测试。
- [ ] 增加 Redis ownership marker 安全检查：DB 空或 marker 匹配才允许 FLUSHDB，非空且无匹配 marker 必须失败。
- [ ] 增加 Mongo 精确 database、etcd 精确 prefix 的 prepare/cleanup 测试，禁止根 prefix 和通配目标。
- [ ] 运行 `go test ./internal/clustere2e -count=1`，确认包缺失导致失败。
- [ ] 实现 artifact 目录、配置渲染、一次构建、进程日志/PID/退出码记录和超时轮询。
- [ ] 实现真实 Redis/Mongo/etcd preflight 与精确清理；普通单元测试使用 fake 接口，不连接真实依赖。
- [ ] 忽略仅由编排器生成的 artifacts，不忽略 tracked config template。
- [ ] 运行定向测试、竞态测试和 `git diff --check`。

### Task 5: 实现无状态真实集群命令与场景

**Files:**
- Create: `/Users/a47/workspace/txgame/server/cmd/cluster-e2e/main.go`
- Create: `/Users/a47/workspace/txgame/server/cmd/cluster-e2e/main_test.go`
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/suite.go`
- Create: `/Users/a47/workspace/txgame/server/internal/clustere2e/suite_test.go`
- Modify: `/Users/a47/workspace/txgame/README.md`
- Modify: `/Users/a47/workspace/txgame/AGENTS.md`

**Command Contract:**
- `go run ./cmd/cluster-e2e --artifact-root <path>`。
- Environment overrides: `TXGAME_E2E_ETCD_ENDPOINTS`、`TXGAME_E2E_REDIS_URI`、`TXGAME_E2E_MONGO_URI`。
- Defaults: Redis 验收专用 DB 12, Mongo `txgame_cluster_e2e`, etcd `/tx/e2e/txgame/{actor,discovery}`。

- [ ] 使用 fake dependencies/process/client runner 增加 Preflight→Prepare→Start→Exercise→Restart/Drain→Assert→Cleanup 顺序测试。
- [ ] 增加任一阶段失败保留 artifacts、成功精确清理、失败摘要包含重跑命令测试。
- [ ] 实现四进程启动和基于 control status、Discovery membership、端口握手的 ready 判断。
- [ ] 调用 txclient cluster CLI，验证 TCP/WS、Seq/Ack、断线恢复、业务错误、直接 Pub 和 Topic 广播。
- [ ] 通过实例 ID 证据验证两个 game；drain game-a 后新请求只落到 game-b；以新 instance ID 重启 game-a。
- [ ] drain 所有进程并断言预期退出码、Discovery 注销和无遗留 PID。
- [ ] 更新 README/AGENTS：启动前提、覆盖变量、隔离、artifacts、失败清理和明确非保证项。

### Task 6: 运行无状态真实验收

**Files:**
- Verify only: tx、txgen、txgame、txclient。

- [ ] 运行 tx `go test ./... -count=1`。
- [ ] 运行 txgen `go test ./... -count=1`。
- [ ] 运行 txgame/gen、txgame/server 的 `go test ./... -count=1`。
- [ ] 运行 txclient `npm test` 与 `npm run build`。
- [ ] 在真实 etcd/Redis/Mongo 上运行 `go run ./cmd/cluster-e2e`，保存终端摘要和 artifact 路径。
- [ ] 检查真实 Redis owner/session、Mongo 业务结果、etcd membership；确认没有跳过项。
- [ ] 对四工程运行 `git diff --check` 与 `git status --short`。
- [ ] 记录 tx、txgen、txgame、txclient 各自建议的中文 Angular Commit Message；每条使用独立 `text` 代码块，不执行提交。
