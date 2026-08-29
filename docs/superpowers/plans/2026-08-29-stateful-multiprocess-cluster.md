# Stateful Multi-process Cluster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 使用真实 etcd、Redis、Mongo 和 txclientslg，闭环验证独立 agent、msgq、slggame、slgworld 组成的有状态固定槽位集群。

**Architecture:** txgameslg 为非 release Agent 注入调试身份和固定槽位路由；RoleActor 通过真实远程 Actor 调用 WorldActor，同时 Agent 保留直接 WorldActor 路径。项目编排器显式创建/提升 Slot，以 CacheManager、Mongo WAL/checkpoint 和 generation fence 的真实状态作为通过证据。

**Tech Stack:** Go 1.26、TypeScript、Node.js、Actor Cluster、gRPC、etcd、Redis 验收专用 DB 13、MongoDB、Fusion、CacheManager、WAL。

**Spec:** `/Users/a47/workspace/tx/docs/superpowers/specs/2026-08-29-multiprocess-cluster-closure-design.md`

**Depends On:** `2026-08-29-multiprocess-cluster-foundation.md`; the stateless suite may be completed first but is not a runtime dependency.

## Global Constraints

- 不提交 Git，由用户统一提交工作区。
- release 环境不注入调试 IdentityResolver 或固定槽位映射。
- 初始 Slot 通过 `txctl create-slot` 创建；交接通过 generation CAS promote，不直接写 etcd JSON。
- WAL 追加成功不等于 checkpoint 完成；关闭必须等待 checkpoint idle。
- 真实依赖不可用时验收命令硬失败，不 skip。
- 每个任务先运行新增测试并确认失败，再实现最小代码使其通过。

---

### Task 1: 让 txgameslg 独立 Agent 可在 debug/test 启动

**Files:**
- Modify: `/Users/a47/workspace/txgameslg/server/cmd/server/main.go`
- Modify: `/Users/a47/workspace/txgameslg/server/cmd/server/main_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/options.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app_test.go`

**Interfaces:**
- Produces: `applicationOptions(environmentName string) []app.Option`。
- Non-release maps `slggame -> slggame-1`, `slgworld -> slgworld-1` through `agent.NewFixedSlotHandlers`。
- Release returns no debug identity/slot injection and continues to require explicit production SPI.

- [ ] 增加 debug/test 独立 agent 能构建、release 不注入、未知 service target 明确失败测试。
- [ ] 运行 `go test ./cmd/server ./internal/app -count=1`，确认当前 main 无 options 使测试失败。
- [ ] 复用现有 debug identity resolver 和 `SlotIDResolverFunc` 注入 handlers，不复制 dispatcher。
- [ ] 运行定向测试、全量测试和 `git diff --check`。

### Task 2: 实现 RoleActor 到 WorldActor 的真实调用

**Files:**
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/model.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/services.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/services_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/config/debug/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/config/test/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/config/release/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/service_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/handler.go`
- Create: `/Users/a47/workspace/txgameslg/server/services/slgworld/handler_test.go`

**Interfaces:**
- Stable config: `services.slggame.worldSlot: slgworld-1`。
- `Role_StartMarch` enters RoleActor, resolves `slgworld-1`, calls WorldActor over Actor/RPC, and decodes `Role_StartMarchResp`。
- `World_MoveUnit` remains Agent direct route with string route key。

- [ ] 增加 config 默认/缺失/非法 worldSlot 测试和三环境配置一致性测试。
- [ ] 增加 slggame handler 测试：成功远程调用、WorldActor 业务拒绝、路由/超时错误映射。
- [ ] 增加 slgworld handler 测试：StartMarch 接收 world/role/target 并返回 accepted。
- [ ] 运行 slggame/slgworld/config 定向测试，确认 handler 和 config 缺失导致失败。
- [ ] 让 slggame service 显式依赖 Slots/Discovery/Actor，并构造可测试的 World caller。
- [ ] 在两个 Actor kind 分别注册生成命令 handler，保持 CommandID 和 BON payload 不变。
- [ ] 运行定向测试、竞态测试和 server 全量测试。

### Task 3: 提供有状态业务主动下行

**Files:**
- Create: `/Users/a47/workspace/txgameslg/server/internal/push/push.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/push/push_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app_test.go`
- Modify: `/Users/a47/workspace/txgameslg/gen/cmd/system.go`
- Modify generated Go: `/Users/a47/workspace/txgameslg/server/gen/cmd/*`
- Modify generated TypeScript: `/Users/a47/workspace/txclientslg/src/gen/cmd.ts`
- Modify generated TypeScript: `/Users/a47/workspace/txclientslg/src/gen/schema.ts`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/handler.go`

**Interfaces:**
- Produces project facade identical in semantics to txgame facade, but maintained independently in txgameslg。
- Adds generated `System_Notify` downstream message containing source service/instance and text。
- slggame or slgworld can call direct Pub; one controlled command can JoinTopics and invoke local BroadcastTopic。

- [ ] 为 facade 增加未安装、安装/释放和并发读取测试。
- [ ] 为 `System_Notify` 增加 txgen golden/manifest 回归，确认只生成到 txgameslg/server 与 txclientslg。
- [ ] 为 slggame/slgworld 增加直接 Pub、本地广播和失败映射测试。
- [ ] 运行失败测试后实现 app setup Sender 安装、业务调用和生成产物。
- [ ] 运行 txgameslg/gen、txgameslg/server、txclientslg 全量测试与构建。

### Task 4: 连接 Slot generation 与 World bucket fence

**Files:**
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/pipeline.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/pipeline_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/cache_runtime.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/cache_runtime_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/service_test.go`

**Interfaces:**
- World service consumes current `slgworld-1` assignment generation before accepting bucket mutations。
- `BucketPipeline.PromoteGeneration(ctx, key, current, next)` performs checkpoint generation CAS before new owner serves traffic。
- WAL append/checkpoint includes the accepted generation; old generation writes return a typed fence error。

- [ ] 增加首次 owner generation 初始化、连续 promotion、旧 generation 迟到 append 拒绝测试。
- [ ] 增加 checkpoint+连续 WAL recovery 只接受当前 generation 测试。
- [ ] 增加新 owner 在 generation promotion 完成前不 ready 测试。
- [ ] 运行 slgworld 定向测试，确认当前 service 未连接 Slot generation 时失败。
- [ ] 在 service startup 读取 assignment，执行幂等初始化或 CAS promotion，并把 generation 注入 cache runtime/pipeline。
- [ ] 保持 checkpoint/WAL 已有 collection 与顺序契约，不引入脚本侧状态伪造。
- [ ] 运行定向测试、integration 快速测试和全量测试。

### Task 5: 扩展 txclientslg 有状态集群驱动

**Files:**
- Create: `/Users/a47/workspace/txclientslg/src/cli/cluster-e2e.ts`
- Create: `/Users/a47/workspace/txclientslg/tests/cluster-e2e.test.ts`
- Modify: `/Users/a47/workspace/txclientslg/package.json`
- Modify: `/Users/a47/workspace/txclientslg/README.md`

**CLI Contract:**
- `npm run cluster:e2e -- --tcp 127.0.0.1:9000 --ws ws://127.0.0.1:9001 --role <id> --world <id> --artifact <path>`。
- Scenarios: Ping/Pong、Role_Rename、World_MoveUnit direct route、Role_StartMarch cross Actor、主动通知、Seq/Ack、断线恢复。
- 输出 JSON Lines，包含 role/world、command、seq/ack、notification 和 session 恢复证据。

- [ ] 使用 fake transport/server 增加全部场景和错误不断线测试。
- [ ] 运行 cluster 定向测试，确认 CLI 缺失导致失败。
- [ ] 实现 CLI，复用 AgentClient 与生成 schema，不手写 CommandID 或 BON 字段表。
- [ ] 运行 `npm test` 与 `npm run build`。

### Task 6: 建立 txgameslg 进程编排与真实状态断言

**Files:**
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/config.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/config_test.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/process.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/process_test.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/state.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/state_test.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/suite.go`
- Create: `/Users/a47/workspace/txgameslg/server/internal/clustere2e/suite_test.go`
- Create: `/Users/a47/workspace/txgameslg/server/tests/cluster/config/debug/app.yml.tmpl`
- Create: `/Users/a47/workspace/txgameslg/server/cmd/cluster-e2e/main.go`
- Create: `/Users/a47/workspace/txgameslg/server/cmd/cluster-e2e/main_test.go`
- Modify: `/Users/a47/workspace/txgameslg/.gitignore`

**Fixed Ports:**
- agent: RPC 9310, Actor 9311, Control 9312, TCP 9000, WebSocket 9001。
- msgq: RPC 9320, Control 9322。
- slggame: RPC 9330, Actor 9331, Control 9332。
- slgworld: RPC 9340, Actor 9341, Control 9342。

**Command Contract:**
- `go run ./cmd/cluster-e2e --artifact-root <path>`。
- Defaults: Redis 验收专用 DB 13, Mongo `txgameslg_cluster_e2e`, etcd `/tx/e2e/txgameslg/{actor,discovery}`。
- Environment overrides: `TXGAMESLG_E2E_ETCD_ENDPOINTS`、`TXGAMESLG_E2E_REDIS_URI`、`TXGAMESLG_E2E_MONGO_URI`。

- [ ] 增加模板、端口、进程生命周期、Redis ownership、精确 Mongo/etcd 清理单元测试。
- [ ] 增加 GDRole/GDWorldBucket fixture、Fusion revision、pending/flush、WAL、checkpoint、generation 查询断言测试。
- [ ] 增加套件阶段顺序、失败保留 artifacts、成功精确清理测试。
- [ ] 实现四进程启动，使用 `txctl create-slot` 显式建立 `slggame-1`/`slgworld-1`。
- [ ] 运行 txclientslg CLI 验证两条 WorldActor 路径、主动下行、Seq/Ack 和恢复。
- [ ] 以长 flushInterval 观察 pending，再等待 flush/Fusion/Mongo 最终状态。
- [ ] 制造 WAL 已追加而 checkpoint 未完成窗口，停止旧 slgworld、promote generation、以新 instance ID 恢复。
- [ ] 断言 checkpoint+WAL 重放、旧 generation fence、覆盖 WAL 清理。
- [ ] 使用 SIGTERM 验证 WaitGroupContext、CacheManager final flush、checkpoint WaitIdle 和预期退出码。
- [ ] 异常终止 MsgQ，只断言明确旧会话结果及新 Session 恢复，不声称未确认下行无损。

### Task 7: 同步有状态文档并运行真实验收

**Files:**
- Modify: `/Users/a47/workspace/txgameslg/README.md`
- Modify: `/Users/a47/workspace/txgameslg/AGENTS.md`
- Modify: `/Users/a47/workspace/txclientslg/README.md`
- Verify only: tx、txgen、txgameslg、txclientslg。

- [ ] 文档记录稳定配置 `services.slggame.worldSlot`、真实依赖、隔离、命令、artifacts、固定槽位和恢复边界。
- [ ] 运行 tx `go test ./... -count=1`。
- [ ] 运行 txgen `go test ./... -count=1`。
- [ ] 运行 txgameslg/gen、txgameslg/server 的 `go test ./... -count=1`。
- [ ] 运行 txclientslg `npm test` 与 `npm run build`。
- [ ] 在真实 etcd/Redis/Mongo 上运行 `go run ./cmd/cluster-e2e`，保存终端摘要和 artifact 路径。
- [ ] 检查真实 Slot assignment/generation、Redis/Fusion、Mongo role/world/WAL/checkpoint；确认没有 skip。
- [ ] 对四工程运行 `git diff --check` 与 `git status --short`。
- [ ] 记录 tx、txgen、txgameslg、txclientslg 各自建议的中文 Angular Commit Message；每条使用独立 `text` 代码块，不执行提交。
