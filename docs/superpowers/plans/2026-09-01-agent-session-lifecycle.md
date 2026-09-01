# Agent Session 生命周期与集群控制 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 tx 增加可恢复断连、不可恢复终止、跨 Agent 广播控制，以及无 Redis key 扫描的 MsgQ 失活终止和低频续期。

**Architecture:** Agent 在握手成功后登记本地 Session 控制句柄；专用 gRPC 控制服务只操作本地 Session，广播器使用服务发现快照并行调用全部 Agent。MsgQ 将显式终止、失活终止和积压溢出收敛到精确 binding 解绑路径，Redis 续期按已有 TTL 在本地限频。

**Tech Stack:** Go、gRPC、Protocol Buffers 36.0、etcd 服务发现、Redis Lua 条件更新

**Spec:** `docs/superpowers/specs/2026-09-01-agent-session-lifecycle-design.md`

## Global Constraints

- 不提交 git；阶段完成后仅输出可复制的中文 Angular Commit Message。
- 不使用 Redis `SCAN`、`KEYS` 或任何 key 遍历。
- `OfflineTimeout` 默认保持 `180s`。
- 配置和 Go 字段统一使用 `TerminateDeadSessionInterval`，不保留 `SweepInterval` 兼容别名。
- tx 不出现业务化 Kick、封禁或顶号接口。
- 新增和修改的 GoDoc、维护说明使用中文。

---

### Task 1: MsgQ 终止语义与失活终止命名

**Files:**
- Modify: `modules/msgq/mailbox_test.go`
- Modify: `modules/msgq/mailbox.go`
- Modify: `modules/msgq/client.go`
- Modify: `modules/msgq/grpc.go`
- Modify: `modules/msgq/proto/msgq.proto`
- Regenerate: `modules/msgq/pb/msgq.pb.go`
- Regenerate: `modules/msgq/pb/msgq_grpc.pb.go`
- Modify: `modules/msgq/service/service.go`
- Modify: `modules/msgq/service/service_test.go`

**Interfaces:**
- Produces: `Server.TerminateSession(context.Context, SessionHeader) error`
- Produces: `Server.TerminateDeadSessions(context.Context, time.Time) (int, error)`
- Produces: `SessionClient.TerminateSession(context.Context, SessionHeader) error`
- Produces: `ServerOptions.TerminateDeadSessionInterval time.Duration`

- [x] 写失败测试，证明显式终止删除精确 binding 和本地邮箱但不调用 `UnbindPrimary`。
- [x] 写失败测试，证明 `TerminateDeadSessions` 只终止超过 `OfflineTimeout` 的本地 Session，且不调用 `UnbindPrimary`。
- [x] 运行 `go test ./modules/msgq/...`，确认测试因新 API 尚不存在而失败。
- [x] 将 `Close/CloseSession/Reap/SweepInterval` 分别重命名并收敛到一个精确终止内部函数。
- [x] 将 MsgQ protobuf 的 `Close` RPC 改为 `TerminateSession`，使用仓库脚本和 `protoc 36.0` 只重生成 msgq 包。
- [x] 运行 `go test ./modules/msgq/... ./tests/...`，确认通过。

### Task 2: MsgQ Redis 续期限频

**Files:**
- Modify: `modules/msgq/session_redis.go`
- Modify: `modules/msgq/session_redis_test.go`
- Modify: `modules/msgq/mailbox.go`
- Modify: `modules/msgq/mailbox_test.go`

**Interfaces:**
- Produces: Redis Session store 根据配置 TTL 返回 `TTL / 2` 的内部续期间隔。
- Consumes: Task 1 的 `TerminateSession` 与 `TerminateDeadSessions`。

- [x] 写失败测试，连续两次 `Touch` 只触发第一次到期续期，未到阈值的活动不调用 Store `Refresh`。
- [x] 写失败测试，到达 `TTL / 2` 后精确 binding 与主会话投影各续期一次。
- [x] 运行 `go test ./modules/msgq/...`，确认 Redis 调用计数断言失败。
- [x] 在邮箱 Session 中保存下一次允许续期时间，并由 Redis store 的现有 TTL 推导间隔。
- [x] 保持每次合法活动都更新本地 `lastActivity`；Store 续期失败继续沿用当前失败关闭语义。
- [x] 运行 `go test ./modules/msgq/... ./tests/...`，确认通过。

### Task 3: Agent 本地 Session 注册表与双层能力

**Files:**
- Create: `modules/agent/session_registry.go`
- Create: `modules/agent/session_registry_test.go`
- Modify: `modules/agent/session.go`
- Modify: `modules/agent/types.go`
- Modify: `tests/agent_session_test.go`

**Interfaces:**
- Produces: `Server.DisconnectSession(int64) SessionControlResult`
- Produces: `Server.TerminateSession(context.Context, int64) (SessionControlResult, error)`
- Consumes: `SessionClient.TerminateSession(context.Context, SessionHeader) error`

- [x] 写失败测试，成功握手后 SessionID 出现在本地注册表，连接退出后按 ConnectionID 精确移除。
- [x] 写失败测试，旧连接退出不会移除已经恢复的新 ConnectionID。
- [x] 写失败测试，`DisconnectSession` 关闭物理连接但不调用 MsgQ 终止，相同 SessionID 可以恢复。
- [x] 写失败测试，`TerminateSession` 先调用 MsgQ 精确终止再关闭连接，原 SessionID 无法恢复。
- [x] 运行 `go test ./modules/agent/... ./tests/...`，确认新行为测试失败。
- [x] 实现聚焦的本地注册表，在 MsgQ Init 前登记初始化中连接，并在 Init 成功后、Ready 前原子晋升。
- [x] 实现两项基础控制方法和包含 SessionID、ConnectionID、Handled 的结果。
- [x] 合并同一 SessionID 的并发终止请求，并在 Init 返回不确定结果时按新 ConnectionID 执行精确补偿。
- [x] 运行 `go test ./modules/agent/... ./tests/...`，确认通过。

### Task 4: Agent 控制 gRPC 与广播器

**Files:**
- Create: `modules/agent/proto/control.proto`
- Create: `modules/agent/pb/control.pb.go`
- Create: `modules/agent/pb/control_grpc.pb.go`
- Create: `modules/agent/control.go`
- Create: `modules/agent/control_test.go`
- Create: `modules/agent/control_client.go`
- Create: `modules/agent/control_client_test.go`
- Modify: `cmd/protoc/run.sh`

**Interfaces:**
- Produces: `RegisterControlService(grpc.ServiceRegistrar, *Server) error`
- Produces: `SessionControlBroadcaster`，使用 `discovery.Directory.RunningInstances("agent")`。
- Produces: `DisconnectSession(context.Context, int64, string) BroadcastResult`
- Produces: `TerminateSession(context.Context, int64, string) BroadcastResult`

- [x] 写失败测试，控制 RPC 对本地未持有 Session 幂等返回 `handled=false`。
- [x] 写失败测试，两个 Agent 实例中只有持有目标 Session 的实例返回 `handled=true`。
- [x] 写失败测试，无命中且一个实例超时时广播结果标记为不确定。
- [x] 运行 Agent 控制相关测试，确认因协议和广播器不存在而失败。
- [x] 新增专用 protobuf/gRPC 控制服务；Payload 不进入 Game/Actor 协议。
- [x] 实现并行广播和逐实例结果聚合，不做 Redis 查询。
- [x] 使用 `protoc 36.0` 生成 Agent 控制协议代码。
- [x] 运行 `go test ./modules/agent/... ./tests/...`，确认通过。

### Task 5: Bee 与 Ant Agent 装配

**Files:**
- Modify: `modules/agent/service/service.go`
- Modify: `modules/agent/service/service_test.go`
- Modify: `bee/services/agent/registration.go`
- Modify: `bee/services/agent/registration_test.go`
- Modify: `ant/services/agent/registration.go`
- Modify: `ant/services/agent/registration_test.go`

**Interfaces:**
- Consumes: `RegisterControlService`。
- Produces: 任何独立 Agent 服务在共享 gRPC Host 上自动暴露控制服务。

- [x] 写失败测试，Agent service 缺少 RPC Registrar 时拒绝启动。
- [x] 写失败测试，Bee 与 Ant 注册都把 `CModRPC` Registrar 传入 Agent service。
- [x] 运行相关包测试，确认失败原因是控制服务尚未注册。
- [x] 扩展 Agent service Dependencies，创建运行时时注册控制 gRPC。
- [x] 保持 TCP、WebSocket 与控制 RPC 共用既有生命周期和优雅关闭边界。
- [x] 运行 `go test ./modules/agent/... ./bee/... ./ant/...`，确认通过。

### Task 6: 示例工程配置迁移

**Files:**
- Modify: `../txgame/server/internal/config/model.go`
- Modify: `../txgame/server/internal/config/services.go`
- Modify: `../txgame/server/internal/config/*_test.go`
- Modify: `../txgame/server/assets/{debug,test,release}/app.yml`
- Modify: `../txgame/server/tests/cluster/config/debug/app.yml.tmpl`
- Modify: `../txgameslg/server/internal/config/model.go`
- Modify: `../txgameslg/server/internal/config/services.go`
- Modify: `../txgameslg/server/internal/config/*_test.go`
- Modify: `../txgameslg/server/assets/{debug,test,release}/app.yml`
- Modify: `../txgameslg/server/tests/cluster/config/debug/app.yml.tmpl`

**Interfaces:**
- Consumes: `msgqservice.Options.TerminateDeadSessionInterval`。

- [x] 将配置测试先改为只接受 `terminateDeadSessionInterval`，运行并确认旧模型失败。
- [x] 修改两套项目配置模型与装配，不保留 `sweepInterval` 别名。
- [x] 运行两个项目的 `go test ./server/internal/config/...`，确认通过。

### Task 7: 文档与 tx 全量回归

**Files:**
- Modify: `README.md`
- Modify: `docs/design.md`
- Modify: `docs/implementation_plan.md`
- Modify: `../txgame/README.md`
- Modify: `../txgameslg/README.md`

- [x] 更新双层 Session 生命周期、广播数据流、配置名称、续期限频和禁止 Redis 扫描的说明。
- [x] 运行 `rg -n "SweepInterval|sweepInterval|Reap|CloseSession"`，确认没有遗留旧公开命名；示例工程仅在拒绝旧配置的反向测试中保留旧字段文本。
- [x] 运行 `gofmt` 和 `git diff --check`。
- [x] 运行 `go test ./...`、`go vet ./...`。
- [x] 分别运行 txgame 与 txgameslg 的 Go 全量测试，确认 tx API 迁移完整。
- [x] 汇总阶段结果，并按项目提供中文 Angular Commit Message，不执行提交。

### 后续独立计划

- txgame + txclient：AgentPool 票据、Login 控制调用和双 Agent 无状态验收。
- txgameslg + txclientslg：相同基础能力的有状态独立验收与 Borrow 验收。
