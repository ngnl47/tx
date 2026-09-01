# 可信会话身份贯穿实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Login 校验后的账号、区服、角色、会话、Borrow 与创建角色许可贯穿 Agent、Actor Delivery 和两个示例工程的 HandlerContext。

**Architecture:** 公共 `protocol` 保存稳定身份值；项目身份解析器只返回不含 ConnectionID 的 SessionPrincipal；Agent 固化物理连接身份并强制覆盖 Actor Delivery；txgen 为 HandlerContext 生成只读访问入口。

**Tech Stack:** Go 1.26、Protocol Buffers 36.0、ProtoActor、txgen 模板与双示例生成闭环。

**Spec:** `docs/superpowers/specs/2026-09-01-trusted-session-identity-design.md`

## Global Constraints

- 不提交 Git，由维护者统一审核提交。
- 不修改 `/Users/a47/workspace/txgameslgworld`。
- 新增或修改的注释、GoDoc、模板注释和维护说明使用中文。
- 不保留旧 IdentityResolver 返回 Identity 的兼容层。
- 本阶段不修改 MsgQ 主键、业务响应路径、上行 Ack 语义或客户端协议。
- 两个示例工程必须由各自 `gen/run.sh` 重新生成，禁止手改 `server/gen/cmd` 与客户端生成产物。

---

### Task 1: 定义公共可信身份值

**Files:**
- Create: `/Users/a47/workspace/tx/protocol/session_identity.go`
- Create: `/Users/a47/workspace/tx/protocol/session_identity_test.go`

- [x] **Step 1: Write failing validation tests**

覆盖普通身份、Borrow 只读身份、缺失正 ID、未知模式、普通模式带 BorrowAccountID、Borrow 模式缺少目标账号和 Borrow 模式带创建角色许可。

- [x] **Step 2: Run tests and verify RED**

Run: `go test ./protocol -run 'TestSession' -count=1`

- [x] **Step 3: Implement minimal values and validation**

增加 `AccessMode`、`SessionPrincipal`、`SessionIdentity` 和 `Validate`。

- [x] **Step 4: Run tests and verify GREEN**

Run: `go test ./protocol -run 'TestSession' -count=1`

### Task 2: 贯穿 Agent 与 Actor Delivery

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/agent/types.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/session.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/actor_dispatcher.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/actor_dispatcher_test.go`
- Modify: `/Users/a47/workspace/tx/tests/agent_session_test.go`
- Modify: `/Users/a47/workspace/tx/modules/actor/proto/actor.proto`
- Regenerate: `/Users/a47/workspace/tx/modules/actor/pb/actor.pb.go`
- Modify: `/Users/a47/workspace/tx/modules/actor/delivery.go`
- Modify: `/Users/a47/workspace/tx/tests/actor_runtime_test.go`

**Interfaces:**

- `agent.IdentityResolver.ResolveIdentity` 返回 `protocol.SessionPrincipal`；`agent.Identity` 是 `protocol.SessionIdentity` 的别名。
- Agent 只能用项目 Principal 与本次服务端 `ConnectionID` 构造 Identity，随后调用 `Identity.Validate()`。
- `actor.Delivery` 增加 `Identity *protocol.SessionIdentity` 和 `UpSeq int64`；二者必须同时存在或同时省略。
- Agent 上行必须携带完整 Identity 与正 UpSeq；Actor 内部调用可以同时省略。
- `NewActorDispatcher` 在调用 Messenger 前无条件以 `DispatchRequest.Identity` 与 `DispatchRequest.Seq` 覆盖 resolver 返回的 Delivery 字段。
- protobuf `AccessMode` 使用 `ACCESS_MODE_NORMAL = 0` 与 `ACCESS_MODE_BORROW_READ_ONLY = 1`，`SessionIdentity` 的全部 ID 使用 `int64`。

- [x] **Step 1: Write failing Agent and Actor contract tests**

断言 Agent 固化 ConnectionID、DispatchRequest 取得完整 Identity、ActorDispatcher 覆盖 resolver 伪造值，以及 protobuf 往返完整 Identity/UpSeq。

- [x] **Step 2: Run tests and verify RED**

Run: `go test ./modules/agent ./modules/actor ./tests -run 'Identity|Delivery|ActorDispatcher|AgentSession' -count=1`

- [x] **Step 3: Implement and regenerate Actor protobuf**

Run: `test "$(protoc --version)" = "libprotoc 36.0"`

Run: `PROTOC="$(command -v protoc)" ./cmd/protoc/run.sh actor`

- [x] **Step 4: Run focused tests and verify GREEN**

Run: `go test ./modules/agent ./modules/actor ./tests -run 'Identity|Delivery|ActorDispatcher|AgentSession' -count=1`

### Task 3: 扩展 txgen HandlerContext

**Files:**
- Modify: `/Users/a47/workspace/txgen/gen/templates/cmd_handler_registry.go.tmpl`
- Modify: `/Users/a47/workspace/txgen/tests/cmd_handler_registry_behavior_test.go`
- Modify: `/Users/a47/workspace/txgen/tests/generate_cmd_test.go`
- Regenerate: `/Users/a47/workspace/txgen/tests/output/cmd/cmd_handler_registry.go`

**Interfaces:**

- 生成的 `HandlerContext` 增加 `Identity() txprotocol.SessionIdentity` 与 `UpSeq() int64`。
- 不逐项重复生成 AccountID、RoleID 等 getter；业务从同一不可变 Identity 值读取。
- 生成测试的假上下文必须返回字面值 Identity 与 UpSeq，并由真实 `HandlerBinding.Invoke` 传给 Handler 验证。

- [x] **Step 1: Write failing generated behavior tests**

生成上下文必须提供 `Identity() txprotocol.SessionIdentity` 与 `UpSeq() int64`。

- [x] **Step 2: Run tests and verify RED**

Run: `go test ./tests -run 'HandlerContext|CmdHandler' -count=1`

- [x] **Step 3: Update template and regenerate fixture**

Run: `go test ./tests -run 'HandlerContext|CmdHandler' -count=1`

测试成功后的 `TestMain` 使用仓库生成器重新生成 `tests/output`，不手改生成文件。

- [x] **Step 4: Run txgen full tests**

Run: `go test ./... -count=1`

### Task 4: 同步两个示例工程

**Files:**
- Modify: `/Users/a47/workspace/txgame/server/services/agent/debug_identity.go`
- Modify: `/Users/a47/workspace/txgame/server/services/agent/actor_dispatcher.go`
- Modify: `/Users/a47/workspace/txgame/server/services/internal/actorcmd/command_actor.go`
- Modify: corresponding tests
- Modify: `/Users/a47/workspace/txgameslg/server/services/agent/debug_identity.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/agent/actor_dispatcher.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/actorcmd/command_actor.go`
- Modify: corresponding tests
- Regenerate: both projects' generated cmd outputs

**Interfaces:**

- debug/test 的 `<roleID>:<sessionID>` 映射为 `AccountID=RoleID`、`ServerID=1`、相同 RoleID/SessionID、`AccessModeNormal`，返回 `protocol.SessionPrincipal`。
- 两个 `actorcmd.CommandContext` 实现生成接口的 `Identity() protocol.SessionIdentity` 与 `UpSeq() int64`；没有 Agent 身份的内部 Actor 调用返回零值。
- 两个项目的 Agent 命令完成日志在原有 SessionID 基础上增加 AccountID、BorrowAccountID、ServerID、ConnectionID 与 AccessMode，确保同一逻辑会话的物理连接可追踪。
- 不在本阶段修改 Handler 响应返回 Agent 的旧路径；Game/Actor 直发 MsgQ 属于后续独立阶段。

- [x] **Step 1: Write failing project tests**

断言调试 Token 返回完整 Principal，且 CommandContext 返回 Delivery 的 Identity 与 UpSeq。

- [x] **Step 2: Regenerate both projects**

Run: `/Users/a47/workspace/txgame/gen/run.sh`

Run: `/Users/a47/workspace/txgameslg/gen/run.sh`

- [x] **Step 3: Implement project adapters**

两个调试解析器采用相同示例映射；两个 CommandContext 采用相同只读语义，但工程之间不互相 import。

- [x] **Step 4: Run project tests**

Run: `go test ./... -count=1` in both `server` modules.

### Task 5: 更新稳定说明并完成全量回归

**Files:**
- Modify: `/Users/a47/workspace/tx/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/txgen/README.md` and design/plan if public generation contract changes
- Modify: `/Users/a47/workspace/txgame/README.md`
- Modify: `/Users/a47/workspace/txgameslg/README.md`

- [x] **Step 1: Document current behavior and next migration order**

- [x] **Step 2: Run format and diff checks**

Run: `git diff --check` in every changed repository.

- [x] **Step 3: Run full verification**

Run full Go tests for tx, txgen, txgame/gen, txgame/server, txgameslg/gen and txgameslg/server. Tests that bind local ports must run outside the restricted sandbox.
