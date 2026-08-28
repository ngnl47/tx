# Agent Session Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 统一 UpMsg/DownMsg 术语，并让 TCP、WebSocket 客户端在网络中断后可靠重放尚未确认的上行帧。

**Architecture:** txclient 在 AgentClient 内持有只读 UpMsg 队列，重连时沿用 AgentSession 并重放未确认帧；msgq 继续持有 DownMsg 队列，并严格区分新建和恢复。Agent 在恢复成功后立即反馈已接受的 UpMsg 序号，重复上行由 msgq 幂等吸收。

**Tech Stack:** Go 1.26、TypeScript、Vue 3、Node test runner、TCP、WebSocket、gRPC、BON。

**Spec:** `docs/superpowers/specs/2026-08-28-agent-session-recovery-design.md`

## Global Constraints

- Frame 枚举数值和 32 字节线协议布局保持不变。
- 正式方向术语只使用 `UpMsg/DownMsg`；`sent/received` 只用于观察事件。
- 新增或修改的代码注释、GoDoc、TypeScript 注释和维护说明使用中文。
- Payload 入队后只读，不为重放增加逐帧复制。
- 不提交 Git，由用户统一提交工作区。

---

### Task 1: 固定 Frame 与 Session 方向术语

**Files:**
- Modify: `modules/agent/frame.go`
- Modify: `modules/agent/session.go`
- Modify: `modules/msgq/mailbox.go`
- Modify: `/Users/a47/workspace/txclient/src/protocol/frame.ts`
- Modify: `/Users/a47/workspace/txclient/src/protocol/session.ts`
- Test: `tests/agent_frame_route_test.go`
- Test: `/Users/a47/workspace/txclient/tests/frame.test.ts`
- Test: `/Users/a47/workspace/txclient/tests/session.test.ts`

**Interfaces:**
- Produces: `FrameUpMsg`、`FrameDownMsg`、`FrameKind.UpMsg`、`FrameKind.DownMsg`。
- Produces: `AgentSessionState.nextUpSeq`、`lastDownSeq`、`lastUpAck`。

- [x] 修改测试，只接受最终 UpMsg/DownMsg 名称并保持枚举数值不变。
- [x] 运行 Go 与 TypeScript 定向测试，确认旧名称导致编译失败。
- [x] 重命名 Frame、Session 状态和 Agent/msgq 内部方向变量。
- [x] 运行定向测试并执行 gofmt。

### Task 2: 实现 txclient UpMsg 重放队列

**Files:**
- Create: `/Users/a47/workspace/txclient/src/protocol/up-msg-queue.ts`
- Modify: `/Users/a47/workspace/txclient/src/client.ts`
- Modify: `/Users/a47/workspace/txclient/src/protocol/session.ts`
- Test: `/Users/a47/workspace/txclient/tests/client.test.ts`
- Test: `/Users/a47/workspace/txclient/tests/session.test.ts`

**Interfaces:**
- Produces: `AgentClient.connect(token)`，仅建立新会话。
- Produces: `AgentClient.reconnect(token)`，恢复并重放未确认 UpMsg。
- Consumes: `AgentSession` 的 UpMsg Seq/Ack 状态。

- [x] 添加失败测试：UpMsg 在 transport.send 失败前已经入队，重连后按原 Seq 重发。
- [x] 添加失败测试：合法 Ack 释放队列前缀，未确认后缀继续保留。
- [x] 添加失败测试：已有 UpMsg 的 AgentClient 拒绝再次调用新会话 connect。
- [x] 实现紧凑 UpMsg 队列和 connect/reconnect 门面。
- [x] 运行 txclient 单元测试，确认重放不重新编码、不重新分配 Seq。

### Task 3: 收紧 msgq 新建与恢复语义

**Files:**
- Modify: `modules/msgq/mailbox.go`
- Modify: `modules/msgq/session.go`
- Modify: `modules/msgq/grpc.go`
- Test: `modules/msgq/mailbox_test.go`
- Test: `tests/msgq_mailbox_test.go`

**Interfaces:**
- Produces: 非恢复 Init 始终安装新的 mailbox Session。
- Produces: 恢复 Init 只接受现存且 SessionID 相同的 Session。
- Produces: `InitResult.UpMsgSeq` 和 `RecResult.UpMsgSeq`。

- [x] 添加失败测试：同 SessionID 的非恢复 Init 重置 UpMsg/DownMsg 状态。
- [x] 添加失败测试：恢复不存在或 SessionID 不同的会话明确失败。
- [x] 添加失败测试：恢复保留 UpMsg Seq，并重置 DownMsg 发送游标。
- [x] 实现原子的新建/恢复分支并同步 gRPC 响应校验。
- [x] 运行 msgq 全部测试和竞态相关测试。

### Task 4: Agent 恢复确认与重复上行

**Files:**
- Modify: `modules/agent/session.go`
- Test: `tests/agent_session_test.go`
- Test: `tests/agent_transport_test.go`

**Interfaces:**
- Consumes: `msgq.InitResult.UpMsgSeq`。
- Produces: 恢复成功后立即写出 `FrameAck{Ack: UpMsgSeq}`，零值时不发送。

- [x] 添加失败测试：恢复会话立即确认服务端已经接受的 UpMsg Seq。
- [x] 添加失败测试：重放已经接受的 UpMsg 不再次调用 Dispatcher。
- [x] 在连接级串行 writer 上发送恢复确认，再启动 DownMsg Fetch。
- [x] 运行 Agent 测试并确认控制帧与 DownMsg 不会并发写乱。

### Task 5: 调试页自动选择新建或恢复

**Files:**
- Modify: `/Users/a47/workspace/txclient/src/App.vue`
- Modify: `/Users/a47/workspace/txclient/src/debug/timeline.ts`
- Test: `/Users/a47/workspace/txclient/tests/app-layout.test.ts`
- Test: `/Users/a47/workspace/txclient/tests/timeline.test.ts`

**Interfaces:**
- Consumes: `AgentClient.connect` 与 `AgentClient.reconnect`。
- Produces: 同 endpoint/token 重连自动恢复；变化后创建新会话。

- [x] 添加失败测试：页面不再暴露可产生矛盾状态的 restore 复选框。
- [x] 添加失败测试：时间线以 `UP/DOWN` 展示 UpMsg/DownMsg。
- [x] 保留断开后的 AgentClient 和 AgentSession，同 endpoint/token 使用 reconnect。
- [x] endpoint 或 token 改变时释放旧门面并创建新会话。
- [x] 运行 txclient 全部测试和生产构建。

### Task 6: 真实链路与文档同步

**Files:**
- Modify: `README.md`
- Modify: `docs/design.md`
- Modify: `/Users/a47/workspace/txclient/README.md`
- Modify: `/Users/a47/workspace/txclient/src/cli/e2e.ts`
- Modify: `/Users/a47/workspace/txgame/server/tests/integration/agent_transport_test.go`（若现有集成入口位于其他文件，则修改对应 Agent 集成测试文件）

**Interfaces:**
- Documents: UpMsg/DownMsg、双向队列、新建登录、断线恢复和进程退出边界。
- Verifies: TCP 与 WebSocket 均可在中断后重放未确认 UpMsg。

- [x] 修改 E2E：首次 Ping 后断开，恢复同一 Session 并验证后续 Pong 的 Seq/Ack 连续。
- [x] 增加丢失上行模拟：客户端分配 Seq 后 transport 失败，恢复时重发并只执行一次业务处理。
- [x] 更新 README 与 design，仅使用最终术语和最终 API。
- [x] 运行 tx、txclient、txgame、txgameslg 全量测试。
- [x] 对所有工作区执行 `git diff --check`，检查旧术语残留和生成产物一致性。
