# MsgQ 精确会话与角色主会话实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 MsgQ 邮箱、Redis binding 和跨进程路由迁移到 SessionID，并建立只包含正常可写会话的 RoleID 主会话投影与显式 `PubRole`。

**Architecture:** SessionID 精确 binding 与 RoleID 主 Session 投影使用两个单 key 条件索引；owner 负责活性续期，Router 只读；所有下行最终通过 `PubSession` 进入一个逻辑会话邮箱。

**Tech Stack:** Go 1.26、Redis Lua、gRPC、Protocol Buffers 36.0、双示例多进程集群。

**Spec:** `docs/superpowers/specs/2026-09-01-msgq-session-primary-design.md`

## Global Constraints

- 不提交 Git，由维护者统一审核提交。
- 不修改 `/Users/a47/workspace/txgameslgworld`。
- 不保留旧 `Pub`、RoleID mailbox key 或隐式兼容分支。
- 新增或修改的注释、GoDoc 和文档使用中文。
- 遵循测试驱动；竞态测试使用 channel/barrier，不依赖 sleep。
- protobuf 生成物只能通过仓库脚本和 protoc 36.0 生成。
- 本阶段不改变客户端 Frame、Seq/Ack、Game/Actor 响应返回 Agent 的路径。

---

### Task 1: 重构 SessionStore、角色主会话存储与进程内邮箱

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/msgq/session.go`
- Modify: `/Users/a47/workspace/tx/modules/msgq/session_redis.go`
- Modify: `/Users/a47/workspace/tx/modules/msgq/msgqredis/adapter.go`
- Modify: `/Users/a47/workspace/tx/modules/msgq/mailbox.go`
- Modify: `/Users/a47/workspace/tx/modules/msgq/mailbox_test.go`
- Modify: `/Users/a47/workspace/tx/tests/msgq_session_test.go`
- Modify: `/Users/a47/workspace/tx/tests/msgq_mailbox_test.go`
- Modify: Store fakes in affected tx tests

- [x] 先增加失败测试：SessionID 分片/key、完整 record、Create NX、Rebind CAS、同 Role 多 Session、主会话条件替换/续期/删除。
- [x] 修正真实 Redis mailbox 用例的显式 OfflineTimeout，确认旧失败原因。
- [x] 实现 SessionID 精确 Store 和 RoleID 主会话 Store；使用版本化 key。
- [x] 先增加失败测试：同 Role 双 Session 隔离、干净登录不继承、主会话替换、Borrow 不入主投影、恢复属性固定。
- [x] 增加确定性交错测试，复现旧 Touch 与 Restore 并发误删问题。
- [x] 将 sessions、初始化锁、查找和删除全部改为 SessionID；fresh 使用 Create，restore 使用 Rebind。
- [x] 将主会话 Bind/Refresh/Unbind 接入 Init、Touch、Close、Reap 和 overflow。
- [x] 运行 Store/mailbox 单元测试、真实 Redis 集成和 `go test -race`。

### Task 2: 迁移 gRPC、Router、Sender 与 Agent

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/msgq/proto/msgq.proto`
- Regenerate: `/Users/a47/workspace/tx/modules/msgq/pb/msgq.pb.go`
- Regenerate: `/Users/a47/workspace/tx/modules/msgq/pb/msgq_grpc.pb.go`
- Modify: `/Users/a47/workspace/tx/modules/msgq/grpc.go`
- Modify: `/Users/a47/workspace/tx/modules/msgq/client.go`
- Modify: `/Users/a47/workspace/tx/modules/msgq/sender.go`
- Modify: corresponding tx tests
- Modify: `/Users/a47/workspace/tx/modules/agent/types.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/session.go`
- Modify: corresponding Agent/service tests

- [x] 先增加失败测试：ResolveSession、ResolveRole、PubSession、PubRole、fresh/restore owner 选择和主会话 Init 标记。
- [x] 将 protobuf `Pub` 替换为 `PubSession`，为 Init 增加 `primary`，使用 protoc 36.0 重新生成。
- [x] 实现显式 Router/Sender API；Router 不刷新 TTL，gRPC 连接仍按 endpoint 复用且不自动重试发布。
- [x] Agent 以 SessionID 选择/恢复 owner，并仅为 `AccessModeNormal` 设置 Primary。
- [x] 运行 msgq、Agent、gRPC 和竞态测试。

### Task 3: 同步 txgame 与 txgameslg

**Files:**
- Modify: both projects' `/server/internal/push` façades and tests
- Modify: both projects' notification handlers and tests
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/handler.go` and tests
- Modify: both projects' cluster-e2e Redis evidence readers and tests

- [x] 将 push 门面改为显式 `PubSession`、`PubRole`、SessionID Topic owner API，删除旧 `Pub`。
- [x] 跨角色通知和世界回推使用 `PubRole`；当前订阅/本地广播从可信 HandlerContext 取 SessionID。
- [x] 集群 Redis 证据按 `session:v2:{SessionID}` 读取并核对 RoleID。
- [x] 运行两个 server 模块全量测试；客户端无需生成或修改线协议。

### Task 4: 稳定文档和完整验收

**Files:**
- Modify: `/Users/a47/workspace/tx/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/txgame/README.md`
- Modify: `/Users/a47/workspace/txgameslg/README.md`

- [x] 将公共 API、Redis key、生命周期、主会话和 Topic 语义更新为当前实现。
- [x] 从实施计划删除已完成的 SessionID/主会话两项，并保留下一个 Game/Actor 直发响应任务。
- [x] 运行 `gofmt`、`git diff --check`、tx 与两个 server 全量测试。
- [x] 运行真实 Redis Store/mailbox、`go test -race` 和可用的两套多进程消息验收。
- [x] 独立代码复审通过后给出分项目中文 Angular Commit Message。
