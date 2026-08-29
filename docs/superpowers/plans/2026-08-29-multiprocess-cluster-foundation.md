# Multi-process Cluster Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为无状态和有状态多进程真实集群验收补齐稳定的 Slot bootstrap、MsgQ 本地 Topic 路由和六工程文档契约。

**Architecture:** `tx` 控制面通过现有 Discovery `SlotStore` 暴露显式 `CreateSlot`，项目编排器不直接写 etcd；MsgQ Sender 使用角色当前 owner 选择一个 MsgQ 实例完成 JoinTopics 与本地 BroadcastTopic，明确不提供全局广播假象。六工程文档固定两条生成链和两套顺序验收边界。

**Tech Stack:** Go 1.26、gRPC、Protocol Buffers、etcd、Redis、Markdown。

**Spec:** `docs/superpowers/specs/2026-08-29-multiprocess-cluster-closure-design.md`

## Global Constraints

- 不提交 Git，由用户统一提交工作区。
- 新增或修改的注释、GoDoc、测试说明和维护文档使用中文。
- `tx` 不包含 `game`、`slggame`、`slgworld` 等项目业务名。
- Topic 广播保持单 MsgQ 实例语义；跨实例分发由业务层负责。
- Redis key 保持现有模块契约，不增加全局项目名前缀。
- 每个任务先运行新增测试并确认失败，再实现最小代码使其通过。

---

### Task 1: 为控制面增加显式 CreateSlot

**Files:**
- Modify: `modules/control/proto/control.proto`
- Modify: `modules/control/pb/control.pb.go`
- Modify: `modules/control/pb/control_grpc.pb.go`
- Modify: `modules/control/slot.go`
- Modify: `modules/control/server.go`
- Modify: `modules/control/promoter.go`
- Modify: `modules/control/server_test.go`
- Modify: `modules/control/promoter_test.go`
- Modify: `modules/discovery/slot.go`
- Modify: `modules/discovery/slot_test.go`
- Modify: `cmd/txctl/main.go`
- Modify: `cmd/txctl/main_test.go`

**Interfaces:**
- Produces RPC: `ControlService.CreateSlot(CreateSlotRequest) returns (CreateSlotResponse)`。
- Produces CLI: `txctl create-slot --slot <slot> --instance <id>`；service namespace 由目标进程的 Discovery prefix 决定，不在 assignment 中重复编码。
- Consumes: `discovery.SlotStore.CreateSlot(ctx, assignment)`；assignment 初始 generation 固定为 1。
- Preserves: `PromoteSlot` generation CAS 语义和既有 CLI 参数。

- [x] 在 `server_test.go` 增加成功创建、重复创建冲突、SlotStore 错误透传测试。
- [x] 在 `slot_test.go` 增加初始目标必须存在且为 running、成功创建固定 generation 1 的测试。
- [x] 在 `promoter_test.go` 增加 promoter 对 `CreateSlot` 的参数校验与委托测试。
- [x] 在 `cmd/txctl/main_test.go` 增加缺参、成功输出和 RPC 失败退出测试。
- [x] 运行 `go test ./modules/control ./cmd/txctl -count=1`，确认新测试因 API 缺失失败。
- [x] 修改 proto，重新生成 control pb 文件，并检查生成命令只改预期产物。
- [x] 扩展 `SlotController`/promoter/server，把 already-exists 与非法参数返回明确 gRPC 状态。
- [x] 扩展 txctl 子命令和帮助文本，成功输出 slot、instance 与初始 generation。
- [x] 运行 `gofmt` 与定向测试；共享阶段全量 `go test ./...` 在 Task 4 统一执行。

### Task 2: 为 MsgQ Sender 增加本地 Topic 操作

**Files:**
- Modify: `modules/msgq/client.go`
- Modify: `modules/msgq/sender.go`
- Modify: `modules/msgq/grpc.go`
- Modify: `modules/msgq/grpc_client_test.go`
- Create: `modules/msgq/sender_test.go`
- Modify: `modules/msgq/mailbox_test.go`

**Interfaces:**
- Produces: `Sender.JoinTopics(ctx context.Context, roleID int64, topics ...string) error`。
- Produces: `Sender.BroadcastTopic(ctx context.Context, ownerRoleID int64, request BroadcastTopicRequest) (BroadcastTopicResult, error)`。
- Extends internal `Connection` with `JoinTopics` and `BroadcastTopic` methods matching the existing gRPC MailboxService operations.
- `ownerRoleID` only selects that role当前绑定的 MsgQ 实例；返回结果只描述该实例的 delivered/skipped/backlog。

- [x] 在 `sender_test.go` 增加 owner 缺失、JoinTopics 精确会话透传、本地广播结果透传测试。
- [x] 在 `grpc_client_test.go` 增加 Session header、topic 列表、Broadcast request/response 转换测试。
- [x] 运行 `go test ./modules/msgq -count=1`，确认测试因接口缺失失败。
- [x] 扩展 Connection 和 gRPC client，沿用现有 owner session 校验与连接缓存。
- [x] 实现 Sender 两个方法；沿用 Resolve 的 owner binding 校验与受控刷新，不扫描全部 MsgQ 实例，也不在远端副作用未知时自动重试。
- [x] 为导出 API 添加中文 GoDoc，明确本地广播边界。
- [x] 运行 `gofmt`、`go test ./modules/msgq -count=1` 和 `go test ./... -count=1`。

### Task 3: 同步六工程稳定契约

**Files:**
- Modify: `README.md`
- Modify: `docs/design.md`
- Modify: `docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/txgen/README.md`
- Modify: `/Users/a47/workspace/txgen/docs/design.md`
- Modify: `/Users/a47/workspace/txgen/docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/txgame/README.md`
- Modify: `/Users/a47/workspace/txgameslg/README.md`
- Modify: `/Users/a47/workspace/txclient/README.md`
- Modify: `/Users/a47/workspace/txclientslg/README.md`

**Documentation Contract:**
- 固定 `txgame/gen -> txgame/server/gen + txclient/src/gen`。
- 固定 `txgameslg/gen -> txgameslg/server/gen + txclientslg/src/gen`。
- 定义无状态/有状态两套验收并行、在本地顺序执行。
- 定义 Redis DB、Mongo database、etcd prefix、MsgQ 本地广播和失败保留证据边界。

- [x] 更新 tx 设计与实施计划，把 P0 从五工程修订为六工程。
- [x] 更新 txgen 生成边界与回归矩阵，禁止两个客户端交叉消费协议。
- [x] 更新四个示例工程 README，记录各自套件职责；具体命令在后续项目计划落地时复核。
- [x] 运行 `rg -n "txclient|txclientslg|DB 14|DB 15|End-to-End|本地.*广播"` 检查术语和配对。
- [x] 对六工程运行 `git diff --check`。

### Task 4: 共享阶段验收

**Files:**
- Verify only: six project worktrees.

- [x] 运行 tx `go test ./... -count=1`。
- [x] 运行 txgen `go test ./... -count=1`。
- [x] 运行 txgame/gen、txgame/server、txgameslg/gen、txgameslg/server 的 `go test ./... -count=1`，确认共享 API 没有破坏编译。
- [x] 运行 txclient、txclientslg 的 `npm test` 与 `npm run build`。
- [x] 对六工程运行 `git status --short` 与 `git diff --check`，确认没有生成目录越界和无关修改。
- [x] 记录建议的中文 Angular Commit Message；不执行 `git commit`。
