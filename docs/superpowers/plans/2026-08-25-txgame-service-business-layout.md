# txgame Service Business Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 txgame 运行工程重构为以 L4 Service 为所有权边界、以稳定业务能力和内部 Procedure 组织业务代码的可维护目录结构。

**Architecture:** `server/internal` 只保存进程组装、配置和基础设施 facade；Agent、Actor 命令及路由适配按所有权移动到 `server/services`。`game/modules` 使用包级函数提供稳定业务能力，`game/internal/procedure` 单向组合这些能力，并通过 Go `internal` 规则限制在 game 服务内使用。

**Tech Stack:** Go 1.26、tx/server、txgen 生成 cmd、ProtoActor-go、YAML v3。

**Spec:** `docs/superpowers/specs/2026-08-25-txgame-service-business-layout-design.md`

## Global Constraints

- 不提交 Git，由用户手动审核提交。
- 新增或修改的代码注释、GoDoc 和维护文档使用中文。
- 不修改 `server/gen/**` 生成产物。
- 保持 Agent、Actor、命令路由、Fusion 和 Procedure 的既有运行时语义。
- txgame 业务 API 不暴露 writer、Store、完整 `server.Context` 或 `txn.Runtime`。

---

### Task 1: 迁移服务专属适配器

**Files:**
- Move: `txgame/server/internal/agent/*` -> `txgame/server/services/agent/*`
- Move: `txgame/server/internal/actor/*` -> `txgame/server/services/internal/actorcmd/*`
- Move: `txgame/server/internal/commandroute/*` -> `txgame/server/services/internal/commandroute/*`
- Modify: `txgame/server/internal/app/app.go`
- Modify/Move: affected unit and integration tests

**Interfaces:**
- Consumes: tx 的 `modules/agent`、`modules/actor`、`modules/discovery` 与生成 `cmd` registry。
- Produces: `services/agent` 项目扩展入口，以及仅供 `services/**` 使用的 `actorcmd`、`commandroute`。

- [x] 先移动测试并修改 import，使测试因实现尚未迁移而失败。
- [x] 移动实现、调整包名和 import，不改变公开方法语义。
- [x] 更新组合根只从 `services/agent` 取得项目回调。
- [x] 运行 `go test ./services/... ./internal/app/... ./tests/integration/... -count=1`。

### Task 2: 建立 game 稳定业务能力和 Procedure 边界

**Files:**
- Create: `txgame/server/services/game/modules/`
- Create: `txgame/server/services/game/modules/internal/role/`
- Create: `txgame/server/services/game/internal/procedure/`
- Modify: `txgame/server/services/game/service.go`
- Test: package boundary and behavior tests beside the new packages

**Interfaces:**
- Consumes: `cmd.HandlerContext`、`txn.Executor`、`*txn.Tx` 和生成 GD 类型。
- Produces: Handler 与 Procedure 可调用的 `modules.RenameRole(*data.GDRole, string) error`；有状态根调用使用 `txn.Executor.Call`，子调用使用 `tx.Call`。

- [x] 将测试改为直接调用 `modules.RenameRole`，确认调用方不需要构造业务 Service。
- [x] 将 `PRenameRole` 移到 `services/game/internal/procedure`，并确认它通过 `modules.RenameRole` 组合稳定业务能力。
- [x] 增加根 Procedure 与子 Procedure 行为测试，锁定 `txn.Executor.Call` 和 `tx.Call` 的调用边界。
- [x] 删除无状态业务实例、构造选项和旧目录。
- [x] 运行 `go test ./services/game/... -count=1`。

### Task 3: 收敛启动、配置与命令入口

**Files:**
- Modify: `txgame/server/cmd/server/main.go`
- Delete: `txgame/server/main.go`
- Move: `txgame/server/config/local/*` -> `txgame/server/config/debug/*`
- Create: `txgame/server/config/test/`
- Create: `txgame/server/config/release/`
- Move: `txgame/gen/main/server/*` -> `txgame/gen/main/txgen/*`
- Modify: `txgame/gen/run.sh`
- Create: `txgame/server/cmd/txctl/run.sh`
- Test: command/config selection tests

**Interfaces:**
- Consumes: `--env`、`--services` 和部署环境变量。
- Produces: 缺省 `debug`、支持独立配置根目录的唯一进程入口，以及稳定的生成和运维脚本入口。

- [x] 写 `--env` 解析与非法环境测试。
- [x] 实现 `config/<env>/app.yml` 选择，移除默认任意配置路径。
- [x] 增加 `--config-root` 与 `TXGAME_CONFIG_ROOT`，避免发布二进制依赖当前工作目录。
- [x] 重命名 txgen 入口并增加 txctl 包装脚本。
- [x] 运行 gen 与 server 两个 module 的命令测试。

### Task 4: 同步文档并完成回归

**Files:**
- Modify: `txgame/README.md`
- Modify: `tx/docs/design.md`
- Modify: `tx/docs/implementation_plan.md` only if unfinished work remains

**Interfaces:**
- Consumes: 实际落地后的目录和 API。
- Produces: 面向使用者的启动/Handler/Procedure 示例，以及面向维护者的依赖边界说明。

- [x] 删除 `server/modules`、旧 `internal/agent`、旧 `internal/actor`、旧业务实例和 `config/local` 描述。
- [x] 增加稳定业务能力、Procedure 单向组合、根/子调用边界和三环境启动示例。
- [x] 运行 `git diff --check`。
- [x] 运行 tx、txgen、txgame 完整 `go test ./... -count=1` 与 `go vet ./...`。
- [x] Redis、Mongo、etcd 可用时运行真实集成回归。
