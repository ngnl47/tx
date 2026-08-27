# Bee 与 Ant 边界迁移审计报告

审计基准 revision：`c9073cbf3981fcfc37f8a51a3133c548ed00be30` 及本轮未提交变更  
Go 版本：`go1.27.0 darwin/amd64`  
CPU：`Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz`  
审计范围：Core L0-L2 组件、私有装配内核、Bee/Ant 门面、标准 agent/msgq 注册、
CModProcedure、txgen 运行模型边界、txgame 和 txgameslg 组合根。

## Findings

### [P1 已修复] Bee/Ant 配置结构没有自动对称性护栏

- 位置：`tests/facade_symmetry_test.go:46`
- 触发条件：Bee 的 `Config`、`RunOptions`、`ServiceSpec` 或 CMod 公共字段发生变更，Ant 未同步。
- 影响：维护无状态和有状态项目的开发者会面对两套意外分叉的装配 API，且编译器不会在另一门面中报错。
- 证据：原有测试只比较 `Context`、`Builder` 和 `Server` 方法集，没有比较公开结构字段。
- 处理：增加结构子集断言，强制 Bee 共有字段在 Ant 中保持同名同类型；Ant 仍可增加 WAL、Transaction 和 LogicalSlot 字段。

### [P1 已修复] 公开门面字段缺少生效条件与零值说明

- 位置：`bee/config.go`、`ant/config.go`、`bee/cmod.go`、`ant/cmod.go`、`bee/service.go`、`ant/service.go`
- 触发条件：项目装配 `Config`、`RunOptions`、`CModSpec` 或 `ServiceRegistration`。
- 影响：使用者需要读取私有实现才能理解字段来源、零值和资源所有权，公开 API 不足以自说明。
- 证据：迁移后的公开结构只有类型 GoDoc，字段无单独说明。
- 处理：补齐字段和公开接口方法的中文 GoDoc，并在 Bee/Ant 共有部分使用相同语义。

### [P1 已修复] 生效审计标准和私有注释仍指向已移除的边界

- 位置：`docs/architecture_audit_standard.md:43`、`internal/framework/control.go`、`internal/framework/storage_adapter.go`
- 触发条件：后续里程碑按审计标准判断 L3 owner，或维护者阅读私有装配内核。
- 影响：后续实现可能把项目重新引向已移除的公开 Server 包，破坏 Core/Bee/Ant 单向依赖。
- 证据：标准把 L3 owner 写为单一 `server.Server`，私有代码仍使用“迁移适配器”和“迁移期”。
- 处理：统一为“Bee/Ant 公开进程宿主 + `internal/framework` 私有装配内核”，并在审计标准中固定门面对称和禁止反向依赖规则。

### [P0 已修复] L4 Service 后台任务可能越过最终刷盘边界

- 位置：`internal/framework/service_host.go`、`internal/framework/context.go`、`internal/framework/shutdown.go`
- 触发条件：Service 通过 `Context.Tasks().Go` 运行后台任务，并在 Service Stop 后、CModProcedure 最终 Drain 前继续提交变更。
- 影响：最终 Flush 完成后仍可能产生新的内存 pending，进程退出时丢失，破坏优雅关闭的数据完整性。
- 证据：Service 原先直接复用进程根 Tasks；关闭顺序只停止 Service，没有先取消并等待其后台任务。
- 处理：为每个 Service 创建独立 `ChildWaitGroupContext`；关闭固定为入口排空、取消并等待 Service Tasks、Service Stop、CMod 最终 Drain。启动回滚也使用相同所有权规则。

### [P1 已修复] 存储 CMod 返回接口宽于 Requires 能力边界

- 位置：`bee/context.go`、`ant/context.go`、`internal/framework/context_access.go`
- 触发条件：同一进程的不同 Service 选择了 Redis、MongoDB 或 Fusion，而当前 Service 只声明其中一种。
- 影响：当前 Service 可从宽 `Storage` 接口看到其他模式 reader，并用 mode-aware writer 写入未声明后端，削弱编译期契约。
- 证据：CMod accessor 已校验直接声明，但返回值仍包含三个 reader 和通用 writer。
- 处理：Redis、MongoDB、Fusion 分别返回窄接口；Core 的模式 writer 在第一次 I/O 前校验 `TypeSpec.Storage.Mode`。组合根的通用 `Storage()` 仅用于安装业务 facade。

### [P1 已修复] Service 启动回滚在释放资源前等待后台任务

- 位置：`internal/framework/service_host.go`
- 触发条件：已启动 Service 的后台任务依赖 listener 或其他 Service 自有资源退出，后续 Service 启动失败。
- 影响：回滚若先等待任务再调用 `Stop`，任务与回滚互相等待，`Server.Start` 永久阻塞。
- 证据：标准 Agent 的 Accept 循环需要关闭 listener；单独取消 context 不会解除阻塞中的 `Accept`。
- 处理：启动回滚使用独立顺序：取消任务、调用幂等 `Stop` 释放部分资源、等待任务退出。正常关闭仍在 Drain 已封口入口后先等待任务，再调用 `Stop`。

## 未发现问题的检查项

- `tx/server` 公开入口已移除；Core 和私有装配内核不 import Bee/Ant，Bee 与 Ant 不互相 import。
- txgame 只使用 Bee，txgameslg 只使用 Ant；txgen 生成代码保持运行模型中立。
- Bee/Ant 的 Builder、Server、Context、ServiceRegistration 和标准 agent/msgq 注册保持对称；CModProcedure 只由 Ant 暴露。
- Service/CMod Factory 只能取得 `Requires` 直接声明的能力；存储 wrapper 同时限制 reader 方法集与 writer 模式，且不暴露 backend 关闭、快照替换、Actor 停止或 Procedure Flush 所有权。
- `component` 依赖图和生命周期 Host 保持中立；计算内核未因门面迁移引入新锁或 I/O。
- 标准 agent/msgq 核心实现位于中立 `modules/*/service`，Bee/Ant 只保留类型安全注册与错误前缀。
- CModProcedure 不隐式选择 Fusion 或 MongoDB；Cache 和可选 WAL 只通过窄 SPI 取得所需能力。
- 四工程全量测试、`tx` 全包 race 与四工程 `go vet ./...` 通过。

## 性能对比

下表使用迁移基线的同一机器、Go 版本、数据规模和 `count=5` 命令。时延为五次运行范围；
分配数与基线一致。槽位缓存路由因混合 benchmark 期间机器调度抖动，另使用 `count=10` 独立复测。

| 场景 | 迁移基线 ns/op | 本轮 ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Fixed Actor 已激活，串行 | 10,602-11,095 | 10,127-10,301 | 729-739 | 12 |
| Fixed Actor 已激活，并行 | 1,206-1,478 | 1,259-1,355 | 636-638 | 10 |
| Fixed Actor 冷激活，串行 | 16,344-16,925 | 15,712-16,585 | 2,639-2,688 | 34 |
| Fixed Actor 冷激活，并行 | 4,441-5,273 | 3,877-4,359 | 2,483-2,609 | 33 |
| Fixed Actor 创建/销毁 | 31,721-34,626 | 21,217-22,111 | 2,294-2,301 | 39 |
| 槽位缓存路由，串行 | 135.2-149.4 | 106.4-116.1 | 0 | 0 |
| 槽位缓存路由，并行 | 68.38-75.12 | 54.21-71.78 | 0 | 0 |
| Fusion ApplyChanges | 1,439-1,513 | 1,465-1,609 | 1,624 | 8 |
| Procedure 根过程 | 997.1-1,017 | 912.1-941.8 | 1,160 | 9 |
| Procedure 子过程复用根锁 | 1,191-1,235 | 1,079-1,106 | 1,192 | 10 |
| Procedure 子过程动态加锁 | 1,826-1,901 | 1,675-1,731 | 1,481 | 14 |
| 事务提交到 Cache Manager | 3,590-3,796 | 2,824-3,392 | 2,234 | 25 |
| WAL NewRecord，1 KiB | 2,827-2,915 | 2,599-2,626 | 0 | 0 |
| WAL checkpoint 合并提交 | 79.84-87.70 | 75.65-78.43 | 0 | 0 |
| 生成 Store，单标量 | 2,932-3,000 | 2,634-2,693 | 5,216 | 46 |
| 生成 Store，100 项 map | 7,630-7,798 | 6,856-7,029 | 12,848 | 157 |
| Snapshot Diff，单标量 | 3,319-3,793 | 2,965-3,019 | 5,488 | 48 |
| Snapshot Diff，100 项 map | 56,763-58,535 | 51,820-53,856 | 26,826-26,878 | 320 |

Fusion 的五次时延范围与基线略有重叠外的抖动，但对应实现本轮未变更，`B/op` 和 `allocs/op`
完全一致，没有门面适配层进入热路径的证据。其余场景时延范围改善或与基线重叠。

## 残余风险

- Bee/Ant 门面保留两份静态强类型定义，以换取独立包文档、明确类型身份和禁止反向依赖。对称测试能防止签名和字段分叉，但不能单独证明语义一致；生命周期对称测试和四工程集成回归共同补足这一点。
- `internal/framework` 作为唯一进程装配内核必然了解多个 L0-L2 组件；Go `internal` 规则和依赖护栏防止项目代码直接依赖它。后续新能力仍应优先下沉中立组件，不把业务策略内联到该包。
- 真实链路回归覆盖本地单节点 Redis、MongoDB 和 etcd，未替代云 Redis Cluster、MongoDB Replica Set 或多机滚动发布验收。

## 建议实施顺序

本轮未留下未解决的 P0/P1 finding。完成最终差异检查和四工程全量验证后，可以结束 Bee/Ant 边界迁移；
后续新增能力按该边界直接进入 Core、Bee 或 Ant，不再引入并行公开装配入口。
