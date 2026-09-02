# 运行实例身份与 SessionID 设计

## 状态

本设计于 2026-09-02 完成讨论，尚未实施。它替代由部署参数提供 `InstanceID`、使用 MongoDB 分配 `SessionID`，以及把运行身份单独暴露为 `CModRuntimeIdentity` 的旧计划。

## 目标

- 在 tx 增加 L1 `tx/etcd` 基础包，统一承接可复用的 etcd 客户端、租约和原子键操作。
- 所有加入 tx 服务发现集群的服务端进程自动取得本次启动唯一的 `InstanceID`。
- 不再要求编排系统注入 `TXGAME_INSTANCE_ID`、`TXGAMESLG_INSTANCE_ID` 或同类参数。
- 通过 etcd 租约保证同一时刻运行节点的 `workSlot` 不重复。
- 由 tx 为 Login 提供 JavaScript 安全整数范围内的 SessionID 分配器。
- 保持物理运行实例、稳定业务槽位和进程内多个服务三种语义分离。
- etcd 租约或时钟异常时停止发号，不以降级策略制造重复 ID。

## 非目标

- 不使用 MongoDB、Redis 或人工配置分配运行实例身份。
- 不把 `workSlot` 当作稳定业务槽位。
- 不改变 `LogicalSlot`、`SlotAssignment` 和 generation 的归属模型。
- 不为客户端生成 AccountID、RoleID 或其他业务 ID。
- 不承诺 SessionID 在跨越 29 位时间周期后仍永久不重复，也不把 SessionID 作为永久业务主键。
- 不用 `tx/etcd` 包装 ProtoActor 官方 etcd Cluster Provider；Actor 集群继续维护自己的成员协议和客户端生命周期。
- 不把 `tx/etcd` 扩展成通用分布式协调框架。

## 分层与所有权

### L1 `tx/etcd`

新增包路径：

```text
github.com/ngnl47/tx/etcd
```

该包只接收已经解析完成的配置，不读取项目 yml、环境变量或业务键空间。它负责：

- etcd 客户端创建、健康检查和关闭。
- 租约的申请、续租、丢失通知、主动撤销与停止续租后自然过期。
- 带租约的键写入。
- 以 `CreateRevision(key) == 0` 原子抢占明确键，并返回成功写入的 `ModRevision`。
- Discovery 已经需要的 Get、前缀快照、Watch、Delete 和基于 `ModRevision` 的 Compare-And-Swap 基础操作。

该包不知道 `InstanceID`、`workSlot`、`SessionID`、Discovery 实例文档和 `LogicalSlot`。它不扫描 Redis，也不通过无边界 etcd 键扫描寻找槽位；上层必须给出明确键或有边界的业务前缀。

### L2 `modules/discovery`

Discovery 组合 `tx/etcd`，拥有以下领域语义：

- 服务实例注册、状态和目录快照。
- workSlot 键空间、槽位抢占和 `InstanceID` 构造。
- 逻辑槽位 assignment、generation 和目录缓存。
- etcd 键前缀、实例 JSON 与 Watch 收敛规则。

Discovery 之外的 CMod 和业务服务不直接操作 workSlot 租约。测试可以通过窄替身验证分配算法；真实集成测试使用 `tx/etcd` 和 etcd。

## 术语与边界

### 服务端进程

一个操作系统进程或容器实例只申请一个 `workSlot`。同一进程选择启动的 Login、Agent、MsgQ、Game、SLGGame 或 SLGWorld 服务共享同一个 `InstanceID`。

txclient、txclientslg 和 txgen 不属于服务端集群节点，不申请 `workSlot`。

### workSlot

`workSlot` 是当前 etcd 集群租约保护下的短期数值槽位，范围为 `[0, 4095]`。槽位可以在旧租约自然过期后由新进程复用，但同一时刻只能有一个所有者。

### InstanceID

`InstanceID` 标识一次具体的进程启动，其格式固定为：

```text
<AppID>-<workSlot>-<ModRevision>
```

`ModRevision` 是进程成功创建 workSlot 键时由 etcd 产生的修订版本。新进程即使复用同一个 `workSlot`，也会得到不同的 `InstanceID`，避免旧路由、延迟请求和日志把两次启动误认为同一实例。

### LogicalSlot

`LogicalSlot` 是由部署控制面维护的稳定业务位置，例如一个有状态世界槽位。它继续通过持久化 `SlotAssignment` 指向当前 `RuntimeInstanceID`，不参与 `InstanceID` 拼接，也不使用 workSlot 替代。

## workSlot 注册

Discovery etcd 配置继续使用同一个应用前缀。标准键空间为：

```text
<discovery-root>/instances/<instance-id>
<discovery-root>/work-slots/<work-slot>
<discovery-root>/slots/<logical-slot>
```

Discovery 启动时：

1. 通过 `tx/etcd` 创建一份进程租约，TTL 复用 Discovery 的 `LeaseTTL`。
2. 从随机起点开始，在 `[0, 4095]` 内循环尝试明确的 workSlot 键。
3. 每次使用 `CreateRevision(key) == 0` 的事务创建带租约的键，不扫描无边界键空间。
4. 以成功事务的 `ModRevision` 和 workSlot 生成 `InstanceID`。
5. 冻结本进程的运行实例快照，并使用同一租约注册实例键。
6. 启动实例目录和逻辑槽位目录的首轮快照与 Watch。
7. 全部前置条件满足后，才允许服务对外进入 running 状态。

全部 4096 个槽位均被占用时，进程启动失败。etcd 不可用或租约在运行期间丢失时，本次运行实例身份立即失效；框架停止接受新的外部工作、触发整进程排水关闭，SessionID 分配器拒绝继续发号。原进程不得静默重新抢占 workSlot 后继续运行；重新加入集群必须经过一次新的进程启动并取得新的 `InstanceID`。

workSlot 分配只做最多 4096 次明确键的有界 Compare-And-Swap。它不读取全部 workSlot 快照后再选择，也不引入额外 Redis 读写。

## CMod 与启动顺序

不新增 `CModRuntimeIdentity`。运行身份是 Discovery 正常工作必需的内部状态，不是业务服务需要单独选择的能力。

- `CModDiscovery` 内部启动 workSlot 租约，生成 `InstanceID`，随后注册实例并启动目录。
- 所有声明 `CModDiscovery` 的服务端进程自动获得运行身份，无须再声明其他身份 CMod。
- 新增 `CModSessionID`，依赖 `CModDiscovery`，只暴露 SessionID 分配能力。
- 当前架构中只有 Login 显式声明 `CModSessionID`；Agent、MsgQ、Game、SLGGame 和 SLGWorld 不生成 SessionID。
- `CModSessionID` 复用 Discovery 已取得的 workSlot 和租约状态，不创建第二份租约，也不允许调用方读取 workSlot。
- 不使用 Discovery 的纯单机服务不申请 workSlot，也不能选择 `CModSessionID`。

框架先创建不含人工 InstanceID 的启动描述；`CModDiscovery` 完成 workSlot 抢占后冻结最终实例快照。Discovery 实例注册、Service Factory、日志字段和其他 CMod 随后只读取该快照，不允许运行中改写 `InstanceID`。

## SessionID 布局

SessionID 使用正 `int64` 保存，但最大值严格限制为 JavaScript 最大安全整数：

```text
29 位秒级时间戳
12 位 workSlot
12 位秒内序列号
─────────────────
53 位 SessionID
```

计算公式为：

```text
elapsed   = unixSeconds(now) - unixSeconds(2026-01-01T00:00:00Z)
timestamp = elapsed & ((1 << 29) - 1)
sessionID = (timestamp << 24) | (workSlot << 12) | sequence
```

边界如下：

- 纪元为 `2026-01-01T00:00:00Z`，纪元之前的时间拒绝发号。
- `timestamp` 保存 `elapsed` 的低 29 位，每 `2^29` 秒自然回绕一次，周期约为 17.01 年。
- `workSlot` 范围为 `[0, 4095]`。
- `sequence` 范围为 `[0, 4095]`，每个 workSlot 每个原始 Unix 秒独立重置。
- 最大 SessionID 为 `2^53-1`，TypeScript `number` 和 JSON 数值均可精确表示。
- 单个 Login 实例最多生成 4096 个 SessionID/秒；多个 Login 实例按各自 workSlot 水平扩展。
- 当 timestamp、workSlot、sequence 都为零时，分配器跳过该序列组合，保证 SessionID 始终为正数；只有 workSlot 0 在每次时间戳回绕后的首秒少一个可用编号。

同一原始 Unix 秒的序列耗尽时，调用等待到下一秒或调用上下文结束，不允许序列回绕。生成器保存上一发号使用的原始 Unix 秒；本机时钟回退到该秒之前时停止发号，直到时钟追平或调用上下文结束。回退检测不使用已截断的 29 位 timestamp，因此原始时间正常跨越回绕边界时不会被误判。

时间戳按位与后的自然回绕是算法的一部分，不设置“纪元耗尽”错误，也不要求停服切换纪元。SessionID 的唯一性契约限定为同一集群的有效 Session 生命周期：

- 进程一次运行时长和 Session 最大有效生命周期都必须显著小于 17.01 年。
- SessionID 不提供永久去重、排序或事件时间语义。
- 业务数据、审计记录和长期存档不得只以 SessionID 作为永久主键或时间依据。
- 日志使用事件时间、SessionID、ConnectionID 和 InstanceID 联合排查。

公开接口只暴露 SessionID 语义：

```go
type SessionIDAllocator interface {
	AllocateSessionID(context.Context) (int64, error)
}
```

调用方不能指定时间戳、workSlot、序列或位段。

## 优雅关闭

关闭顺序固定为：

1. 停止外部准入并排水 Service。
2. 将 Discovery 实例标记为 stopping，删除实例注册并关闭目录 Watch。
3. 停止 `CModSessionID` 接受新分配。
4. 最后停止进程租约续租，让 workSlot 键经过 `LeaseTTL` 后自然过期。

正常关闭不主动撤销进程租约，避免新进程在旧进程退出的同一时间窗口立即复用 workSlot。启动尚未完成、实例未进入 running 且从未生成 SessionID 时，可以主动撤销失败启动创建的租约。

持久化 LogicalSlot assignment 不随进程退出删除，仍由现有发布控制面完成 generation 接管。

## 配置与兼容策略

- 删除 `RunOptions.InstanceID` 以及 Bee、Ant 对应公开字段。
- 删除 txgame、txgameslg 的实例 ID 环境变量和运维说明。
- 保留 `InstanceVersion`、内部 RPC Endpoint、公开 Endpoint 和 `LogicalSlot`；它们不能由 workSlot 推导。
- `CModSessionID` 没有独立 workSlot、worker ID 或纪元配置。
- 不保留人工 InstanceID 兼容入口。测试通过可控的 `tx/etcd` 替身或真实 etcd 获得实例身份。
- Discovery Prefix 仍负责环境隔离；不同前缀属于不同运行集群，允许出现相同数值的 SessionID。

## 测试要求

### `tx/etcd` 单元与集成测试

- 租约能续租、报告丢失，并按选择主动撤销或停止续租后自然过期。
- 两个并发键抢占只能有一个成功创建同一键，成功结果携带有效 `ModRevision`。
- Get、前缀快照、Watch、Delete 和基于 `ModRevision` 的 Compare-And-Swap 保持现有 Discovery 语义。
- 客户端关闭、租约丢失和 etcd 不可达均返回明确错误，不产生后台协程泄漏。

### Discovery 与 SessionID 单元测试

- 两个并发注册只能有一个进程取得同一 workSlot。
- 新进程复用过期 workSlot 时取得不同 ModRevision 和 InstanceID。
- 同一进程内多个服务共享 InstanceID，多个独立进程的 InstanceID 不重复。
- workSlot 耗尽、etcd 不可达和租约丢失均明确失败。
- 未选择 `CModSessionID` 的 Discovery 节点不构造分配器。
- 生成的 SessionID 为正数且不超过 `Number.MAX_SAFE_INTEGER`。
- timestamp、workSlot、sequence 全零时跳过零值并返回下一个正 SessionID。
- 同秒并发生成不重复，4096 个序列耗尽后等待下一秒。
- 不同 workSlot 在相同时间和序列下仍生成不同 SessionID。
- 时钟回退和纪元前时间停止发号。
- 原始时间跨越 `2^29` 秒边界时 timestamp 从最大值自然回绕为零，生成器继续工作。
- 序列按原始 Unix 秒重置，不因截断后的 timestamp 相同而复用同秒序列状态。
- 优雅关闭立即移除实例注册，但 workSlot 在租约自然过期前不可复用。

### 框架与真实 etcd 验收

- Bee、Ant 所有 Discovery 节点不传 InstanceID 也能启动并注册。
- Agent、MsgQ、Game、Login、SLGGame、SLGWorld 的服务目录和日志使用自动 InstanceID。
- 只有 Login 选择 `CModSessionID` 并生成 SessionID。
- 进程重启后 workSlot 可以复用，但 InstanceID 必须变化。
- LogicalSlot assignment 的 RuntimeInstanceID 随新实例更新，SlotID 本身保持稳定。
- 关闭后 Discovery instances 清空，workSlot 在 TTL 后释放，持久 LogicalSlot assignment 保留。

## 迁移顺序

1. 新增 L1 `tx/etcd`，迁移 `modules/discovery` 的客户端、租约、键值和 Watch 基建。
2. 在 Discovery 内实现 workSlot 抢占、自动 InstanceID 和统一进程租约。
3. 实现 29/12/12 SessionID 生成器与自然回绕测试。
4. 新增依赖 `CModDiscovery` 的 `CModSessionID`，只向 Login 暴露窄分配接口。
5. 从 Bee、Ant 和内部框架删除人工 InstanceID。
6. 迁移 txgame、txgameslg 配置、启动器、集群编排器和文档。
7. 完成真实 etcd 多进程身份、SessionID、重启复用与优雅关闭验收。
