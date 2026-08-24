# CMod 运行时与 Fusion 启动恢复设计

## 目标

本次重构建立统一的 `L0-L4` 架构依赖分级，并用可扩展的能力模组图替代固定 `Capabilities` 位图。
所有通过 `--services` 选择的可运行服务都位于 L4；L4 服务只声明需要哪些 L3 能力模组，L3 `server.Server`
负责解析依赖、装配资源、管理生命周期。YAML 只配置 backend、运行时和服务参数，不再重复描述代码中已经确定的
依赖关系。

同时，Fusion 启动恢复增加 Redis lease。多个相同服务实例批量启动时，只有一个实例完整遍历 handoff slot；
其他实例等待本轮恢复成功后继续启动。lease 只减少重复 I/O，不能成为数据正确性的唯一依据，恢复流程仍保持幂等。

本次设计的优先级如下：

1. 不遗漏 Fusion 延迟回写任务，不把恢复失败伪装成启动成功。
2. 业务服务只接触粗粒度能力模组，不感知 Redis applier、Mongo loader、HandoffRecoverer 等内部零件。
3. 一份 YAML 支持任意合法的 `--services` 组合，未选中的服务不创建资源、不执行语义校验。
4. 内置和项目定制能力模组使用同一依赖图和生命周期规则。
5. 删除旧 `Capabilities` 和 `backgroundWorkers` 双轨，不保留兼容别名。

## 术语

架构、性能和审计严重级别分别使用不同前缀：

| 术语 | 含义 |
| --- | --- |
| `L0-L4` | 架构依赖层级 |
| `H0-H2` | 性能路径热度，替代现有性能文档中的 `L0/L1/L2` |
| `P0-P3` | 审计问题严重级别 |

固定术语：

- **能力模组（Capability Module，CMod）**：组合 L0-L2 能力，具有稳定身份、依赖和进程生命周期的 L3 单元。
- **服务（Service）**：可由 `--services` 选择的 L4 运行单元。
- **CMod 闭包**：选中服务直接依赖的 CMod，加上这些 CMod 全部递归依赖后的集合。
- **组合根**：txgame 中构造 CMod、服务和 `server.Server` 的项目入口，不属于可复用运行时层级。
- **启动恢复 lease**：协调多个进程完成同一轮 handoff 启动恢复的 Redis 短租约。

## L0-L4 依赖分级

| 层级 | 职责 | 当前示例 |
| --- | --- | --- |
| L0 | 纯算法、稳定契约、基础数据结构 | `change`、`meta`、`lifecycle`、存储契约、WAL 契约、协议帧 |
| L1 | backend 与传输适配 | Redis、Mongo、task backend、WAL Mongo、gRPC transport、etcd adapter |
| L2 | 可复用运行时组件 | Fusion Strategy、Transaction Runtime、Actor Runtime、Scheduler、RPC Host、Discovery Runtime |
| L3 | 能力模组、进程装配、能力门面和生命周期 | `server.CMod`、`server.Server`、`server.Context`、健康闸门、CMod 图 |
| L4 | 可启动服务和业务应用模块 | agent、msgq、game、room、battlefield、delay、task、pay |

运行时依赖方向固定为：

```text
txgame cmd / app 组合根
          |
          v
L4 可选服务与业务模块
          |
          v 只依赖 server.Context 和 CModID
L3 CMod、Server、CMod 图、进程生命周期、受控门面
          |
          v 按拓扑装配
L2 Fusion / Transaction / Actor / Scheduler / RPC 等运行时组件
          |
          v
L1 Redis / Mongo / gRPC / etcd 等 adapter
          |
          v
L0 change / meta / lifecycle / storage contract
```

约束：

- L0 不认识 Redis、Mongo、Fusion、Server 或业务服务。
- L1 不认识 agent、game、msgq 等具体服务。
- L2 不认识 txgame 的业务包。
- L3 只认识 `Service` 与 `CMod` 契约，不导入 L4 具体实现。
- L4 服务之间不直接导入兄弟服务实现；共享逻辑进入项目 domain 包或稳定低层契约。
- `--services` 只能选择 L4 服务，L0-L3 名称不能作为服务启动项。
- `txgen`、`anno`、`astscan` 和生成命令属于编译期工具链，不放入运行时 L0-L4 图；生成代码只依赖稳定的 L0 契约。

## CMod 身份与契约

CMod 身份使用强类型字符串，不使用固定宽度位图：

```go
type CModID string

const (
	CModRedisStorage   CModID = "redis-storage"
	CModMongoStorage   CModID = "mongo-storage"
	CModFusionStorage  CModID = "fusion-storage"
	CModRPC            CModID = "rpc"
	CModDiscovery      CModID = "discovery"
	CModActor          CModID = "actor"
	CModTransaction    CModID = "transaction"
	CModDelayScheduler CModID = "delay-scheduler"
	CModWAL            CModID = "wal"
)
```

选择字符串 ID 的原因：

- CMod 图只在启动装配期解析，不属于高频路径，位图没有可测量的性能价值。
- 项目可以声明 `const CModWorldCache server.CModID = "world-cache"`，不需要申请位号。
- 错误、日志、配置说明和依赖图可以直接输出稳定名称。
- 重复 ID、缺失依赖和循环依赖可以在启动前给出完整路径。

公开契约：

```go
type CModSpec struct {
	ID       CModID
	Requires []CModID
}

type CMod interface {
	Spec() CModSpec
	Start(CModContext) error
}
```

支持启动前校验与可选生命周期接口：

```go
type CModValidator interface {
	Validate(CModContext) error
}

type CModDrainer interface {
	Drain(context.Context) error
}

type CModStopper interface {
	Stop(context.Context) error
}
```

框架先对完整 CMod 闭包执行 `CModValidator.Validate`，全部通过后才允许任何 CMod 创建外部连接、监听端口或启动
goroutine。项目 CMod 可以借此在 Redis、Mongo、gRPC 等资源创建前报告配置错误。

`CModContext` 只提供能力模组装配所需的根生命周期、已启动依赖和进程身份，不暴露 `server.Server` 的停止、取消、
服务注册或其他 owner 权限。内置 CMod 可以由 `server` 私有实现适配现有 runtime；高级用户通过
`server.WithCMod(mod)` 注册项目 CMod。

项目定制模组保持强类型注入：

```go
const CModWorldCache server.CModID = "world-cache"

worldCache := worldcache.New(options)

appServer, err := server.New(
	config,
	server.WithCMod(worldCache),
	server.WithService(gameservice.New(worldCache)),
)
```

不提供 `runtime.CMod("world-cache").(any)` 之类动态断言入口。`CModID` 用于依赖解析和启动日志，业务调用继续通过
构造函数接收强类型对象。内置 CMod 仍由 `server.Context` 提供稳定的窄接口，例如 `Storage()`、`RPC()`、
`Transaction()`。

## 服务契约

`Service.Spec` 从能力位图改为模组列表：

```go
type Spec struct {
	Name     string
	Requires []CModID
}
```

多数服务只使用静态 `Requires`。少数服务的依赖由合法部署形态决定时，可以额外实现条件依赖接口：

```go
type CModRequirementsProvider interface {
	RequiredCMods(ServiceSelection) ([]CModID, error)
}
```

框架只对已经选中的服务调用该接口，并在创建任何外部连接前把返回值并入静态 `Requires`。该方法只能读取服务配置和
本轮服务选择，不能创建 client、启动 goroutine 或修改全局状态。agent 可据此在同进程包含 msgq 时使用本地会话，
在 agent 独立部署时增加 `CModDiscovery`；普通 game、msgq 等服务不需要实现该接口。`Start` 阶段不能再临时索取
依赖闭包中不存在的 CMod。

示例：

```go
func (Service) Spec() server.Spec {
	return server.Spec{
		Name: "game",
		Requires: []server.CModID{
			server.CModRPC,
			server.CModFusionStorage,
			server.CModTransaction,
			server.CModDelayScheduler,
		},
	}
}
```

首版服务矩阵：

| L4 服务 | 直接依赖模组 | 说明 |
| --- | --- | --- |
| agent | `CModRPC`、`CModRedisStorage`，集群部署时加 `CModDiscovery` | TCP/WebSocket 监听仍属于服务自身 |
| msgq | `CModRPC`、`CModRedisStorage` | 邮箱服务使用 Redis session store |
| game | `CModRPC`、`CModFusionStorage`、`CModTransaction`、`CModDelayScheduler` | Fusion 已包含 Redis、Mongo 和 handoff 正确性保护 |

未来 room、battlefield、delay、task、pay 等服务按相同方式声明，不把服务名写入 tx 的模组实现。

## 内置 CMod 依赖图

```text
CModFusionStorage
├── CModRedisStorage
└── CModMongoStorage

CModTransaction
└── CModFusionStorage

CModDelayScheduler
└── CModFusionStorage

CModActor
├── CModRPC
└── CModDiscovery

CModWAL(mongo backend)
└── CModMongoStorage
```

`HandoffRecoverer`、Fusion `Strategy`、健康闸门、delay task producer 和 TypeRegistry 都是
`CModFusionStorage` 的内部实现，不注册独立 CModID。Scheduler 的领取和处理循环属于
`CModDelayScheduler`；Fusion 保存仅依赖队列 producer，不因为 Scheduler 未部署而绕过 handoff 协议。

若未来 Transaction 支持不依赖 Fusion 的其他持久化运行时，应先抽出稳定的粗粒度存储模组，再修改依赖图；
首版按当前真实实现声明 `CModTransaction -> CModFusionStorage`，不为未实现路径提前抽象。

## CMod 图解析与生命周期

`server.Start` 在创建外部连接前完成以下步骤：

1. 解析 `--services`，得到选中 L4 服务。
2. 汇总服务直接需要的 CModID。
3. 递归展开 CMod 依赖闭包。
4. 校验重复 CModID、空 ID、缺失依赖和依赖环。
5. 对闭包做稳定拓扑排序；注册顺序只作为同级稳定次序，不改变依赖关系。
6. 只校验和构造闭包所需配置与 backend。
7. 按拓扑顺序启动 CMod。
8. 运行项目 setup。
9. 启动选中服务。
10. 开放共享 RPC Host、控制面和 discovery running 状态。

停止顺序：

```text
停止外部准入
  -> Drain L4 服务
  -> 等待服务已接受工作完成
  -> Stop L4 服务
  -> 反向 Drain L3 CMod
  -> 取消并等待根 WaitGroupContext
  -> 反向 Stop L3 CMod并关闭 L1 client
```

`CModDrainer` 用于根 context 仍然有效时完成入口关闭、事务冲刷等收尾；`CModStopper` 用于根任务退出后释放连接、
listener 和只应关闭一次的物理资源。一个 CMod 可以只实现其中一个接口。

CMod 启动失败时，只逆序清理由本轮已经启动的 CMod。未知状态默认失败，不允许缺失 CMod 或未启动依赖被当作成功。

## 配置分层

一份 YAML 可以供多个物理实例和任意合法 `--services` 组合复用：

```yaml
app:
  id: txgame

storage:
  redis: ...
  mongodb: ...
  wal: ...

runtime:
  discovery: ...
  actor: ...
  health: ...
  delayTask: ...
  handoffRecovery: ...
  scheduler: ...
  txn: ...

services:
  agent:
    tcpListen: 127.0.0.1:9000
    webSocketListen: 127.0.0.1:9001
    maxFrameBytes: 1048576
    fetchBatch: 1

  msgq:
    maxBacklogMessages: 1024
    maxBacklogBytes: 8388608
    offlineTimeout: 60s
    sweepInterval: 2s
```

边界：

- `storage` 配置 L1 backend 参数。
- `runtime` 配置 L2 运行时与 L3 CMod 参数，不定义 CMod 依赖，也不使用实现类型名作为 YAML 节点。
- `services.<name>` 配置对应 L4 服务自身参数；节点存在不代表启动服务。
- `--services` 是唯一服务选择入口。
- RunOptions、实例 ID、endpoint、逻辑槽位和监听地址等部署身份继续来自命令行或环境。
- YAML `KnownFields(true)` 继续拦截未知字段。
- 全局语法和 `app.id` 在加载期校验；backend、模组和服务语义只在选中闭包内校验。
- 未选中服务的无效或缺失参数不阻止其他服务启动。
- 未被 CMod 闭包使用的 backend 不创建连接，也不要求对应外部服务可用。

内置 CMod 配置不再保留 `enabled`。CMod 是否启动只有一个事实来源：选中服务展开后的依赖闭包。例如
`runtime.actor.enabled`、`runtime.discovery.enabled`、`storage.wal.enabled` 与 `runtime.backgroundWorkers.enabled`
都应在迁移中删除；当依赖闭包包含对应模组时校验参数并启动，不包含时忽略其参数。条件部署使用服务的
`CModRequirementsProvider`，不能重新引入一组 YAML 启用开关。

同一进程选中多个服务时，CMod 依赖取并集，每个 CModID 只构造和启动一次。若两个服务确实需要参数不同的
同类 worker，它们应声明不同 CModID 和独立队列，而不是定义隐式覆盖优先级。

## Fusion handoff 恢复边界

Fusion 恢复属于 `CModFusionStorage` 的正确性组成部分，不提供 `enabled`：

- 所有装配 `CModFusionStorage` 的进程都会启动本进程定点重试循环。
- 保存时入队或确认失败继续向业务返回原始错误，并把 marker/token 登记到本进程去重表。
- 进程退出后本地表可以丢失，Redis 同槽 marker 仍是恢复依据。
- 启动全量遍历由 Redis lease 协调，避免批量启动实例重复扫描全部 slot。
- `CModDelayScheduler` 独立启动；它不控制 handoff 正确性保护是否存在。

删除 `runtime.backgroundWorkers.enabled`。Scheduler 是否启动只由选中服务是否需要 `CModDelayScheduler` 决定。

## 启动恢复 lease

当前 `delayTask.leaseTimeout` 只用于 Scheduler claim 后的任务租约，不能复用为启动恢复 lease。新增参数：

```yaml
runtime:
  handoffRecovery:
    startupLeaseTTL: 30s
    startupWaitTimeout: 2m
    maxPendingPerSlot: 1024
    retryInitialDelay: 1s
    retryMultiplier: 2
    retryMaxDelay: 1m
    retryJitter: 0.2
```

参数语义：

- `startupLeaseTTL`：持有实例失联后，其他实例多久可以接管；必须大于续租和单次 Redis 调用超时。
- `startupWaitTimeout`：实例等待本轮恢复完成的最长时间；超时后启动失败。
- `maxPendingPerSlot`：单个物理 slot 最多保留多少条未确认根记录，同时作为启动读取单 Hash 上限。
- `retry*`：本进程定点恢复失败后的有界指数退避；不再复用 Scheduler 参数。
- slot 每批读取数量、续租周期和等待抖动由框架根据 TTL 推导，首版不增加配置项。

lease 使用 Fusion delayed task 所在 Redis route 建立一个固定协调 key。Redis adapter 提供 token fencing 的最小
lease 边界；HandoffRecoverer 不直接拼接 Redis 命令。协议状态为 `running` 和 `complete`：

```text
实例 A: SET NX lease = running(token-A), TTL
实例 B..N: 读取 running，等待

实例 A:
  每 TTL/3 按 token 续租
  完整遍历 handoff slot
  CAS running(token-A) -> complete(token-A)，保留 TTL

实例 B..N:
  观察 complete 后跳过遍历并继续启动
```

失败规则：

- A 扫描失败：不得写入 `complete`，当前启动失败，并释放自己仍持有的 lease；释放失败由 TTL 兜底。
- A 崩溃：TTL 到期后一个等待实例重新竞争并完成扫描。
- A 续租失败或 token 已变化：立即取消扫描并返回错误，不能越权完成新持有者的 lease。
- B 等待超过 `startupWaitTimeout`：启动失败，不默认放行。
- Redis 返回未知状态：启动失败。
- 相同 marker 重复恢复仍由任务幂等键、revision 和 token fencing 保证安全；lease 失效最多造成重复扫描，不能造成脏写。

完成状态保留到 lease TTL 自然到期，用于合并同一批容器启动。TTL 到期后新启动实例会重新扫描，确保后续进程异常
遗留的 marker 仍有恢复机会。该窗口可能让极短时间内再次启动的实例跳过扫描，但运行进程的本地定点重试和业务错误
返回仍保持；Redis marker 不会丢失。

## 启动期可观测性

设计文档保留静态 Mermaid/文本图。`server.Start` 使用唯一的依赖图解析器完成闭包展开和稳定拓扑排序，并在启动成功后
仅记录一次解析后的 L4 服务集合与 CMod 顺序。日志不包含 endpoint URI、密码、token 或连接串。

CMod 是进程启动期静态能力，不提供生产期动态装载或远程依赖图查询。循环依赖、缺失模组和重复 ID 在启动阶段输出
完整依赖路径并使进程 fast-fail；修正部署或代码后重启即可，控制面不承担模组装载职责。

## 迁移范围

本次不保留旧 API 兼容层，按以下方向一次收敛：

- 删除 `Capabilities uint32` 和全部 `Capability*` 常量。
- 删除 `requiredCapabilities`、`runtime.capabilities` 和位图缺失检查。
- `Service.Spec.Requires` 改为 `[]CModID`。
- 引入 CMod registry、依赖闭包、稳定拓扑排序和反向生命周期。
- 将现有 storage、RPC、Discovery、Actor、Transaction、WAL 与 Scheduler 装配为内置 CMod。
- 删除 `RuntimeConfig.BackgroundWorkers`、txgame `backgroundWorkers` YAML 和相关文档。
- 删除 Actor、Discovery、WAL 等内置 CMod 的 `enabled` 双重开关，改由 CMod 闭包唯一决定是否装配。
- 将 `runtime.agent`、`runtime.msgq` 迁移到 `services.agent`、`services.msgq`。
- 将 handoff 容量和重试配置迁移到 `runtime.handoffRecovery`。
- Scheduler 保留独立参数，但是否启动由 `CModDelayScheduler` 决定。
- 把依赖 L0-L4 图、服务模组矩阵、YAML 示例和 Handoff lease 时序同步到 README 与 `docs/design.md`。
- 将架构审计文档中的性能 `L0/L1/L2` 改为 `H0/H1/H2`。

## 验证

### CMod 图

- 单服务和组合服务得到正确依赖闭包。
- 共享 CMod 只启动一次。
- 缺失 CMod、重复 ID、空 ID 和循环依赖在外部连接创建前失败。
- 条件依赖只对选中服务求值，并且不能在求值期间创建资源。
- CMod 按稳定拓扑启动，按反向顺序停止。
- 某个 CMod 启动失败时只清理已经启动的前序 CMod。
- 项目自定义 CMod 可以被内置和项目服务依赖。

### 配置与服务选择

- `--services=agent` 不校验、不连接未使用的 Mongo、Fusion、Transaction 和 Scheduler。
- `--services=msgq` 只装配 Redis 与 RPC 相关闭包。
- `--services=game` 装配 Fusion、Transaction、Scheduler 及其依赖。
- `--services=game,msgq` 对共享 Redis/RPC 去重，只创建一个 Fusion Scheduler。
- 未选中服务参数无效不阻止启动；选中后返回包含完整 YAML 路径的错误。
- 未知 YAML 字段始终在加载期失败。

### Handoff lease

- 十个并发实例中只有一个执行 slot 遍历，其他实例观察 complete 后继续。
- 持有者失败、崩溃或 lease 过期后，等待实例可以接管。
- token 变化后旧持有者不能续租、完成或释放新 lease。
- 等待超时和未知 Redis 返回值都会阻止启动。
- 启动恢复完成后 Scheduler 才能启动。
- 运行期定点重试不遍历 slot，失败持续按封顶退避重试。
- Redis `127.0.0.1:6379` 真实链路覆盖竞争、续租、完成、接管和 token fencing。

### 回归

- tx、txgen、txgame 全量测试通过。
- txgame 真实 Redis/Mongo Fusion 保存与延迟回写通过。
- `go test -race` 覆盖模组图并发只读、Handoff lease 续租和服务停止。
- 启动前后资源数测试证明未选中 backend 没有建立 client。

## 非目标

- 不把 Redis、Mongo、HandoffRecoverer 等内部对象直接暴露给业务 handler。
- 不允许 YAML 定义或覆盖模组依赖图。
- 不实现跨数据中心 Redlock；启动恢复只使用当前 Fusion 已依赖的 Redis 路由。
- 不把 Scheduler、HandoffRecoverer 或 Transaction 作为 `--services` 可选名称。
- 不在本次实现 room、battlefield、delay、task、pay 的具体业务服务。
- 不为旧 `Capabilities`、`backgroundWorkers` 或旧 YAML 路径保留兼容解析。
