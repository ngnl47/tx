# tx 设计文档

本文面向 `tx` 的维护者、adapter 作者、代码生成器维护者，以及需要扩展底层能力的高级使用者。它解释框架为什么这样拆分、各模块内部如何协作、哪些状态可以共享、哪些边界必须失败，以及性能优化应以什么证据推进。

普通业务开发者应先阅读仓库根目录的 [README](../README.md)。README 是公共行为和使用方式的事实来源；本文是内部结构和技术选型的事实来源。当前进度见 [实施计划](IMPLEMENTATION_PLAN.md)，质量要求见 [架构审计标准](architecture_audit_standard.md)，某次审计的数字和证据见 `docs/audits/`。

本文只维护当前采用的结构、职责、稳定契约及其设计理由，不记录实施顺序或阶段进度，不保留已淘汰方案的使用说明。
用于解释技术选择的 benchmark 对照可以保留；具体验收日期、临时产物路径和未完成任务分别归入验收报告与实施计划。

## 阅读路径

本文按“系统职责 -> 数据内核 -> 存储 -> 单机事务 -> 分布式模块 -> 性能与维护”逐步深入：

1. 初次维护 `tx`：阅读“设计目标”“系统边界”和“依赖方向”。
2. 修改 GD、Store 或生成契约：阅读“数据对象与 Store”“生成式变更内核”。
3. 修改 Redis、Mongo 或 Fusion：阅读“存储抽象”和“存储后端”。
4. 修改 Procedure 或有状态运行时：阅读“单机事务”和“事务精确 Key 锁注册表”。
5. 修改服务拓扑：阅读“生命周期”和“分布式服务模块”。
6. 优化热点：最后阅读“性能分级与验证”，不要仅凭结构体外观实施优化。

每个核心章节尽量使用相同顺序：

1. 模块负责什么。
2. 模块明确不负责什么。
3. 对外暴露什么契约。
4. 内部由哪些组件组成。
5. 一次完整数据流如何运行。
6. 并发、错误与生命周期如何处理。
7. 为什么选择当前实现。

## 设计目标

正式宿主的交接准备由 `Server.PrepareHandoff` 复用关闭前半段：停止准入、等待已接受工作和业务任务、按组件逆序冲刷，成功后保留通信和租约。最终 Stop 才释放物理资源；两者串行共享已有组件收尾进度。控制面的 StateDrained 仅表示入口排水完成，不能单独作为槽位晋升许可。Actor 运行时替换仍按 [归属接线计划](superpowers/plans/2026-09-13-actor-ownership-integration.md) 分节点推进。

`tx` 的最高优先级不是“所有路径都最快”，而是以下顺序：

1. 不产生脏数据，不把失败伪装成成功。
2. 业务 API 清晰、简洁、强类型，框架可以代理的细节保持黑盒。
3. 数据内核与存储、网络、并发调度保持单向依赖。
4. 高频路径以 benchmark、race、逃逸分析或 profile 为依据优化。
5. 低频路径优先正确性和可维护性。

由此得到几条贯穿全部模块的规则：

- 未识别状态默认失败，不默认成功。
- 存储成功前不能清理 Store 变更状态。
- 外部服务波动返回 `error`，不能让业务进程 panic。
- 编程契约错误应在生成期或开发期暴露，不能在运行时猜测修复。
- 纯算法层不感知 Redis、Mongo、gRPC、Actor 或业务项目。
- 跨 goroutine 共享数据时，必须明确复制或所有权转移规则。
- 新抽象只有在减少真实重复、稳定职责边界或降低调用复杂度时才引入。

### 命名规则

核心类型和高频状态必须让字段名与类型语义互相印证：

- `Kind` 表示互斥分类，例如 `FieldNameKind`、`KeyKind`；它不表达一次变更动作。
- `Op` 表示一条变更的操作语义，例如 `OpSet`、`OpReplace`、`OpDelete`、`OpClear`。
- `ops` 保存 `Op`；`dirty` 只用于布尔脏状态、脏子节点索引或脏通知。
- 同步原语以职责命名。事务锁条目使用 `owner`、`waiters` 和 `signal` 表达所有权、等待队列与唤醒职责，调用方不感知内部互斥实现。
- 时间优先使用 `time.Duration` 固化单位；容量和字节配置在名字中写清口径，例如 `MaxBacklogMessages`、`HeapSoftLimitBytes`。
- JSON、BSON、Msgpack 属于字段命名体系；Redis、Mongo 属于使用这些名称的存储边界，不为存储边界重复生成字段名身份。

公开名称一旦让调用者误解职责，应直接修正并同步生成代码和文档；当前工程不为未发布的中间名称保留兼容别名。

## 系统边界

### Core 的物理边界

Core 是 `tx` 中 L0-L2 公共基础包的架构集合，不对应单独的 `core` Go 包。Go 包按单一职责拆分；维护者和高级使用者
根据需要直接依赖 `change`、`component`、`storage` 或 `modules/*` 等稳定契约，普通项目则只使用 Bee 或 Ant
门面。框架不提供大而全的 `core.Server`、`core.Context` 或 `core.Runtime`，避免把数据、存储、网络和事务重新耦合到
一个万能命名空间。

当前 `tx` 运行时相关包结构如下：

```text
tx/
├── bee/                    # 无状态游戏后端的 L3 门面、CMod 和标准 Service Registration
├── ant/                    # 有状态游戏后端的 L3 门面、CModProcedure 和标准 Service Registration
├── internal/framework/     # Bee/Ant 共享的私有进程装配内核，业务与外部库不可导入
├── component/              # 中立组件契约、依赖图和生命周期宿主
├── change/                 # Store 状态树、StoreView 和变更文档投影
├── lifecycle/              # WaitGroupContext、任务子生命周期和入口准入契约
├── meta/                   # 生成类型、字段、存储与索引元数据
├── protocol/               # 网络帧与 BON 等协议基础能力
├── storage/                # 通用存储契约、Redis、Mongo、Fusion 和延迟任务实现
├── etcd/                   # 通用 etcd 客户端、租约、原子键、快照、Watch 与 CAS 适配
├── modules/
│   ├── actor/              # Actor Runtime、寻址和消息投递
│   ├── agent/              # 长连接、帧处理和会话入口组件
│   ├── writeback/          # 有状态根对象登记与分帧刷盘组件
│   ├── control/            # 节点状态和逻辑槽位控制面
│   ├── discovery/          # etcd 服务发现与本地快照
│   ├── msgq/               # 会话邮箱及其 RPC、Redis、Discovery 适配组件
│   ├── rpc/                # 共享 gRPC Host 和调用基础能力
│   └── txn/                # Procedure、锁、undo、savepoint 和死锁检测
├── anno/                   # 生成注解公共定义
├── astscan/                # Go AST 扫描辅助能力
├── cmd/                    # tx 自带维护命令
├── log/                    # 框架日志契约
└── util/                   # 面向业务的独立工具组件
```

其中，`component`、`change`、`lifecycle`、`meta`、`protocol`、`storage` 和 `modules/*` 等共同构成
Core。`internal/framework` 使用这些包实现进程装配、能力图解析和生命周期编排，但它是私有实现，不是第四个公共门面。
Bee 与 Ant 只负责把 Core 组件组合为两种运行模型；它们不能互相依赖，也不能被 Core 反向依赖。

调用方按下面的边界选择入口：

```text
普通无状态项目      -> bee
普通有状态项目      -> ant
高级组件或 adapter  -> 按职责直接使用 component / storage / modules/*
生成代码            -> 只依赖 change / meta / storage 等中立 Core 契约
```

### 四个工程的职责

```text
txgen 输入 Go 源文件
        |
        v
txgen 编译期扫描和生成
        |
        v
txgame 或 txgameslg 的 server/gen/data 与 gen/cmd
        |
        v
tx runtime 契约与 adapter
        |
        v
Redis / Mongo / etcd / gRPC / Actor
```

`tx`：

- 定义元数据、变更追踪、存储抽象、adapter 和可选运行时模块。
- 不读取业务工程 yml，不扫描业务 Go 源文件，不依赖 `txgen`。
- 不生成业务 facade，不决定业务对象如何创建或初始化。

`txgen`：

- 扫描 type 注解和 field struct tag。
- 校验能在编译前确定的冲突。
- 生成 GD、Store、字段 ID、字段名 resolver、存储定位和 cmd 胶水代码。
- 只依赖 `tx` 的稳定契约，不能要求 `tx` 反向认识生成器。

`txgame`：

- 是无状态游戏服务的生产工程组织方式和集成验证模板。
- 读取 yml、环境变量和命令行参数，构造具体 client 与实例配置。
- 提供 `db.Save(role)` 之类业务 facade。
- 以 `services/*` 组织 L4 微服务；服务专属 Handler、路由、Actor 和业务模块随所属 Service 放置。
- `server/internal` 只保存进程级组合根、配置转换和基础设施 facade，不作为业务代码汇总包。

`txgameslg`：

- 是有状态 SLG/MMO 服务的独立生产模板，不依赖 `txgame`。
- 使用 Actor 作为运行期权威对象的唯一串行命令入口，使用 Procedure 和 WriteBackManager 管理单机事务与分帧保存；对象的实际缓存结构由项目 SPI 决定。
- `slggame` 通过 Fusion 保存角色、军团等根对象；`slgworld` 通过 带所有权保护的 Mongo 直接写回 保存分区场景数据。
- 只依赖 `tx`、`txgen` 及自身生成产物；配置、服务组合、业务 SPI 和测试均在本项目闭环。

客户端协议工程与服务模板固定一一配对：

- `txgame/gen` 生成 `txgame/server/gen` 和 `txclient/src/gen`；`txclient` 只验证无状态模板。
- `txgameslg/gen` 生成 `txgameslg/server/gen` 和 `txclientslg/src/gen`；`txclientslg` 只验证有状态模板。
- 两个客户端独立长期维护，不共享源码目录、符号链接或生成清单。公共 BON、Frame、Session、Seq/Ack 语义变化时分别回归。

多进程真实集群是项目层验收，不改变依赖方向：`txgame + txclient` 与 `txgameslg + txclientslg` 是并行能力，允许在本地顺序执行并复用入口端口；Redis 通过不同 logical database 隔离，MongoDB 使用不同 database，etcd 使用不同 prefix。每套验收同时运行两个 MsgQ，使用新会话验证初始分流、单实例故障承接和重启后恢复分流；证据按 `<prefix>:session:v2:{SessionID}` 读取完整 binding，并核对其中的 RoleID、SessionID、ConnectionID 与实例身份。Topic 仍只属于单个 owner MsgQ，跨实例覆盖由业务以 SessionID 为锚点分别发起本地广播。`tx` 不为隔离统一改写业务 key，也不保存项目服务名。

两个模板的业务模块按稳定能力、领域实现和事务编排分层：`modules/*_service.go` 使用包级函数提供 Handler 与 Procedure
共用的业务能力，`modules/internal/<domain>` 保存具体领域实现，`services/<name>/internal/procedure/P*.go` 负责编排
所属 L4 Service 的本机事务。业务能力不持有无状态 Service 实例，不判断当前事务层级，也不负责最终刷盘。

Procedure 单向依赖并组合 `modules` 的稳定业务能力，`modules` 禁止反向依赖 Procedure。Procedure 放在 Service 的
`internal` 目录而不是 `modules/internal`：这样所属 Service 的 Handler 可以启动根 Procedure，其他 L4 Service 又不能
越界调用。Go 的 `internal` 规则同时禁止 Handler 绕过稳定能力直接依赖 `modules/internal` 领域实现。

无状态 Handler 直接调用 `modules` 并显式保存根 GD；有状态 Handler 使用 `txn.Executor.Call` 启动根 Procedure。
父 Procedure 需要复用另一事务流程时，使用当前 `*txn.Tx.Call` 创建子 savepoint。每个 Procedure 只声明自己直接访问的锁，
子调用可以在运行时增加锁；错误的嵌套根调用仍会直接返回错误。该依赖方向采用组合式事务能力，但不依赖线程局部状态猜测根调用与子调用。

跨 L4 Service 调用必须经过 Actor/RPC client，并在目标服务内执行自己的 Procedure；即使多个 Service 聚合在同一进程，
也不扩大单机事务边界。运行模型属于整个 Service，进程启动后不能按请求切换。

### 示例工程的运行配置组织

YAML 属于 txgame/txgameslg 的组合根，tx 只接收类型化配置，不读取文件路径或解释锚点。
每个 debug/test/release 环境独立维护 `app.yml` 与 `runtime.yml`，允许策略冗余和环境定制，不建立公共默认值继承层。
`app.yml` 声明部署值并 include 同目录 `runtime.yml`；先展开文件，再解析 YAML 锚点，引用单向流入运行策略。

| runtime 分组 | 归属与作用 |
| --- | --- |
| `lifecycle`、`log` | 进程排水与日志，不归属单个 CMod |
| `storageHealth` | Redis/Mongo 后端共用探针，不是服务发现或玩家心跳 |
| `cmods.actor` | CModActor：实体邮箱、生命周期与 Core NATS 通信 |
| `cmods.discovery` | CModDiscovery：etcd 服务注册、成员查询与租约 |
| `cmods.taskQueue` | CModTaskQueue：预部署 JetStream 资源绑定 |
| `cmods.fusionStorage` | CModFusionStorage：热数据 TTL、内部延迟任务及调度器 |
| `cmods.procedure` | CModProcedure：事务与根对象写回，仅有状态示例使用 |

组件内部使用短字段名，由配置层级表达职责；跨文件锚点携带组件/职责前缀和必要单位。
配置项不承担能力选择职责，实际装配仍取决于服务 Requires 闭包；可靠任务的非空绑定由示例组合根转换为能力声明。
字段取值、默认值与 YAML 示例分别维护在 [txgame 配置参考](../../txgame/README.md#yaml-配置参考) 和 [txgameslg 配置参考](../../txgameslg/README.md#yaml-配置参考)，此处不重复维护字段表。

### API 分层

同一个能力可能有三层入口：

| 层级 | 面向对象 | 示例 | 约束 |
| --- | --- | --- | --- |
| 业务层 | handler 和业务模块 | `db.Save(role)` | 不暴露 writer、Store、RecordKey 和根 context |
| 扩展层 | 项目装配和 adapter | `storage.Save(ctx, writer, role.Store())` | 显式表达存储依赖和生命周期 |
| 内核层 | 框架维护者和高级扩展 | `VisitChanges`、`txn.WithLocks` | 调用方必须理解所有权和并发契约 |

新增 API 时先判断它属于哪一层。不能因为测试方便，把内核对象泄露到业务层；也不能为了隐藏一切，阻断高级使用者实现 adapter 的可能。

### Bee 与 Ant 运行时门面

`bee` 是无状态服务公开门面，`ant` 是有状态服务公开门面。两者共同持有 Builder、Server、ServiceRegistration、
ServiceSpec、CMod 和基础 Context；同名 API 具有相同签名和生命周期语义。Ant 只增加 CModProcedure、固定逻辑槽位、
缓存，不改变 Bee 已有调用方式。这个对称契约由 `tests/facade_symmetry_test.go` 在编译期和运行期锁定。

公开 Context 是受控能力视图，不是 Runtime 对象的字段集合：

| Context 能力 | 适用门面 | 服务可做什么 | owner 权限仍在哪里 |
| --- | --- | --- | --- |
| `Tasks()` | Bee/Ant | 注册本服务后台任务、读取取消信号 | 每个 Service 独立子生命周期，`Cancel/Wait` 在私有装配内核 |
| `Admission()` | Bee/Ant | 登记和封口本服务外部入口 | 进程排水顺序在 Service Host |
| `Storage()` | Bee/Ant | 仅供项目组合根安装 mode-aware 业务 facade | client、健康状态、worker 和 Close 在存储 Runtime |
| `CModRedisStorage()` | Bee/Ant | 取得仅允许 Redis TypeSpec 的 reader/writer 和脚本路由 | 连接池所有权在存储 Runtime |
| `CModMongoStorage()` | Bee/Ant | 取得仅允许 MongoDB TypeSpec 的 reader/writer、条件回填和原子 CounterStore | 连接池所有权在存储 Runtime |
| `CModFusionStorage()` | Bee/Ant | 取得仅允许 Fusion TypeSpec 的 reader/writer | Redis、Mongo 和延迟任务所有权在存储 Runtime |
| `CModRPC()` | Bee/Ant | 启动前注册 protobuf service | Serve/Stop 在共享 RPC Host |
| `CModDiscovery()` | Bee/Ant | 查询 running 或指定实例快照 | watch 与 Replace 在 discovery Runtime |
| `CModSessionID()` | Bee/Ant | 为 Login 分配集群内有效生命周期的 SessionID | workSlot、租约与停止发号在 discovery/session-id Runtime |
| `CModActor()` | Bee/Ant | 解析 PID、投递和业务调用 | Start/Shutdown/ActorSystem 在 Actor Runtime |
| `CModProcedure()` | Ant | 执行根 Procedure | CommitParticipant 和生命周期在 CModProcedure |
| `CModSlots()` | Ant | 只读 SlotReader 槽位归属 | etcd client、成员 lease 和 watch 在 discovery Runtime |
| `CModSlotResolver()` | Ant | 解析槽位当前 running 目标 | Watch 和连接生命周期在 discovery Runtime |

Context 对生命周期、目录替换和刷盘能力返回专用 wrapper，调用方不能通过这些视图重新取得
`Cancel`、`Stop`、`Shutdown`、`Replace` 或 `Flush`。槽位读取另以 SlotReader 声明编译期职责，不承诺类型断言隔离。`Context.Instance()` 返回 ownership-safe 的本地实例快照，
状态顺序固定为 `starting -> running -> draining -> stopping`。

普通 Handler 不应保存 Bee/Ant Context。项目组合根通过 `Builder.Setup` 把 `Storage()` 包装成 `db.Save(role)` 等业务
facade；Service 和 CMod Factory 只在构造期间取得自己通过 `Requires` 声明的窄能力。Redis、MongoDB、Fusion 三类
存储 accessor 的 reader 方法集互不重叠，writer 也会在第一次 I/O 前校验 `TypeSpec.Storage.Mode`，不能借进程内其他
Service 已选择的后端跨越能力边界。高级使用者需要 Redis 脚本时使用 `CModRedisStorage()`；
槽位创建和晋升由组合根显式注入完整 `SlotStore`，不从只读 `CModSlots()` 断言取得写能力，
也不通过字符串 service locator 或全局 Runtime 查找能力。

Context 的 `Storage/CMod*` 直接返回能力；声明检查和不可用检查由 tx 维护。公开视图使用内部 CapabilityError 标记能力错误，Service 构造/启动、Setup、CMod Validate/Start 的同步边界仅恢复该标记并返回启动 error，复用既有回滚。普通业务 panic 原样传播，不增加全局 recover 或重试。自定义 CMod Validate 只校验配置，读取运行时能力会立即失败；Start 才读取已就绪前置能力。Context 不用于后台任务或装配回调以外的能力查找；工厂尚未交接的私有资源由其 defer 清理。

Ant 的 CModSlotResolver 在构造期组合已有 SlotReader 与 Directory，业务保存解析器即可。SlotResolver 使用现有 Watch 缓存读取算法，高级 SlotStore/SlotRouter 和只读 SlotReader SPI 保留；不在消息热路径创建解析器。

## 依赖方向

核心依赖保持由上到下单向：

```text
anno / astscan
       |
       v
      meta
       |
       v
change <------ generated Store
  |
  v
storage contracts
  |
  +--------+---------+
  v        v         v
redis    mongo     fusion

optional modules:
lifecycle <- discovery / rpc / msgq / agent / actor / control / writeback manager
change <- txn runtime <- writeback manager -> storage contracts

process assembly:
L0-L2 components -> internal/framework private kernel -> bee / ant -> project L4 services
```

关键约束：

- `change` 不能依赖 `storage`，否则纯变更算法会被具体 I/O 语义污染。
- `storage` 通用契约不能依赖 Redis 或 Mongo adapter。
- `modules/actor` 不代表 GD，不依赖 Store、Fusion 或 `txn.Runtime`。
- `modules/txn` 只依赖 `change`，不持有 writer 或具体 backend。
- `modules/writeback` 依赖 `txn` 提交 SPI 和通用 `storage` 契约，不依赖 Redis、Mongo 或 Fusion 实现。
- 项目业务可以组合 Actor、Procedure 和存储 runtime，模块之间不反向耦合。
- `internal/framework` 可以编排中立组件，但不能成为项目可 import 的公共 API。
- Bee 与 Ant 不能互相 import；Core 和私有装配内核不能反向 import Bee 或 Ant。

全局 registry、回调和 `any` 也可能形成隐式反向依赖。审查依赖时不能只看 import cycle。

### 模块组合规则

维护时按以下职责层次判断新增代码放置位置：

```text
change/meta/storage contracts
  -> storage backend 与 lifecycle/discovery/rpc/msgq/actor/txn 基础模块
  -> 中立 runtime、adapter 与标准 agent/msgq service 组件
  -> internal/framework 私有装配内核
  -> bee / ant 公开门面与各自 CMod
  -> txgame / txgameslg 配置转换、项目服务和业务 facade
```

- 基础模块声明最小消费契约。路由器依赖 `discovery.Directory`，不能依赖具体
  `*ServiceDirectory`；服务注册只依赖 `rpc.Registrar`，不能取得 `*rpc.Host`。
- 跨能力胶水必须有中立归属。例如 `modules/msgq/msgqredis` 只把
  `redis.ScriptRoutes` 适配成 `msgq.SessionStore`，不启动 msgq，也不拥有 Redis client。
- 标准服务组件只负责本服务的配置、协议注册和资源启停。`agent` 不导入 msgq 门面；同进程复用通过
  `LocalMsgQ` 契约，跨进程会话解析通过 `modules/msgq` 与中立 Redis adapter 完成。
- 私有装配内核可以实例化和编排下层模块，但下层模块不能反向访问其内部 Runtime。
- 测试 fake 就近放在目标 package 的测试文件或内部测试辅助包，不能为了测试构造任意 Runtime 而扩大生产 API。

### L0-L4 与能力模组

运行时使用下面的固定层级：

| 层级 | 职责 | 示例 |
| --- | --- | --- |
| L0 | 纯算法、基础结构和稳定契约 | `change`、`meta`、`lifecycle`、storage 契约 |
| L1 | 外部 backend 与传输 adapter | Redis、Mongo、gRPC、`tx/etcd` adapter |
| L2 | 可复用运行时组件 | Fusion Strategy、RPC Host、Scheduler、Transaction Runtime、Actor Runtime |
| L3 | 进程能力门面 | Bee/Ant CMod、受控 Context、由私有内核执行的依赖图和进程生命周期 |
| L4 | 可由 `--services` 选择的服务 | agent、msgq、game、room、battlefield |

CMod 固定属于 L3，是带稳定身份、依赖关系和进程生命周期的能力门面。L0-L2 的算法、backend、Runtime 和 Scheduler
只能作为强类型组件被 CMod Builder 组合，不拥有全局 `CModID`。L4 服务只声明直接 `CModID`，Bee/Ant 复用私有内核中的同一个解析器
得到依赖闭包、稳定拓扑顺序、循环路径和缺失依赖。`Requires` 会选择依赖，`After` 只对已经选中的门面增加顺序边。
共有内置 CMod 的完整依赖图及接入示例见 [README：CMod 依赖与服务能力声明](../README.md#cmod-依赖与服务能力声明)。这里区分两个边界：

- **运行依赖**：`CModSpec.Requires` 由能力实现方声明，负责自动启用、启动顺序及反向关闭。`After` 不扩展能力选择。
- **直接使用依赖**：`ServiceSpec.Requires` 由业务 Registration 声明，同时决定该服务可从 Context 取得哪些能力。间接依赖即使已启动，也不自动成为服务可读取的能力；例如 Actor 自动启用 Discovery，但服务自行查询目录仍须声明 Discovery。

保留这两层边界，是为了让 CMod 的内部实现可以调整，而不让业务意外依赖其传递依赖。服务声明不需要复制整张依赖图；同一服务直接使用 Actor 和 Discovery 时，两者同时出现是对真实调用关系的声明。其他服务启用更多能力，也不能扩大当前服务的访问范围。

声明 ActorKinds 会隐式启用 Actor 及其运行依赖，服务自身要调用 Actor 门面仍须显式声明 Actor。所有所选服务共享按 CModID 去重后的进程能力；Registration 列表顺序不能替代拓扑关系。缺失依赖、重复身份和依赖环由装配内核统一报错，不由业务补救。

门面内部的组件组合保持如下边界：

```text
fusion-storage components: scheduler=delay
procedure components: transaction=default, writeback=<project>
```

`procedure` 是项目注册的 L3 CMod。Transaction 与 WriteBack 由它统一校验、启动、排水和停止。`fusion-storage` 统一拥有 Delay Scheduler，服务不再分别选择 producer 与 consumer。
启动日志保留 CMod 拓扑顺序，同时输出过滤后的组件拓扑，便于核对当前进程实际创建了哪些组件。

Actor CMod 依赖共享 RPC 与 Discovery；业务声明 Kind 后自动纳入依赖闭包，复用同一实例身份、槽位 Watch 和生命周期。NATS 只承担传输，不另建成员表。

组件职责仍保持正交。Transaction 只负责锁、undo、savepoint、死锁检测和提交参与者调度；WriteBack 只登记根 Store 并决定
何时 Flush；Fusion 与 Mongo 是可选目标 Writer。正交不等于平级暴露：
CModProcedure 负责把这些组件按明确顺序组合为一个进程能力门面。

典型 L4 组合与调度边界如下：

| 服务形态 | 显式 CMod | 运行期调度 |
| --- | --- | --- |
| 无状态请求服务 | RPC、Fusion | Handler 修改 GD 后显式保存；Fusion 内部运行 Delay Scheduler |
| 有状态逻辑服 | RPC、Fusion、CModProcedure | Procedure 只提交内存；CModProcedure 的 WriteBack 组件分帧写 Fusion |
| 有状态大世界 | RPC、Mongo、CModProcedure | WriteBack 直接增量写 Mongo，成功后清除脏状态 |

CModProcedure Builder 只组合组件，不隐式选择外部存储 CMod。使用 Mongo Writer 的服务仍须在自身 `Requires` 中显式声明
Mongo；使用 Fusion Writer 的服务显式声明 Fusion。这样配置关系在 L4 仍然可见，组件生命周期又不会泄漏为一组平级 CMod。

项目组合根负责创建唯一的 CModProcedure 实例：

```go
writeBack, err := writeback.NewComponent(config.WriteBackManagerConfig)
if err != nil {
	return nil, err
}
procedure, err := ant.NewCModProcedureBuilder().
	WriteBack(writeBack).
	Build()
if err != nil {
	return nil, err
}

return ant.New(config).
	CMods(procedure).
	Services(slggame, slgworld).
	Build()
```

WriteBack 接收 `CModProcedureWriteBack` SPI。组件只能看到 Tasks、Storage、目标感知 WriteAdmission、Transaction 和提交参与者绑定接口，不能取得 Server 所有权。
启动时先创建 Transaction，再启动 WriteBack。排水时先完成 WriteBack 的最终 Flush，再停止 Mongo/Fusion 等依赖。

启动固定执行“选择 L4 服务 -> 解析 CMod 闭包 -> 惰性转换闭包配置 -> 校验全部 CMod -> 按拓扑启动”。全部
校验通过前不能建立连接、监听端口或启动 goroutine。停止时先排水和停止 L4，再反向排水 CMod，取消并等待根
`WaitGroupContext`，最后反向关闭 CMod 持有的物理资源。

CMod 或 L4 Service 的 `Start` 允许分阶段创建连接、监听器和后台任务，因此实现 `Stop` 时必须保证幂等，并能清理
部分启动状态。每个 CMod 的 `Tasks()` 指向独立子 `WaitGroupContext`：某个单元的 `Start` 返回错误后，Host 先取消并等待
该单元登记的后台任务，再调用失败单元的 `Stop`；随后按逆拓扑顺序以相同流程停止此前成功启动的单元。这样 Redis、Mongo
等依赖不会在使用它们的 worker 仍运行时被提前关闭。启动错误与所有清理错误通过 `errors.Join` 一并返回。

Bee/Ant Server 使用 `starting -> running -> stopping -> stopped` 单向状态机。Context 只传递给启动过程中的 Setup 和
Factory，不提供可在业务运行期任意查询的全局入口；并发 `Stop` 必须等待 `Start` 完成后再关闭已经创建的全部资源，不能返回成功后留下继续运行的进程。
正常停止时先执行 CMod Drain，随后取消并等待根及全部子生命周期，最后才调用 CMod Stop 释放物理依赖。

`CModConfigProvider` 只在依赖图和 L4 服务集合确定后运行，接收只读 `AssemblySelection`。它解决同一份 YAML 被
agent、msgq、game 等不同进程复用时的配置边界：未选择 CModProcedure 时不解析事务时长，未选择 Actor 时不要求 Actor
host，未选择 Mongo 时不验证或连接 Mongo。
未知 YAML 字段仍由项目解析器在加载期拒绝。CMod ID 只用于依赖解析和启动日志；业务调用使用构造函数强类型注入或
Bee/Ant Context 的稳定窄接口，不提供按字符串取 `any` 的 service locator。

`CModContext.HasCMod` 查询本轮已经解析完成的完整选择闭包，不是“此前已经启动”的瞬时状态。因此所有 Validator 都能在
创建连接之前按组合条件预检项目配置；Start 仍只能使用拓扑中已经位于自己之前的实际运行时能力。

## 元数据与代码生成边界

### 元数据链路

唯一输入链路是：

```text
Go 注释 / struct tag
        |
        v
anno 解析单条注解
        |
        v
astscan 绑定 package/type/field
        |
        v
meta 规范化和校验
        |
        v
txgen 模板生成
```

`anno` 不猜注解属于哪个类型；绑定关系必须由 AST 确定。`meta` 表达的是生成器和 runtime 共同理解的稳定语义，例如存储模式、路由字段、字段 ID 和多序列化名称。

`meta.Normalize` 是注解字符串进入强类型元数据的唯一转换边界。duration、bool 和 Mongo 数值参数必须在这里
严格解析并携带属性名返回错误，不能把非法输入静默转换为 `0` 或 `false`。纯整数 duration 按秒兼容既有输入，
但使用 `time.ParseDuration` 完成范围检查，避免整数乘法溢出产生错误生命周期。

紧凑注解参数必须保留声明顺序、重复项以及参数和裸 flag 的区别，不能先压入 `map[string]string` 再规范化。
Mongo 命名复合索引依赖同一个 `index:<name>` 在多个字段上重复出现，单个字段也可以同时声明命名索引和匿名
索引。例如 `Platform mongodb:"index:p_id"` 与 `PlatformUId mongodb:"index:p_id,index"` 共同声明
`p_id=(Platform, PlatformUId)`，并为 `PlatformUId` 额外声明一个匿名单字段索引。注解解析层只保存有序参数，
`meta.Normalize` 负责将其转换成强类型索引元数据。

`meta.Validate` 是存储元数据的唯一共享校验入口。除校验 `idField`、`routeBy` 的 Go 字段身份和类型外，它还
要求 Redis 的 `routeType/key` 与 Mongo 的 `routeType/collection` 是非空且没有首尾空白的原始文本。这些值
直接参与路由表、记录 key 和 collection 定位，校验层不能补默认值或静默去空白；txgen 和高级手工元数据调用方
都必须复用该入口并得到相同的失败语义，避免配置错误延迟到首次外部 I/O。通用 `meta.Registry` 允许保存只供独立模块
读取的部分元数据，因此不隐式调用完整存储校验；手工构造完整存储规范的高级调用方必须显式调用 `meta.Validate`。

存储模式同时决定允许存在的 type 级 backend 元数据：Redis、Mongo 模式只能携带各自的配置，Fusion 必须同时
携带两者。不能保留一份运行时不会消费的额外 backend 配置，否则生成器、RecordKey 解析和维护者会面对多套互相
矛盾的定位信息。字段 JSON/BSON/MessagePack 名称不属于 backend 启用开关，不受这项排他规则影响。

### 字段名体系

字段名本体是：

- Go 字段名。
- JSON 字段名。
- BSON 字段名。
- MessagePack 字段名。

Redis 和 Mongo 是使用这些名称的后端，不是额外的字段名体系：

- Redis JSON 文档使用 JSON 名。
- Mongo 完整编解码使用 BSON tag。
- cmd 下行协议使用 MessagePack 名。
- Store API 和 `LoadFields` 输入使用 Go 字段名。

生成输入采用统一的 ASCII 标识符契约：显式 Alias、JSON、BSON 和 Msgpack 名必须匹配
`[A-Za-z_][A-Za-z0-9_]*`。普通字段只接受 `omitempty`，匿名嵌入字段只接受 BSON/Msgpack 的 `inline`。
框架不为特殊静态字段名维护 TypeScript 引号属性、数据库名称转义或多边界兼容分支；txgen 在生成前失败。
运行期 `map[string]` 的动态 key 不属于这条规则，Mongo 边界仍按既定策略在不安全 key 上降级为整字段写。

部分读取是严格的元数据契约。`storage.ValidateLoadFields` 使用 `TypeSpec.Fields` 校验所有请求字段；
生成 helper、Redis/Mongo/Fusion adapter 与 cluster 路由都必须在自定义 loader、选路和外部 I/O 前完成校验。
未知字段统一返回 `storage.ErrUnknownField`，不能把只返回记录键或空字段投影解释为读取成功。空字段列表表示完整读取；
重复的合法字段不改变语义，可由具体读取器去重。该校验针对输入侧 Go 字段身份，读取结果仍按具体边界使用
JSON 或 BSON 名称。

生成 resolver 负责 `FieldID -> go/json/bson/msgpack name` 的静态映射。Redis、Mongo、Provider 在各自边界选择对应命名体系，运行时不通过反射重新推导生成期已经知道的关系。

字段编号从 1 开始连续分配，因此生成代码用私有定长表保存 Go 名和 JSON 名，通过 FieldID 直接索引。业务侧需要把 Go 字段名传给 `LoadFields`、`FindOptions` 等接口，所以生成 `GD*Field<Field>` 公开常量；JSON、BSON、MessagePack 名只在序列化和存储边界使用，不额外导出逐字段常量。BSON、MessagePack 仅为与 JSON 不同的字段生成覆盖分支，例如 Mongo `_id` 或协议短名。该布局避免按每个字段展开四套重复 switch，同时保持无反射、零运行时字符串转换和零分配。

生成代码还会基于上述名称表和紧凑字段类型表构建一份包内私有的只读 `TypeSpec`。生成的 Load、Find、Delete、SaveChange 和类型注册路径直接复用该静态元数据，不在每次 I/O 前重新展开字段描述。公开的 `GD*TypeSpec()` 与 `Store.TypeSpec()` 则通过 `meta.CloneTypeSpec` 返回隔离副本，调用方修改字段切片、存储指针、字段配置 map 或 Mongo 索引时不会污染生成包的静态规范。

`CloneTypeSpec` 只负责深拷贝可变成员；`TypeRef` 的去空格和完整名称补齐属于 Registry 注册边界。所有接收私有静态 `TypeSpec` 的 tx 内部 adapter 必须把它视为只读输入，不能修改其中的 map、slice 或指针成员。

Mongo 索引元数据由 `storage/mongoindex` 解析为 driver 无关模型。该包只依赖 `meta`，负责 BSON 路径转换、命名索引合并、
稳定匿名名称以及 TTL/文本权重语义校验；txgen 在生成前调用它，Mongo runtime 在创建索引前调用同一实现。具体 Mongo
driver 只负责把已经验证的模型转换为 `mongo.IndexModel`，生成器不会因此依赖 Mongo driver 或文档 codec。

### 生成包基础层

txgen 在每个 data 输出包只生成一份私有基础层，当前与类型注册表一起放在 `type_registry.go`。它不是业务 API，也不改变 `GD*`、`GD*Store` 和 `GD*StoreView` 的公开签名；职责是承接所有类型都会重复、但不需要逐字段静态展开的实现：

- 集合操作状态的复制、恢复和单 key 回滚。
- 普通字段赋值与 undo 登记。
- 已加载字段 bitset 的查询、设置和字段名枚举。
- RecordKey 字段补齐检查，以及单条/批量读取后的 Store 装配流程。
- StoreView 的叶子状态、路径尾部和低频状态查询辅助逻辑。

具体 `GD*Store.go` 仍保留生成期已知类型的字段 switch、集合 key/index 校验、StoreView `record/copy/visit`、嵌套 `ResolveValue` 和子 Store 路由。这些路径位于 setter、捕获、合并或文档遍历的高频调用链，静态生成能避免反射、`any` 断言和回调分发，并给编译器更多内联机会。顶层 `ResolveValue` 仍先经过单个紧凑字段 switch；只有指针、map 和 slice 的嵌套路径按类型静态展开。维护者不能只为缩短生成文件而把热路径改成动态注册表；也不能把低频固定流程重新复制到每个 Store。生成体积和运行时 benchmark 必须一起作为模板重构门槛。

### FieldID 稳定性

`FieldID` 是运行时变更路径的字段身份，不是网络协议号。它由生成代码为每个结构定义并配套 resolver。维护时必须保证：

- 同一生成版本内唯一。
- 变更创建方和解析方使用同一份生成代码。
- 不把 FieldID 直接持久化成长期数据库协议。
- 跨版本持久化任务保存记录定位，不保存依赖旧 FieldID 的长期变更流。

## 数据对象与 Store

### GD 的职责

`GD*` 是纯业务数据结构：

```go
type GDRole struct {
    RoleId int64
    Name   string
    Items  map[int32]*GDItem
}
```

它可以被 JSON、BSON、MessagePack 编解码，可以直接读取 public field，也可以用于快照差分。GD 不负责：

- 选择 Redis 或 Mongo。
- 持有数据库 client。
- 自动提交事务。
- 在字段中暴露 `*GD*Store`。

生成的便捷 setter 可以代理默认主 Store，但这不改变 GD 的数据职责。

### Store 的职责

`GD*Store` 是可变的变更追踪结构。它绑定一个 GD 数据指针，但不保存第二份完整业务数据：

```text
GDRole <--------------------- main GDRoleStore
唯一可变业务数据                可变状态树
                                   |
                                   | View(configure) 同步捕获
                                   v
	                            GDRoleStoreView
	                            ├── 稀疏 GDRole 值
	                            └── txgen 生成的强类型视图状态树
```

Store 负责：

- 提供强类型 setter 和集合 wrapper。
- 使用生成期字段槽位和集合 key/index 状态记录变更语义。
- 为子 Store 组合父路径。
- 在输出边界读取当前 GD 值。
- 为事务绑定临时 undo recorder。
- 同步捕获强类型 StoreView 并构建下行文档。

Store 不负责：

- 保存业务对象的历史完整副本。
- 自动选择持久化时机。
- 作为 cmd 或 GD 字段类型发送给客户端。

### Store 状态树

嵌套对象和集合形成 Store 组合树：

```text
GDRoleStore
  |
  +-- ItemsStore(map key=1001)
          |
          +-- GDItemStore
                  |
                  +-- Quantity
```

叶子 Store 只保存自己的局部字段状态；集合 Store 保存发生变化的 key/index 及对应子 Store，父级通过 `notifyDirty` 只记录“这个分支需要访问”。调用根 Store 的 `VisitChanges(visitor)` 时，框架才递归访问已标脏分支、追加集合 key/index，并得到：

```text
Items[1001].Quantity
```

因此 Store 状态树确实分层保存局部变更状态，但不会在每次修改时遍历整棵树，也不会让根 Store 长期保存一份已经展开的全路径副本。所谓“冒泡”是脏通知：子节点通知父集合对应 key/index 已变更；真正的路径拼接发生在输出边界。根 Store 只递归访问脏子节点索引，使用 `JoinPathLimited` 组合路径。生成 Store 的状态在记录时已经按字段或 key 唯一化，不需要输出后再做第二次压缩。父子关联属于 Store 路径关系，不代表 GD 对象只能有一个父节点。

### 共享指针与按路径追踪

同一个 `*GDItem` 可以放入多个玩家或多个集合。业务数据允许共享引用，变更身份由 Store 路径决定：

```text
roleA.Items[1] ----+
                   +----> same *GDItem
roleB.Items[9] ----+
```

两个根 Store 会创建各自的子 Store 路径。通过 `roleA.ItemsStore().Get(1)` 得到的 Store 只向 roleA 根记录变化；不能让裸 `item.SetQuantity()` 自动猜测应该写入哪个根或哪个 view。

### 主 Store、selector 与 StoreView

这三个对象的职责刻意分开：

- 主 Store 是唯一可变追踪对象，用于受控修改、持久化、undo 和有状态 Runtime 累计。
- selector 是 `View` 回调期间存在的临时 `GD*Store`。它复制主 Store 当前的选择状态，只允许 `Touch/UnTouch`；调用 `Set/Clear/Replace` 或集合增删改会立即 panic，暴露框架 API 的编程误用。
- StoreView 是生成的独立类型，例如 `GDRoleStoreView`。它持有捕获时刻的稀疏 `GDRole`、同源身份和 txgen 为 `GDRole` 生成的私有强类型状态树，不再持有根 GD、主 Store 或 selector。普通字段状态放在定长 `ops` 中，集合 key/index 使用原始 Go key 类型，生成对象子状态按需创建；完整 `change.Path` 只在 `VisitChanges` 输出边界临时组合。

默认捕获主 Store 当前全部变更：

```go
role.SetName("alice")
role.SetPower(0)

view, err := role.Store().View(nil)
if err != nil {
    return err
}

// Role 是捕获时刻的稀疏值；HasPower 能区分“选中的零值”和“未选择”。
fmt.Println(view.Role.Name) // alice
fmt.Println(view.HasPower()) // true
```

只选择广播字段时，先清空继承的选择，再 Touch 目标路径：

```go
view, err := role.Store().View(func(selector *data.GDRoleStore) {
    selector.AllUnTouch()
    selector.TouchName()
    selector.TouchPower()
})
if err != nil {
    return err
}

body, err := view.Document(change.FieldNameMsgpack)
```

`TouchPower` 只调整 selector，不修改 `role.Power`，也不污染主 Store。捕获过程只复制所选数据：标量直接赋值，嵌套指针、map、slice 只复制所需分支，整字段 Replace/Clear 才复制整个字段。因此成本与所选路径规模相关，不等于深复制整个根 GD。

StoreView 创建完成后，根 GD 的后续修改、map key 增删、slice 长度变化和指针替换都不会改变已经捕获的值。`view.Role` 对业务可读，但应在捕获后按不可变对象使用。根对象仍处于同一所有权区间时，用 `view.Capture(store, configure)` 直接累计当前批次；后续批次已经创建为独立 StoreView 或进入队列时，再通过 `Merge` 合并。两种方式都不能通过直接修改稀疏 Role 来补内容。

### 输出与帧内合并概览

生成代码提供两类输出入口：

- 主 Store 的 `VisitChanges(visitor)` 同步逐条访问当前规范化路径，不创建中间变更切片。Redis、Mongo、Fusion 与 Runtime 容量检查使用该入口。
- StoreView 的 `VisitChanges(visitor)` 访问生成的强类型视图状态树；`Document(kind)` 从 StoreView 自身的稀疏 GD 取值，不再读取根对象。

存储边界接受 `Source`，它组合 `VisitChanges` 与 O(1) 的 `HasChanges`。这避免 adapter 为判空先遍历一次、真正写入时再遍历一次，也把“源在状态不变时可重复访问”的契约固定在编译期。

`Source`、`Visitor` 和 `VisitChanges` 只描述同步遍历，不暗示返回新对象、改变字段名或构建文档。真正的输出转换由边界名称表达：`BuildDocument` 构建通用文档，Redis 构建 JSON patch，Mongo 构建 update document。`Path` 是结构化字段路径；输出后的字符串路径使用 `DocumentPath`，两者不会混用。

```go
if store.HasChanges() {
	err := store.VisitChanges(change.VisitorFunc(
		func(path change.Path, op change.Op) error {
			// 在回调内同步消费 path/op；不要保留可变 Store 引用。
			return nil
		},
	))
	if err != nil {
		return err
	}
}
```

字段名体系与使用边界保持正交：

```text
Store state (FieldID + PathKey + Op)
├── Document(FieldNameGo/JSON/BSON/Msgpack)  # 高级通用文档构建
├── cmd Provider                                    # 固定选择 Msgpack
├── Redis adapter                                   # 固定选择 JSON，并应用 Redis patch 规则
└── Mongo adapter                                   # 固定选择 BSON，并应用 Mongo update 规则
```

`Document(FieldNameJSON/BSON)` 只改变通用文档的路径名称，不会执行 Redis subKey 分组、Mongo
`_id` 保护或 unsafe key 降级。存储调用不能用它绕过 adapter。

Battlefield 一类按帧广播的对象，在同一个实体锁或 Actor 执行段内优先直接累计：

```go
frame, err := role.Store().View(nil)
if err != nil {
    return err
}
role.ClearChanges()

// 同一帧内后续逻辑继续修改相同 role。
item, _ := role.ItemsStore().Get(1001)
item.SetQuantity(7)
if err := frame.Capture(role.Store(), nil); err != nil {
    return err
}
role.ClearChanges()
body, err := frame.Document(change.FieldNameMsgpack)
```

累计规则由生成的强类型视图状态树与稀疏值复制共同完成：同字段或同 key 的后写覆盖先写，后续父字段 Replace/Clear/Delete 会清除此前子路径，互不相交的字段保留；Clear/Delete 后再出现子路径时，父路径提升为 Replace 并保留后批次捕获的数据。`Capture` 必须使用创建视图的同一个主 Store；即使两个主 Store 绑定同一个 GD 指针，也会返回 `ErrValueMismatch`，避免混淆两个独立追踪边界。`Capture` 会同步读取根 GD，因此只能在根对象所有权边界内调用，且不会替调用方清理源 Store。

如果后续批次已经创建为独立 StoreView，则使用 `frame.Merge(next)`；该路径只访问两份稀疏视图，不读取根 GD，适合批次已经进入帧队列后的合并。`Merge` 不修改 `next`，因此来源视图在合并后仍可读取或用于其他只读输出。

StoreView 已经与根 GD 脱离，可以在实体锁或 Actor 单线程区间内捕获后送入帧队列；发布后仍必须按只读对象使用。若先构建 `change.Document` 再跨协程，文档中的复杂 value 只可能引用 StoreView 的稀疏数据，不会引用根 GD，但同样不得在发布后修改。

### 两种变更模式

Store 受控修改模式：

```go
role.Store().SetName("alice")
item, _ := role.Store().ItemsStore().Get(1001)
item.SetQuantity(7)
```

- O(1) 更新字段槽位或集合 key/index 状态。
- 适合大对象、高频修改和有状态服务。
- 事务必须使用这一模式，undo 只覆盖受控 API。

Snapshot Diff 模式：

```go
role.BeginChange()
role.Name = "alice"
role.Items[1001].Quantity = 7
err := role.SaveChange(...)
```

- 允许自然修改 public field。
- 对整棵根对象创建基线并在结束时比较。
- 适合低频请求，不适合持续高频大对象循环。

同一变更周期不能混用两种模式。框架无法在裸字段写入时同步维护 Store 的实时变更状态和事务 undo。

## 生成式变更内核

当前内核由“GD 当前值、Store 状态树、同步遍历和输出边界”组成：

```text
GDRole                                      业务数据唯一事实源
└── GDRoleStore                             根 Store
    ├── ops[Name] = OpSet                   顶层字段槽
    ├── ops[Power] = OpSet
    └── GDRoleItemsMapStore                 map 集合 Store
        ├── ops[1001] = OpReplace           集合元素槽
        └── dirtyChildren[1002]
            └── GDItemStore
                └── ops[Quantity] = OpSet   子对象字段槽
```

Store 不保存第二份业务数据，也不保存每次操作的 value、时间或顺序。`GDRoleStore.Value`、集合 Store 的 `parent` 和子 Store 的 `Value` 都指向当前 GD 数据；Store 只保存“哪些持久化位置需要按什么语义重新读取”。

生成类型名直接携带容器语义：map 字段使用 `<GD><Field>MapStore`，slice 字段使用
`<GD><Field>SliceStore`。这些集合 Store 只能由所属 GD 或父 Store 的 `<Field>Store()` 创建，
不支持手工构造或零值使用。

### 限界上下文与固定术语

后续设计、评审和文档使用下表术语。一个词只在对应上下文内表达一种职责：

| 限界上下文 | 固定术语 | 对应 Go 结构或方法 | 精确定义 |
| --- | --- | --- | --- |
| 数据上下文 | 根 GD | 生成的 `GD*`，例如 `GDRole` | 一个可持久化聚合的业务数据对象，也是当前值的唯一事实源 |
| 数据上下文 | 当前值 | `GD*` 的字段 | 输出边界实际读取的最终业务值，不是 Store 中保存的副本 |
| 追踪上下文 | 主 Store | `GD*Store`、`GD*.Store()` | 绑定根 GD、允许受控修改并可交给持久化入口的 Store |
| 追踪上下文 | Store 状态树 | `GD*Store`、直接指针子 Store、集合 Store | 按生成类型和持久化路径组织的变更状态；树节点不保存父路径字符串 |
| 追踪上下文 | Store 变更状态 | 字段槽、集合元素槽、脏子节点索引 | 主 Store 当前保留的规范化操作集合；同一位置只保留最后语义，不保存业务值副本 |
| 追踪上下文 | 标脏 | `Set*`、`Touch*`、集合 Store 受控 API | 把一个持久化位置写入 Store 变更状态；不等于立即构建文档或执行 I/O |
| 追踪上下文 | 字段槽 | `GD*Store.ops[fieldID-1]` | 顶层或子对象普通字段的最后变更语义 |
| 追踪上下文 | 集合元素槽 | `<GD><Field>MapStore.ops[key]`、`<GD><Field>SliceStore.ops[index]` | map key 或 slice index 本身的最后 Replace/Delete 等语义 |
| 追踪上下文 | 脏子节点索引 | 生成的 map/slice Store `dirtyChildren` | 只标识哪些缓存子 Store 需要遍历，不保存路径或 value |
| 追踪上下文 | 脏通知 | `dirtyHook`、`notifyDirty`、`markChildDirty` | 把子 Store 加入父级待遍历索引或通知运行时；不是完整变更向根复制 |
| 加载上下文 | 已加载字段状态 | `loadedFields`、`LoadedFields()` | 描述部分加载结果，与字段是否变更相互独立 |
| 加载上下文 | 字段投影 | `Load*Fields`、`[]meta.FieldRef` | 指一次存储读取请求选择的字段集合；不是 Store 状态树、StoreView 或变更文档 |
| 快照差分上下文 | 快照基线 | `BeginChange()` 保存的根 GD 副本 | 只用于直接修改 public 字段后的差分，不是 StoreView，也不是待发送载荷 |
| 快照差分上下文 | 快照差分 | `BeginChange()`、`SaveChange()` | 比较根 GD 当前值与快照基线并生成变更；与主 Store 受控修改模式互斥 |
| 遍历上下文 | 变更源 | `change.Source` | 由 `HasChanges` 和 `VisitChanges` 组成的同步只读访问契约 |
| 遍历上下文 | 结构化路径 | `change.Path`、`PathNode`、`PathKey`、`FieldID` | 使用字段编号和强类型 key/index 表达的当前位置 |
| 遍历上下文 | 变更位置 | 一条结构化路径对应的位置 | 容量统计和去重使用的逻辑单位；不能写作含义不明的 dirty path |
| 遍历上下文 | 变更操作 | `change.Op` | 当前结构化路径上的 Set、Replace、Delete 或 Clear 语义 |
| 遍历上下文 | 同步遍历 | `VisitChanges(change.Visitor)` | 在当前调用栈中从 Store 状态树生成 `Path + Op`；不创建中间变更切片 |
| 取值上下文 | 值解析 | `change.ValueResolver`、`Store.ResolveValue` | 按结构化路径从当前 GD 读取最终值，并区分业务 nil 与解析失败 |
| 捕获上下文 | selector | `Store.View(func(*GD*Store))` 的回调参数 | 继承主 Store 当前选择状态的临时 Store；只允许 Touch/UnTouch，不允许修改业务数据，也不会返回给调用方 |
| 捕获上下文 | StoreView | 生成的 `GD*StoreView` | 持有捕获时刻的稀疏 GD 值、生成的强类型视图状态树和同源身份；不引用根 GD 或主 Store |
| 捕获上下文 | 稀疏 GD | `StoreView.Role` 等生成字段 | 只复制所选路径的强类型数据；字段为零值时使用 `Has<Field>()` 判断是否被选择 |
| 捕获上下文 | 同步捕获 | `Store.View(configure)` | 在当前调用栈内完成路径选择、状态规范化和值复制；返回后根 GD 的变化不影响 StoreView |
| 捕获上下文 | 同源累计 | `StoreView.Capture(store, configure)` | 在同一根对象所有权区间内，把当前 Store 的路径和值直接累计进既有视图；不创建中间 StoreView，也不清理源 Store |
| 合并上下文 | 帧内合并 | `StoreView.Merge(next)` | 按批次先后合并同一主 Store 捕获的独立 StoreView；`next` 表示后发生批次 |
| 合并上下文 | 覆盖吸收 | 父级 `OpReplace/OpClear/OpDelete` | 父路径已经完整决定写入语义，子路径不再单独输出 |
| 防御上下文 | 祖先提升 | 最近安全祖先的 `OpReplace` | 超深路径、Store 合并冲突或后端路径限制下的正确性降级 |
| 容量上下文 | 容量收敛 | `storage.CollapsibleChangeStore`、`CollapseChanges()` | 单根变更位置超预算后，清理细粒度状态并只 Replace 实际变脏的顶层分支 |
| 输出上下文 | 变更文档构建 | `Document(kind)`、`change.BuildDocument`、Redis/Mongo document builder | 转换字段名、解析值并形成目标边界需要的变更文档；不能简称为“投影” |
| 输出上下文 | 编码载荷 | BON/JSON/BSON `[]byte` | 由视图、变更文档或其他独立对象编码而成；发布后按只读载荷使用 |
| 发布上下文 | 只读发布 | `msgq.PubSession`、`msgq.BroadcastTopic` 等异步所有权交接 | 单播成功后、广播调用开始后，不得修改、追加或复用载荷内存；广播错误也可能部分入队 |
| 持久化上下文 | 保存成功清理 | `storage.Save`、`Store.ClearChanges()` | 存储成功后递归清理 Store 状态，不修改 GD 数据 |
| 持久化上下文 | 保存失败保留 | `storage.Save` 的错误路径 | I/O 或边界构建失败时保留全部状态，供后续重试 |
| Fusion 上下文 | 纯净回源缓存 | Mongo 回源后写入 Redis、尚未承载业务变更的文档 | 可以使用有限 TTL；不是待回写事实 |
| Fusion 上下文 | 脏热数据 | Fusion 业务写入后、尚未完成 Mongo 可靠回写的 Redis 文档 | main、subKey 和 `@v` 使用 `dirtyTTL` 安全窗口，并由每次热写刷新 |
| Fusion 上下文 | 延迟保存任务 | `fusion.DelayedSave` | 到期后读取 Redis 当前完整文档并 Replace Mongo，不保存业务对象或变更文档 |
| Fusion 上下文 | TTL 收敛 | `storage.RevisionTTLApplier` | Mongo 保存成功后，仅在 revision 未变化时把 main、subKey 和 `@v` 的 TTL 统一缩短为 `delayDelTime` |
| 延迟任务上下文 | 任务租约 | `task.DelayTaskLease`、busy 状态 | worker 在限定时间内取得一次任务处理权；不能简称为 Store 或事务锁 |
| 延迟任务上下文 | 任务载荷 | Redis task backend 的 `payload` 字段 | 任务定位与执行参数的编码值；不是业务消息载荷，也不保存 GD 或 Store 变更状态 |
| 延迟任务上下文 | 后续到期时间 | Redis task backend 的 `pending` 字段 | 任务已被租用期间再次入队时保存的新到期时间；不是待刷根对象 |
| 有状态缓存上下文 | 缓存运行时 | `writeback.Runtime` | CModProcedure WriteBack 组件依赖的提交、生命周期和实体释放 SPI；默认实现是 `writeback.Manager`，不是业务对象缓存的固定形态 |
| 有状态缓存上下文 | 待刷根对象 | 默认 `writeback.Manager.Pending()` 管理的 root | 已发生受控修改且尚未成功 Flush 的持久化根对象；一个 root 的多条路径只计一个 |
| 事务上下文 | 撤销记录 | `change.UndoRecorder`、`change.UndoFunc` | 恢复事务前 GD 值和 Store 状态；与帧内合并职责独立 |

“帧内合并”中的“帧”表示一次业务合并窗口，不限定为 20ms；网络上下文中的“协议帧”才表示 Agent `Frame`。Store 状态本身是按位置归一化的集合，遍历顺序不构成业务契约；`Capture` 的调用顺序和 `Merge(next)` 的参数顺序都具有“后批次覆盖前批次”含义。

### 生成 Store 的常驻状态

生成 Store 是持久化和下行变更构建共用的变更内核。txgen 已经知道每个类型的字段数量、字段 ID、集合类型和子对象类型，因此生成代码直接保存规范化状态：

```go
type GDRoleStore struct {
    Value *GDRole
    ops   [10]change.Op
    // generated child/collection stores...
}

type GDRoleItemsMapStore struct {
    parent        *GDRoleStore
    ops           map[int32]change.Op
    stores        map[int32]*GDItemStore
    dirtyChildren map[int32]struct{}
}
```

- 普通字段用 `ops[fieldID-1]` 保存最后语义；同一字段重复 `Set` 不追加条目。
- map/slice 的显式元素操作用 `ops[key/index]` 保存最后语义。
- 生成对象子字段的变化仍保存在对应子 Store；`dirtyChildren` 只索引真正变脏的子 Store，`stores` 可以缓存已经访问但仍干净的子 Store。
- 整字段 Replace/Clear 会在同步遍历时覆盖该字段下的 key/子 Store 状态；`UnTouch` 可以撤销显式选择，并重新暴露仍真实存在的子 Store 变更。
- `View(configure)` 只在回调期间复制这些规范化状态作为 selector；捕获完成后返回 StoreView，不返回复制的主 Store 状态树。

例如连续执行：

```go
role.ItemsStore().Set(1001, first)
role.ItemsStore().Set(1001, second)
role.ItemsStore().Set(1001, final)
```

常驻状态始终只有 `ops[1001] = OpReplace`。调用 `VisitChanges`、`Document(kind)` 或常规持久化 API 时会同步访问一条 `Items[1001] Replace`，不会创建中间变更切片。

Snapshot Diff 也先构造同一种临时生成 Store 状态，再走统一输出边界。这样两种变更模式共享字段槽、集合状态和子 Store 语义；下行文档与持久化 adapter 都直接同步消费。

### 标脏与路径组织

普通字段 setter 同时修改 GD 当前值和字段槽：

```go
role.SetName("alice")
role.SetName("bob")
```

两次调用只留下 `ops[fieldName-1] = OpSet`。Store 不保存两个字符串，输出边界最终从 `role.Name` 读取 `"bob"`。

复杂集合按“元素操作”和“子对象字段变化”分别记录：

```go
role.ItemsStore().Set(1001, item)       // ops[1001] = OpReplace
current, _ := role.ItemsStore().Get(2)
current.SetQuantity(7)                  // dirtyChildren[2] + 子 Store 的 Quantity 槽
```

`Set(key, value)` 遵循 Go map 赋值的整体覆盖语义，因此复杂 value 使用 Replace。`Get(key)` 返回绑定当前持久化位置的子 Store；子 Store 发生变化时，`dirtyHook` 只把该 key 加入 `dirtyChildren`，完整路径到 `VisitChanges` 时才按生成类型组装。

Slice 只有索引身份稳定时才能保留 index 级状态。`SetAt` 和元素子 Store 修改可以使用 index；Append、Insert、RemoveAt 或其他长度/顺序变化会把整个 slice 标为 Replace，避免旧 index 指向新元素。

同一个子 GD 指针可以挂载到多个根对象或集合位置。每个位置创建独立子 Store 并独立标脏；框架不会按指针身份自动把一次修改扩散到所有挂载位置。把同一对象 `Set` 到多个集合时，每个 `Set` 会分别记录该位置的 Replace；后续若要求所有位置同步更新，业务必须操作相应 Store 路径或整体 Touch 共同祖先。

### GD 运行时 sidecar

`GD*` 是业务数据载体，不应让 Store 引用、模式和 snapshot 基线永久放大每一个数据对象。生成结构只保留一个私有指针：

```go
type GDRole struct {
    // business fields...
    state *storeState[GDRole, GDRoleStore]
}

type storeState[V any, S any] struct {
    store    *S
    mode     change.Mode
    snapshot *V
}
```

`state` 在首次进入 Store 模式或 snapshot 模式时才分配。只做完整 JSON/BSON/Msgpack 编解码、跨层传递或只读访问的 GD 不承担 sidecar 分配。`Copy()` 和 `ResetStore()` 会清空 `state`；完整 `Load` 替换业务字段时也不会把临时对象的运行时状态复制给调用方。

集合内部按路径创建的子 Store 使用集合自己的 `stores` 绑定，不会把共享的子 GD 强行绑定到某一条父路径。这样同一个 `*GDItem` 可以出现在多个角色或多个集合中，每条路径仍独立记录 Store 变更状态。

### 同步遍历、捕获与帧内合并

主 Store 的 `VisitChanges(visitor)` 在调用栈内遍历规范化状态，按值传递 `Path + Op`。visitor 必须在回调内完成处理，不能把主 Store 或根 GD 指针交给无锁异步队列。Redis、Mongo、Fusion 与事务 Runtime 直接消费这一契约。

`View(configure)` 分成三个同步阶段：

1. 没有 configure 时直接遍历主 Store；有 configure 时，`selectionCopy()` 才复制 `ops`、集合元素槽、脏子节点索引和必要子 Store，形成临时 selector。
2. 可选 configure 回调用 Touch/UnTouch 调整 selector；任何数据修改 API 都被生成代码拒绝。
3. 生成 visitor 逐条把捕获时刻的值复制进稀疏 GD，并按字段类型写入 StoreView 自己的定长字段槽、强类型集合槽和按需子状态，不创建中间路径批次。

返回的 StoreView 实现 `change.Source`，但不实现 `storage.ChangeStore`。因此它可以构建下行文档和参与帧内合并，却无法在编译期传给 `storage.Save`。视图状态树只保存字段或集合位置上的 `change.Op`，业务值只存在于生成的稀疏 GD 中，不维护第二套 `any` value 日志。

Battlefield 等按帧广播场景在根对象所有权区间内通过 `StoreView.Capture(store, configure)` 直接累计：生成 visitor 把当前 Store 的本批值复制到既有稀疏 GD，并立即记录进同一棵生成状态树，避免临时 StoreView 的值复制和二次状态登记。若后续批次已经创建为独立视图，则通过 `StoreView.Merge(next)` 从 next 的稀疏 GD 复制值并合并状态；该过程不访问根 GD，也不修改 next。跨对象合并应在业务层分别构建独立视图或编码载荷后再组织消息。

捕获动作必须发生在实体锁、Actor 单线程或其他根 GD 所有权边界内，确保读到一致时刻的路径和值。StoreView 一旦返回便不受根 GD 后续修改影响；它可以进入异步帧队列，但从此必须只读。

### PathNode 与 PathKey

```go
const MaxPathDepth = 4

type Path struct {
    Nodes [MaxPathDepth]PathNode
    Depth uint8
}

type PathNode struct {
    Field FieldID
    Key   PathKey
}

type PathKey struct {
    Kind KeyKind
    Int  int64
    Uint uint64
    Str  string
}
```

一个节点把字段和进入该字段集合的 key 绑定在一起：

```text
Role.Items[1001].Quantity

Node 0 = FieldItems + KeyInt(1001)
Node 1 = FieldQuantity + KeyNone
Depth  = 2
```

这比把 `Items` 和 `1001` 分成两个节点更紧凑，也让“整个集合元素”和“元素子字段”天然具有前缀关系。

`Depth == 0` 表示空或无效路径。有效变更路径至少一层。`Path.Valid()` 同时校验深度、字段编号和 key
类型；所有公开文档/存储边界以及生成的 `ResolveValue`、路径 resolver 都必须先校验，再访问定长数组。
外部 `Source` 提供 `Depth > MaxPathDepth` 或未知节点时返回 `ErrInvalidPath`，不能因数组切片越界 panic。

### 最大深度

最大深度固定为 4。设计依据不是认为业务永远不会更深，而是控制每次路径传递、拼接和边界转换的固定成本。典型表达能力：

```text
Depth 1: Role.Name 或 Role.Items
Depth 2: Role.Items[1001].Quantity
Depth 3: Role.Items[1001].Buffs[5].Type
Depth 4: Role.Items[1001].Buffs[5].Attrs["ATK"].Value
```

路径超过上限时不能 panic，也不能截断后继续把叶子值写到祖先。`JoinPathLimited` 返回最近可表达祖先和 `complete=false`，生成 Store 必须把该祖先提升为整体 Replace，并输出可观测提示。

### Op

当前支持四种严格语义：

| 操作 | 含义 |
| --- | --- |
| `OpSet` | 设置当前路径值；用于字段或集合元素局部更新 |
| `OpReplace` | 用当前完整值覆盖路径，不保留旧对象中未出现字段 |
| `OpDelete` | 删除 map 元素或存储字段路径 |
| `OpClear` | 清空容器，但保留字段为空 map/slice |

`OpClear` 的空值类型必须由 `ValueResolver` 按完整路径解析。例如根字段是 `LordInfo`、实际变更路径是
`LordInfo.Skills` 时，边界读取 `Skills` 的 `[]int32` 类型并构造同类型空 slice；Redis/Mongo 不得只根据
`LordInfo` 猜成空对象。无法解析集合类型时写入失败并保留 Store 变更。

`Set` 和 `Replace` 必须区分。例如旧文档为：

```json
{"quantity": 1, "quality": 9}
```

当前 Go 对象只有：

```json
{"quantity": 3}
```

Replace 必须得到仅含 `quantity` 的对象，不能残留旧 `quality`。集合 wrapper 的 `Set(key, value)` 遵循 Go map 赋值的整体覆盖语义，因此复杂 value 记录 Replace；修改 value 的子字段才记录细粒度 Set。

未知 Kind 是协议错误。每个输出边界都必须明确拒绝，不能按 Set 或成功处理兜底。

### 延迟取值与异步边界

同步遍历事件不保存 value。输出边界通过生成 Store 的 resolver 读取当前值：

```text
VisitChanges(Path=Items[1001].Quantity, OpSet)
        |
        v
Store.ResolveValue(path)
        |
        v
当前 GD 中的 int64(7)
```

这使同一路径在一个周期内修改多次时无需保存多个中间值。生成 Store 的字段槽、集合状态和子 Store 索引已经只保留最终变更语义。

解析结果必须区分：

- `(nil, true)`：路径存在，业务值就是 nil。
- `(_, false)`：路径无法解析，属于错误。

不能把解析失败写成数据库 null。

`change.Document` 会创建独立的 `Set/Delete` map。若来源是主 Store，`Set` 中的复杂 map、slice 或指针 value 可能仍引用当前根 GD；若来源是 StoreView，它们只引用 StoreView 自己的稀疏数据。术语上必须区分三个阶段：

- 调用 `Document(kind)` 是“边界文档构建”。
- StoreView 是与根 GD 脱离的“独立视图”；把视图或变更文档编码为 BON/JSON/BSON 字节后，才得到编码载荷。
- 把编码载荷交给 msgq、网络或其他异步队列后进入“只读发布”阶段，各方都不得原地修改、追加或复用载荷内存。

只有不再引用根 GD 的独立视图或编码载荷才能退出对象锁；交给异步队列时优先使用编码载荷。否则后续 GD 修改可能改变待发送内容。只读发布是所有权契约，不表示 Go 运行时把 StoreView、map、slice 或 `[]byte` 变成不可写内存，也不限制根 GD 和主 Store 继续处理下一批业务变更。

### 确定性合并边界

主 Store 在每次记录时按字段槽或集合位置原地归一化，不提供批次合并 API。跨时刻的帧内累计发生在同类型 StoreView 上：同一根对象所有权区间内使用 `Capture`，已经创建为独立视图的批次使用 `Merge`，二者执行相同的确定性规则：

1. 同一字段槽或集合 key/index 由后者覆盖前者。
2. 后发生的父字段 Replace/Delete/Clear 清理更早子路径。
3. 前批次已记录父字段 Clear/Delete、后批次又捕获其子路径时，最终提升为父字段 Replace；`Capture` 从当前根 GD、`Merge` 从两份稀疏数据取得可靠值。
4. 后批次直接操作集合元素时，该 key/index 的元素状态覆盖此前同位置的子路径。
5. 后批次修改同一子路径时，`Capture` 复制当前 Store 对应的根值，`Merge` 复制后批次 StoreView 已捕获的值。
6. 互不相关的位置分别保留。
7. 超深路径在主 Store 捕获前提升为最近祖先 Replace；不安全后端 key 仍在具体存储边界提升，二者不由 StoreView 猜测。
8. 两份 StoreView 不是由同一个主 Store 捕获时返回 `change.ErrValueMismatch`，即使主 Store 恰好绑定同一个 GD 指针也不能合并。

它不会猜测：

- “先创建后删除，因此可完全抵消”的数据库状态推断。
- 依赖当前 map 是否原本存在的业务判断。
- 跨主 Store、跨 flush 批次的持久化日志重放压缩。

原因是主 Store 和 StoreView 都不持有数据库基线，也不保存创建来源。需要业务上下文的抵消必须由业务层决定；runtime 只维护当前 GD 的可靠写入位置。

同一所有权区间内直接累计并修改接收者状态：

```go
frame, err := role.Store().View(nil) // 前批次已独立捕获
if err != nil {
    return err
}
role.ClearChanges()

role.SetPower(100) // 后批次
if err := frame.Capture(role.Store(), nil); err != nil {
    return err
}
```

`Capture` 返回后，`frame` 同时保留前批次互不冲突的位置和后批次捕获值；随后不再读取 `role`，可以脱离 role 生命周期按只读视图保存，或编码后发布。若后批次已先创建为 `next`，改用 `frame.Merge(next)`，该过程完全不访问根 GD，也不会消费 next。

### 集合规则

Map：

- `Set(key, value)`：复杂 value 整体 Replace。
- `Delete(key)`：记录元素 Delete。
- `Get(key)` 返回子 Store：修改子字段记录细粒度路径。
- 新建元素后修改子字段：生成 Store 必须保证至少整体写入新元素，避免数据库出现残缺对象。

Slice：

- `SetAt(index, value)` 可以表示已存在索引的整体 Replace。
- 修改元素子字段可以记录 index 路径。
- Append、Insert、RemoveAt 或任何导致索引漂移的操作提升为整个 slice Replace。
- StoreView 在捕获时已经复制稳定 index 对应的值；捕获后 slice 长度变化不会改变旧 StoreView。

### 动态 key 的后端降级

`map[string]` key 在内存中可以包含任意普通字符串；runtime 不对业务 key 做私有编码。Mongo update path 中 `.` 是路径分隔符，前导 `$` 等形式也可能改变表达语义。因此 Mongo 边界遇到不能安全表达的 key 时：

1. 不修改 key。
2. 不生成转义后的数据库字段名。
3. 回溯到最近安全祖先。
4. 读取当前完整容器并整体 Replace。

这样运维直接查询数据库时看到的 key 与业务一致。代价是该次写入放大，但存储边界不是 setter 热循环。

### 物理尺寸与历史对照

定长路径消除了路径节点分配，但 `Path` 按值传递仍有固定复制成本。任何布局优化必须同时衡量：

- `PathKey` 与 `Path` 大小。
- 同步遍历与边界文档构建频率。
- map key 类型表达完整性。
- 生成代码改动范围。
- benchmark 中的真实收益。

不要为了“零分配”过早引入自研 value union、`unsafe` 字符串或后端专属二进制值。

2026-08-21 在 x86_64 macOS、Go 1.26.0 上，对 1024 个不同 map key 的生成 Store 基准进行同机前后对比：

| 场景 | 旧常驻 DeltaSet | 生成字段槽/key 状态 | 结果 |
| --- | ---: | ---: | --- |
| 记录 1024 个不同 key | 约 37.3-40.2 ms/op，1.57 MB/op | 约 0.43-0.44 ms/op，0.87 MB/op | 耗时约降低 98.8%，分配字节约降低 44% |
| 成功构建文档并清理 1024 个 key | 约 35.2-37.7 ms/op，1.51 MB/op | 约 0.41-0.44 ms/op，0.80 MB/op | 耗时约降低 98.9%，分配字节约降低 47% |
| 同一 key 重复修改 | 约 593-664 ns/op | 约 488-522 ns/op | 热路径约快 17%-25% |

这些数字用于证明本次物理模型选择，不是跨机器性能承诺。基准仍保留在 txgen 的 `BenchmarkGeneratedStoreDirtyRuntime`，模板或 runtime 改动后必须复跑。

2026-08-21 使用 Go 1.26.5 比较已删除的 `[]DeltaEntry` 中间批次实验路径与生成式同步遍历/合并，测试对象均为 1024 个不同 map key：

| 场景 | DeltaSet 路径 | 生成式同步遍历/合并 | 结果 |
| --- | ---: | ---: | --- |
| 只遍历脏路径 | 约 0.219-0.225 ms/op，693 KB/op，13 allocs/op | 约 0.083-0.087 ms/op，0 B/op，0 allocs/op | 遍历耗时约降低 60%，消除中间切片分配 |
| 生成 `change.Document` | 约 0.71-0.74 ms/op，1.16 MB/op，3015 allocs/op | 约 0.40-0.44 ms/op，212 KB/op，2996 allocs/op | 省去约 950 KB 中间 Delta/Clone；剩余分配来自最终字符串路径和文档 map |
| 16 批 x 64 key 帧合并 | 约 0.46-0.52 ms/op，1.16 MB/op，140 allocs/op | 约 0.17-0.18 ms/op，37 KB/op，17 allocs/op | 生成 Store 直接合并变更状态，最后只构建一次文档 |

这组结果决定只保留 `VisitChanges + Merge`。显式中间批次、通用 Compact 及其公开 API 已删除；表格仅记录技术选型依据。

2026-08-22 使用 Go 1.26.5，在 txgame 真实生成 `GDRole.Items` 上继续比较存储边界。对象包含 1024 个不同 map key，每个 key 修改 `Quantity`；两条路径都生成相同 Redis patch 或 Mongo update document：

| 场景 | 先创建 DeltaSet 中间批次 | Store 直接同步遍历 | 结果 |
| --- | ---: | ---: | --- |
| Redis document patch | 约 1.47-1.54 ms/op，1.31 MB/op，4713 allocs/op | 约 0.88-0.90 ms/op，359 KB/op，4692 allocs/op | 耗时降低约 40%，分配字节降低约 73% |
| Mongo update document | 约 13.37-14.55 ms/op，1.28 MB/op，5740 allocs/op | 约 0.98-1.00 ms/op，330 KB/op，5718 allocs/op | 避免中间批次、Clone、通用 Compact 和二次路径扫描，耗时降低约 93%，分配字节降低约 74% |

因此 Redis/Mongo/Fusion adapter 与 `writeback.Manager` 直接消费 `Source`。旧的 `Build*WithDeltas` 与显式批次类型已经删除，`storage.Save` 不得重新引入中间批次层。

2026-08-22 删除显式批次 API 与过渡适配层后，使用 Go 1.26.5 在同一台 x86_64 macOS 设备复测：`VisitChanges1024` 为约 `0.082-0.088 ms/op, 0 B/op, 0 allocs/op`，`BuildDocument1024` 为约 `0.389-0.399 ms/op, 212 KB/op, 2996 allocs/op`，`MergeAndBuildFrameDocument` 为约 `0.167-0.174 ms/op, 37 KB/op, 17 allocs/op`。txgame 的真实生成结构边界中，Redis patch 为约 `0.873-0.928 ms/op, 359 KB/op, 4691 allocs/op`，Mongo update document 为约 `0.988-1.041 ms/op, 330 KB/op, 5717 allocs/op`。这些结果与删除前的同步遍历路径处于同一量级，说明精简公开 API 和运行时中间层没有引入可见性能回退。

同日增加公开 `Path` 合法性防线后，先消除边界、`FullPath` 与生成 resolver 的重复扫描，再以相同 1024 key 场景复测：Redis 为约 `0.920-0.992 ms/op, 359 KB/op, 4691 allocs/op`，Mongo 为约 `0.984-1.029 ms/op, 330 KB/op, 5716 allocs/op`。结果仍处于原有波动区间且分配次数未增加；完整节点校验只在 I/O/文档边界执行一次，生成 resolver 仅保留固定数组深度护栏。

2026-08-23 继续消除存储边界的重复路径构造：`Path.FieldNameAt` 在不创建 `DocumentPath` 的情况下解析单个字段，并保留嵌套类型的 `PathFieldNameResolver` 语义。Redis/Mongo 只用根 Go 字段查询元数据，因此不再为每条变更额外构造完整 Go 路径；JSON/BSON 输出路径仍在对应边界完整解析。相同 1024 key 基准中，Redis 从约 `359 KB/op, 4691 allocs/op` 降至 `292 KB/op, 2743 allocs/op`，Mongo 从约 `330 KB/op, 5716 allocs/op` 降至 `263 KB/op, 3768 allocs/op`，两者每次均减少约 66.5 KB 和 1948 次分配，耗时未出现稳定回退。

同日使用 txgame 真实生成 `GDRole.Items` 把容量成本拆为记录、遍历和最终边界三段。测试先创建 1024 个已有 `GDItem`，再通过子 Store 修改每个 `Quantity`：记录状态约为 `0.340-0.359 ms/op, 290 KB/op, 3126 allocs/op`；对已经记录的状态反复执行 `VisitChanges` 约为 `0.250-0.264 ms/op, 0 B/op, 0 allocs/op`；Redis patch 约为 `0.777-0.846 ms/op, 292 KB/op, 2743 allocs/op`，Mongo update document 约为 `0.848-0.871 ms/op, 263 KB/op, 3768 allocs/op`。这说明首次创建强类型集合子 Store 和最终文档仍是主要分配来源，保留状态的同步遍历不会在每轮 Flush 额外分配。当前不增加按估算字节拒绝或收敛的配置；只有真实服务证明 Store 状态树的常驻内存无法由 `MaxRootChanges`、`MaxPendingRoots` 和进程 Heap 观测解释时，才重新评审 byte 预算。

同日继续把生成 GD 的三个常驻运行时字段收敛为一个懒分配 sidecar。使用 txgame 的真实生成结构与参考工程做 `unsafe.Sizeof` 对比：

| 结构 | sidecar 前 | sidecar 后 | 参考工程 |
| --- | ---: | ---: | ---: |
| `GDRole` | 1048 B | 936 B | 936 B |
| `GDItem` | 56 B | 40 B | 40 B |
| 根 Store | 448 B | 448 B | 1440 B |
| Items 集合 Store | 32 B | 32 B | 32 B |
| Item Store | 56 B | 56 B | 64 B |

`GDRole` 的收益大于单个对象尾部减少的 16 B，是因为它按值包含的多个生成结构也同时缩小，收益会递归累积。sidecar 本身在真正进入 Store/snapshot 模式时按需分配；表格只比较结构体常驻内联尺寸。相同 benchmark 下，1024 个不同 map key 的当前结果约为 `0.44-0.45 ms/op, 0.855 MB/op`，成功构建文档并清理约为 `0.41-0.43 ms/op, 0.788 MB/op`，相对 sidecar 前继续减少约 15-17 KB/op，耗时保持同一量级。

sidecar 不是无条件收益：首次直接修改一个独立 GD 会多一次小对象分配。txgame 的真实 `GDRole` 首次标脏从约 `1600 B/op, 2 allocs/op` 变为 `1496 B/op, 3 allocs/op`，总字节减少且耗时处于同一波动区间；极小的独立 `GDItem` 从约 `128 B/op, 2 allocs/op` 变为 `136 B/op, 3 allocs/op`。集合中的生成对象子字段通过路径子 Store 修改，不会为每个值绑定 sidecar，因此大集合基准仍然净受益。`BenchmarkGeneratedStoreDirtyRuntime/FirstScalarWrite` 用于持续观察首次写入成本；若未来真实业务证明大量微型 GD 都作为独立根对象高频首次写入，再评估专用 fast path，不能以增加所有纯数据对象常驻尺寸为代价提前优化。

### StoreView 生成状态树

StoreView 不使用通用完整 `Path` 集合。txgen 已知每个 GD 类型的字段 ID、集合 key/index 类型和生成对象嵌套关系，因此为每个类型生成私有状态树。例如 `GDRole` 的普通字段操作保存在定长 `ops` 中，`Items` 使用 `map[int32]change.Op` 保存元素整体操作，并使用 `map[int32]*viewStateGDItem` 按需保存叶子字段状态。状态树的结构与 Go 类型一致；只有真正发生变更的集合 key 和子对象才分配节点。

`Record`、`Contains`、`Capture` 和 `Merge` 都直接访问这些强类型槽位。同位置后写覆盖、父路径吸收和 Clear/Delete 后子写提升在生成状态树内完成；`VisitChanges` 才递归组合定长 `change.Path` 并交给输出边界。整数 map key 从公开 `PathKey` 转回实际 Go key 前必须做无损范围校验，例如 `math.MaxInt64` 不能截断成 `int32(-1)` 后误命中已有状态或业务值。

2026-08-22 在 x86_64 macOS、Go 1.26.5 上，以 16 批、每批 64 个不同 map key 的真实生成 `GDRole` 回测：

| 场景 | 通用完整路径状态 | txgen 生成状态树 | 结果 |
| --- | ---: | ---: | --- |
| 先创建 16 份 StoreView、依次 Merge、最后构建文档 | 约 `2.020-2.202 ms/op, 1.866 MB/op, 6794 allocs/op` | 约 `1.34-1.53 ms/op, 0.694 MB/op, 6645 allocs/op` | 分配字节降低约 63%，耗时降低约 24%-39% |
| 同一所有权区间直接 Capture、最后构建文档 | 约 `1.340-1.377 ms/op, 1.267 MB/op, 5323 allocs/op` | 约 `1.02-1.18 ms/op, 0.537 MB/op, 5309 allocs/op` | 分配字节降低约 58%，耗时降低约 12%-26% |
| 1024 个已存在 map 元素只捕获叶子 `Quantity` | 无同版历史基线 | 约 `1.65-1.80 ms/op, 0.755 MB/op, 9156 allocs/op` | 持续观察深层叶子复制和最终文档成本 |

分配次数只小幅下降，说明剩余小对象主要来自稀疏值复制和最终 `change.Document` 的字符串路径/map，而不再来自每条变更保存完整 `Path`。基准由 `BenchmarkGeneratedStoreDirtyRuntime/CaptureMergeAndBuildFrameDocument`、`CaptureAndBuildFrameDocument` 和 `CaptureLeafAndBuildFrameDocument` 持续回归。

生成状态树需要保留类型专属热路径，但不意味着每个 Store 都应复制完整基础设施。2026-08-22 将集合状态、字段 undo、加载位图和读取装配下沉到生成包基础层后，txgame 真实 `GDRoleStore.go` 从 `29,434` 行、约 `723 KB` 降至 `21,663` 行、约 `584 KB`；全部 `*Store.go` 加 `type_registry.go` 从 `411,893` 行、约 `10.16 MB` 降至 `303,451` 行、约 `8.22 MB`，总行数减少约 `26.3%`，源码字节减少约 `19.1%`。固定 txgen fixture 的 `GDRoleStore.go` 受 `record/copy/visit` 和嵌套 resolve 静态展开保护，设置了 `4,550` 行和 `119.5 KB` 的体积回归上限。运行时分配次数保持不变，捕获、合并和文档构建仍以 `BenchmarkGeneratedStoreDirtyRuntime` 持续校验。

## 存储抽象

### 统一文档边界

通用存储层使用 JSON 字段名的 `map[string]any` 作为后端之间的公共文档：

```text
generated Store / TypeSpec
            |
            v
JSON-name Document
      /           \
     v             v
Redis JSON      Mongo BSON adapter
```

Mongo adapter 通过生成的 `DocumentFactory` 把公共文档还原为 GD，再由 Mongo driver 根据 BSON tag 编解码。这样 `_id`、缩写 BSON 名和 Redis JSON 名不会互相泄漏。

### RecordKey

`RecordKey` 是存储定位身份，通常包含：

- 记录 ID。
- route/shard key。
- 生成 TypeSpec 提供的类型和存储元数据。

RecordKey 不等于事务 LockKey：

- RecordKey 面向 Redis/Mongo 路由。
- LockKey 面向单机一致性域。
- 纯内存 GD 可以有 LockKey 而没有存储注解。

### 接口按能力拆分

存储接口不按每个业务操作组合成定制类型，而按基础能力拆分：

- `ChangeApplier`：应用当前规范化变更。
- `DocumentLoader` / `BatchDocumentLoader`：读取公共文档。
- `DocumentReplacer`：完整替换文档。
- `DocumentBackfiller`：仅在目标不存在时原子回填文档，并显式报告是否插入。
- `mongo.CounterStore`：按 routeType 和稳定分片键，为调用方指定 collection/key 原子增加 size，返回新上界。
- `DeleteApplier`：Redis-only、Mongo-only adapter 的整记录删除能力。
- `RecordLocker`：记录级互斥。
- `RecordRevisionReader` / `RevisionDeleteApplier`：版本保护删除。

Redis 与 Mongo adapter 通过组合这些接口表达单后端能力；Fusion 组合读、写、回填和延迟回写能力，但不实现 `DeleteApplier`。公共 API 可以保持 `storage.Save`，而不是暴露 `ApplyInsert/ApplyUpdate/ApplyReplace` 组合给业务。

Mongo 条件回填使用单条 `$setOnInsert + upsert`；匹配现有文档时只报告 `inserted=false`，不能改写字段。
CounterStore 不定义业务 collection、计数键或号段缓存；驱动每次以 majority 写关注执行单条 `findOneAndUpdate`，
用 `$inc`、缺失创建和返回新值预留调用方指定的正增量。过滤条件限制旧值在 `[0, MaxInt64-size]`，
避免 Mongo 将溢出整数提升为浮点数；超界旧文档无法匹配，upsert 的同 `_id` 唯一约束使操作失败，不覆盖原值。
全局范围由调用方的 Mongo routeType、稳定分片键、collection 和 key 共同决定。本地号段与空洞策略属于项目，
SessionID 不消费此原语。
条件回填的过滤字段必须具有唯一性。常规业务模型把 `idField` 保存为语义明确的普通 BSON 字段，并由生成元数据声明
单字段唯一索引；否则 adapter 在写入前拒绝调用。仅有复合唯一索引不能保证按单个业务主键并发创建时唯一。
物理 `_id` 自带唯一性，但只用于存储结构完全确定的特殊模型。

Fusion 根记录代表玩家、军团等长期身份数据。Redis 与 Mongo 无法在一次原子事务中共同物理删除；任一侧成功、另一侧失败，都可能导致旧 Mongo 回填 Redis、旧 Redis 延迟回写 Mongo，或调用成功但数据丢失。因此框架在生成期和类型系统中禁止 Fusion 根记录物理删除，业务使用显式状态字段完成逻辑删除。该限制只作用于整条根记录，不影响 `ItemsStore().Delete(key)` 这类集合元素变更。

### 保存成功边界

`storage.Save` 的核心契约：

```text
Store (Source)
    |
    v
writer.ApplyChangesWithOptions(...)
    |
    +-- success -> ClearChanges
    |
    +-- error   -> 保留全部变更状态
```

只要策略整体没有成功，就不能清理 Store。Fusion 的策略成功边界是 Redis patch 成功且根记录已经登记到进程内分发器；登记不执行外部 I/O。延迟任务 backend 发布失败发生在业务调用返回之后，由分发器保留本地 pending、输出结构化 error 日志并持续重试，不能反向把已经成功的 Redis 业务写改成失败。

### 文档返回值严格性

Loader 返回值必须成对成立：

- `exists=true` 时 document 非 nil。
- `exists=false` 时 document 必须 nil。

Replacer：

- 空 map 是合法空对象。
- nil map 没有明确写入语义，返回 `ErrNilDocument`。

未知 Lua 返回值、损坏 JSON、无有效变更、字段路径无法解析等都返回错误。不能把“不知道发生了什么”归类为成功。

## 存储路由

`storage.RouteTable` 是 Redis 与 Mongo 共用的启动期只读路由内核。数字 `routeBy` 直接转换为 `int64`；字符串
`routeBy` 使用 FNV-1a 32 位哈希，哈希值为零时归一化为 `1`。实现直接遍历字符串字节，不创建 hasher 或
`[]byte`，使高频路由保持零分配。相同输入必须在不同进程和不同存储 backend 得到相同路由值。

允许的 `routeBy` Go 类型由 `meta.Validate` 统一定义为内建整数、`byte`、`rune` 和 `string`。AST 扫描、txgen
和高级手工元数据都复用该入口；txgen 不维护第二份类型列表。这样不支持的类型会在扫描、生成或启动装配期失败，
不会拖到首次 Redis/Mongo 路由。`RouteTable` 仍保留运行期具体值校验，作为高级调用方绕过元数据后的最后防线。

路由按两步执行：先用路由值选择 `min/max` 区间，再用 `路由值 % len(shards)` 选择该区间的 endpoint。
FNV-1a 算法、字符串零值规则、区间和 shard 顺序共同组成持久化布局契约。字符串即使只含数字也按字符串哈希，
不能因为表现形式相同而与数值 ID 混用。

Fusion 只有一份 `RecordKey.ID` 和 `RecordKey.ShardKey`。`meta.Validate` 因此要求 Redis 与 Mongo 的 `idField`
引用相同 Go 字段，`routeBy` 也引用相同 Go 字段；否则生成层若优先取一侧字段，另一侧会把同一个值解释成不同
业务身份并产生错读写。`routeType` 不属于记录身份，可以在两个 backend 上不同。JSON、BSON、Msgpack 字段名
也不参与这项比较，它们继续由同一个 Go 字段在各自输出边界独立映射。

路由配置表达的是应用层独立存储终端，不展开终端内部拓扑。一条包含多个副本集成员或 `mongos` 地址的 Mongo URI
仍是一个 endpoint；云 Redis 代理后的节点和 slot 也不进入 `RouteTable`。原生集群内部扩缩容由驱动和云服务处理，
通常不修改应用配置。

已有数据后直接增加、删除或重排同一区间的 shard 会改变取模结果。tx 不热更新路由表，也不提供自动双读、双写
或数据迁移。应用层布局变更必须先封口写入、迁移并验证数据，再切换配置。单调递增数字 ID 可以通过保留旧区间、
为未来 ID 增加新区间来避免旧数据漂移；字符串哈希不具备时间或大小单调性，不能使用该方法区分新旧记录。

## Redis 后端

### 数据模型

Redis 使用 string 保存 JSON 文档：

```text
mainKey                   根文档
mainKey:<subKey>          拆分的大字段文档
mainKey@v                 热数据生命周期版本
mainKey@lock              回源、回写和删除互斥锁
```

带 subKey 的类型仍必须存在 main 文档。即使全部业务字段都拆到 subKey，main 至少保存 `{}`，作为热记录存在标记。

### 为什么业务文档不使用 Redis Hash 或 RedisJSON

当前默认选择普通 Redis string JSON：

- 所有 Redis 环境都支持，不依赖模块。
- Lua 可以原子合并 main/subKey 文档和版本。
- 与参考生产路径和云 Redis 代理兼容。

代价是 Lua 局部 patch 需要解码和重新编码对应 JSON 文档。RedisJSON/ReJSON 作为未来可选 backend 评估，不改变通用 Store 和变更遍历契约。

### Lua patch

Redis Lua 在对应 main/subKey 文档内应用 set/delete/clear/replace。局部路径缺少父对象时可以创建正确的中间 table；如果已存在父节点类型与路径要求冲突，返回错误，不把异常标量自动替换成容器。

Fusion 热写与普通 Redis 文档写入使用同一套 Lua：patch 文档、递增 revision，并持久化 main/subKey/`@v`。延迟任务登记属于进程内分发器职责，不向业务文档 Lua 增加第二类 key 或容量协议。

严格失败是为了暴露数据来源问题。运行时自动修复异常拓扑可能掩盖旧代码或人工写库造成的数据损坏。

### Cluster 路由

同一记录的 main、subKey、`@v` 和 `@lock` 必须共享 hash tag。云代理或单节点可直接执行 MGET；本地原生 Redis Cluster 在客户端按 slot/hash tag 分组后多次 MGET。

Lua 的 `KEYS[]` 是 Redis 命令显式标记的 key 参数。任意一次 Lua 调用涉及的 key 必须同 slot；云代理不能分析脚本内部任意字符串并替调用方拆分执行。

## Mongo 后端

Mongo 文档主键必然是 `_id`，但业务 `idField`、`routeBy` 和 BSON `_id` 绑定承担不同职责：

- BSON `_id`：Mongo 文档物理主键字段。
- `idField`：框架生成 RecordKey 和过滤条件所需业务字段。
- `routeBy`：选择 Mongo endpoint/shard 的路由字段。

常规业务模型将三者分开：业务 ID 保留明确 BSON 字段并建立唯一索引，Mongo 自行维护物理 `_id`；只有物理主键语义
完全确定的特殊模型才显式复用 `_id`。生成期必须校验 `_id` 绑定唯一，避免 routeBy/idField 与其他 `_id` 字段产生冲突。
`idField` 在记录创建后不可变：普通 Mongo 增量更新拒绝修改它，upsert 从 `$set` 排除该字段并依靠等值过滤条件写入；
生成 Store 折叠大量变更时也必须跳过记录身份字段。

局部变更最终转换为 Mongo `$set` / `$unset` 文档。Replace 复杂对象时，边界读取当前完整值并整体写入该路径；不能按子字段 set 后遗留旧 BSON 字段。

Mongo 写入不是 setter 热路径，优先使用官方 driver 的 BSON tag 映射保证正确性；只有 benchmark 证明必要时才为具体字段生成更低层编码。

字段索引注解支持三种组合：`mongodb:"index"` 声明匿名单字段索引；多个字段使用相同的
`mongodb:"index:<name>"` 声明按字段顺序聚合的命名复合索引；
`mongodb:"index::<field1>:<order1>:<field2>:<order2>"` 在单个字段上声明显式复合键。
`order`、`unique`、`weight` 和 `expire/expireAfter` 只修饰它们前面最近声明的索引；一个字段可以
通过重复 `index` 同时属于多个索引。普通索引键在元数据中用空路径表示“注解所在 Go 字段”，Mongo 边界再通过
`TypeSpec` 解析为 BSON 名；以 `.` 开头的路径表示当前字段下的相对 BSON 路径，其余显式路径已经是 BSON 路径。

Mongo CMod 在连接和生成类型注册表就绪后，只汇总当前服务 Mongo 路由表覆盖的类型；注册表中属于其他服务且
没有配置对应 `routeType` 的类型不参与本次初始化。相同非空名称的索引按字段声明顺序合并；匿名索引根据最终
BSON 键和顺序生成稳定名称。启动逻辑只创建缺失索引，不自动删除数据库已有索引，也不静默修改同名索引。
已有同名索引与生成规范不一致、同一个命名索引的 TTL 定义互相冲突、索引字段无法解析时，启动必须
fast-fail。多个实例并发启动复用 MongoDB 的幂等创建语义，不为 Mongo-only 服务额外引入 Redis 分布式锁。

Mongo 批量读取用 BSON 类型与编码值构造内部回序身份，不直接把 `any` ID 放入 Go map。
因此重复 ID 可以恢复到全部输入位置，`[]byte` 等合法但不可比较的 BSON 主键也不会触发
`hash of unhashable type` panic；无法编码成 BSON 的定位值必须在查询边界返回错误。

## Fusion 后端

普通 Mongo 更新与 Fusion 延迟落地共用文档级乐观锁原则，但不共用业务数据源：前者在根对象锁内读取 Mongo 版本后生成内存 GD 增量，后者读取 Mongo 版本后加载 Redis 最新完整文档。通用 storage runtime 的 Mongo 非 Upsert 更新要求 `VersionedChangeApplier`；显式 Upsert 走独立原路径。条件失败返回调用方，不自动给旧变更换版本重发。Redis 与 Fusion 热写不经过 Mongo 增量入口。

### 职责

Fusion 是 Redis 热数据与 Mongo 冷数据的组合策略：

- Redis 承担在线热读写。
- Mongo 承担持久化完整文档。
- 延迟任务保存记录定位，稍后把 Redis 当前状态回写 Mongo。

Fusion 不是双写事务，也不是业务事件队列。

### 保存状态机

```text
业务 Store 变更状态
      |
      v
Redis Lua：patch + @v 递增 + 刷新 dirtyTTL
      |
      v
进程内 DelayTaskDispatcher 登记 TypeSpec + RecordKey
      |
      v
当前请求成功并清理 Store 变更状态

后台分发器
      |
      +---- 发布成功 -> 移除本地 pending
      |
      +---- 发布失败 -> error log + 有上限指数退避重试

到期 worker
      |
      v
claim save task -> 获取记录锁 -> 读取 Mongo 版本 -> 读取 @v 和 Redis 当前完整文档 -> 条件 Replace Mongo
      |
      +---- revision 未变化 -> 原子设置 delayDelTime TTL -> complete
      |
      +---- revision 已变化 -> 保留 dirtyTTL -> 重新排 save task
```

延迟任务不保存 GD 快照，也不保存每次变更。十分钟内修改同一角色多次，任务到期只读取一次 Redis 最终状态。

Mongo 版本基线必须在读取 Redis 快照之前取得。条件替换在同一文档写入中匹配 `_tx_version` 并递增，迟到旧请求不能覆盖已推进版本的新文档。文档不存在时使用插入并依赖记录标识唯一索引；已存在但尚无版本时以 0 为基线。更新不允许 upsert 绕过冲突。该元数据仅属于存储层，不包含世界、实例、操作标识，也不要求业务 GD 声明。

超时、条件冲突或确认丢失均沿现有延迟任务重试，从 Mongo 版本读取开始重新执行，再获取 Redis 最新数据。不缓存版本、不保留第二份业务快照，也不在旧快照上修改版本后重发。每次正常后台落地增加一次按记录索引读取 Mongo 元数据的 I/O，Procedure 和 Redis 增量热写不变。此保证要求该记录遵守统一 Fusion 写入路径；绕过框架直接修改 Mongo 不在保护契约内。

### 热写与任务发布

Redis 热文档和 delayed task backend 是两个独立写边界。第一版不引入持久化 outbox 或同槽索引，而是明确划分同步业务提交与后台任务发布：

```mermaid
sequenceDiagram
    participant B as 业务 Save
    participant R as Redis 热文档
    participant D as DelayTaskDispatcher
    participant Q as delayed task backend

    B->>R: Lua patch + INCR @v + 刷新 dirtyTTL
    R-->>B: 写入成功
    B->>D: Submit(TypeSpec, RecordKey)
    D-->>B: 本地登记完成
    B-->>B: 返回 nil，清理 Store
    D->>Q: Enqueue save
    alt 发布成功
        D->>D: 删除相同代次 pending
    else 发布失败
        D->>D: 输出 error log 并退避重试
    end
```

本地 pending 按根记录身份去重，每次 Submit 生成递增代次。分发器复制当前 pending 后执行外部 I/O；只有发布成功且 map 中仍是同一代次时才删除，避免发布期间的新业务写被旧结果误清。下一轮按同一幂等任务键重发 save；任务 backend 合并重复任务，不产生并发 lease。

发布失败日志固定为 `fusion delay task publish failed`，包含 pending 数、重试等待和完整错误链，供外部日志中心告警。退避从一秒开始，倍增到一分钟后持续按封顶间隔重试，并使用随机扰动错开实例压力。业务不配置本地 pending 容量；相同根记录不会膨胀条数，剩余容量由进程内存和运行监控负责。

后台发布循环与关闭排空共享一个可被 context 中断的发布闸门，同一时刻只有一个执行者向 delayed task backend 发布快照。正常关闭时，先停止业务入口、等待已受理工作并完成必要的最终保存，Fusion 随后调用 `DelayTaskDispatcher.Drain`；每条任务只有在 backend 确认接收后才从 pending 删除。排空复用 `runtime.lifecycle.drainTimeout`，超时错误包含剩余 pending 数量。正常关闭遇到排水或必要持久化失败时，`Server.Stop` 保留资源并返回错误；`Server.Run` 间隔一秒自主重试三次，仍失败则记录日志并返回错误，由进程入口非零退出，不把未完成持久化视为成功。租约丢失等致命故障走强制终止路径，不适用保留资源等待重试。

```text
停止业务入口并等待已受理工作
        |
        v
业务最终保存（使用 Fusion 时）-> 新增本地 pending
        |
        v
Fusion Drain -> Redis delayed task backend 确认接收
        |
        v
取消根 WaitGroupContext -> 停止后台循环 -> 关闭存储连接
```

这个方案仍接受一个明确故障窗口：进程在 Redis 热写成功、任务持久化成功之前异常退出，或排空超时后被终止，本地 pending 不可恢复。选择该边界是为了避免每次热写增加持久化索引 I/O、全槽扫描、启动选举和容量闸门。正确性要求转化为两条可运维契约：进程存活时发布失败绝不静默；正常关闭必须先尝试排空；异常退出或排空超时时应排查根因，而不是依赖框架用更复杂的旁路协议掩盖崩溃。

任务载荷使用 JSON 保存定位信息，但 `ID` 与可选 `ShardKey` 不是裸 JSON number。线格式分别记录
`kind` 和十进制 `value`，claim 时据此恢复 `int/int8/int16/int32/int64/uint/uint8/uint16/uint32/uint64`
或 `string` 的原始 Go 动态类型。任务幂等键同样包含 `kind`，因此数值相同但 Go 类型不同的记录键不会碰撞。
例如 `uint32(7)` 不能恢复成 `int64(7)`；`uint64(9223372036854775809)` 在 claim、complete 和 retry
全链路中也必须保持同一个类型和值，不能舍入或改写为科学计数形式。其他动态类型在 backend I/O 前失败，
存储根的 `idField/routeBy` 则由 txgen 在生成期限制为 Go 内建 string/整数，runtime 不负责猜测具名类型。

### at-least-once

claim、lease、retry 和进程重启决定任务是 at-least-once。Mongo 回写必须幂等：同一 RecordKey 的当前 Redis 文档重复 Replace，只能得到相同或更新后的终态，不能重复发奖或累计统计。

重试原因写日志，不写入 Redis error hash，避免不可控错误文本占满内存。退避达到最大值后持续按封顶间隔重试，不能因次数耗尽丢弃持久化任务。

### 租约期间的新写入

Redis delayed task backend 在每个 bucket 中维护六类状态，六个 key 使用同一个 hash tag，因此单次 Lua 可以在 Redis Cluster 中原子访问：

| 状态 | 结构 | 职责 |
| --- | --- | --- |
| `due` | ZSet | 等待 claim 的任务与到期时间。 |
| `payload` | Hash | 每个幂等任务键的最新定位信息。 |
| `busy` | ZSet | 已发放 lease 的任务与租约到期时间。 |
| `token` | Hash | 隔离过期 worker 的 lease token。 |
| `attempts` | Hash | 当前失败周期的处理次数。 |
| `pending` | ZSet | 任务被租用期间再次入队时的后续到期时间。 |

任务被租用后，worker 可能已经读取 Redis 文档并开始写 Mongo；此时普通 Fusion 热写不会持有 worker 的记录锁。若入队只覆盖任务载荷字段 `payload`，旧 worker 随后的 `Complete` 会删除任务状态，Mongo 将永久停留在旧快照。因此 enqueue 在发现 `busy` 时必须同时更新最新任务载荷和后续到期时间，但不能发放第二份并发任务租约。

```text
t1  claim(save role:1001)，worker 读取 name=before-worker
t2  业务写 Redis name=during-worker，并 enqueue 同一任务
t3  enqueue 更新任务载荷，并把后续到期时间写入 pending
t4  旧 worker Replace Mongo(before-worker) 后 Complete
t5  Complete 原子地把 pending 移回 due，并重置 attempts
t6  新 worker 重新读取 Redis，Replace Mongo(during-worker)
```

`Complete` 表示旧处理周期成功，后续任务进入新的尝试周期，下一次 `Attempts` 从 1 开始。`Retry` 表示当前存储故障仍未解除，会采用最新任务载荷、清除单独的后续到期时间标记，并沿当前次数和退避时间继续重试。任务租约超时恢复同样清除后续到期时间标记，立即以最新任务载荷恢复当前任务，避免同一新写入被执行两次。

失效旧租约的 `Complete` 不改变当前任务；`Retry` 则返回 `task.ErrLeaseLost`，不能把未安排重试当作成功。Fusion Scheduler 在 handler 失败且 Retry 返回该错误时，按当前时间加退避间隔调用已有 Enqueue，重新登记记录身份而非旧文档快照。若已有新 owner，沿用后续任务标记，不修改其租约；否则恢复待执行任务。下一次执行重新读取 Redis 最新数据。再次 Enqueue 失败会返回聚合错误，不输出“已安排重试”。该恢复仅增加异常分支 Redis 入队，不增加正常保存或 Mongo 写回 I/O；它不保证连续入队失败、旧写后进程退出或最新 Redis 数据已丢失时仍能纠正。

lease 的 Redis score 使用毫秒时间戳，因此显式 `LeaseTimeout` 必须至少为 `1ms` 且使用整毫秒。亚毫秒值或
带纳秒余数的值若直接截断，可能让新 lease 提前到期并被另一 worker 并发重领；标准 server 在启动校验阶段拒绝，
底层 backend 也在任何脚本调用前复用同一错误防线。

claim 必须先检查本批所有 `due` member 都存在对应任务载荷字段 `payload`，随后才能批量写入 `busy/token/attempts`。缺少任务载荷属于队列状态损坏：Lua 返回显式错误信封，Go adapter 返回错误，且整批状态保持不变。不能先租出部分正常任务再报错，因为调用方拿不到已经创建的任务租约；也不能删除损坏 member 后继续，否则会把待持久化记录静默丢弃。

Redis backend 会轮询多个 bucket，而不同 hash tag 之间不能组成一次原子操作。因此后续 bucket 失败时，`ClaimDue` 同时返回之前 bucket 已成功解析的 leases 和 error。Scheduler 必须像处理 `io.Reader` 的 `n > 0, err != nil` 一样，先逐条调用 handler 并执行 `Complete/Retry`，再向运行循环报告聚合错误。某一条 lease 的 `Complete/Retry` 失败时也继续处理本批剩余 lease；错误结果附带任务 Type、ID、Action，并使用 `errors.Join` 保留所有原始原因。这样不会为了隐藏部分结果额外执行不可靠的 Redis 回滚，也不会让已领取任务只能等待 lease 超时。

### 读取与并发回填

单条读取：

```text
读 Redis
  |
  +-- hit -> 返回
  |
  +-- miss
        |
        v
     获取 RecordLock
        |
        v
     二次读 Redis
        |
        +-- hit -> 返回较新热数据
        |
        +-- miss -> 读 Mongo 全文 -> Lua 仅缺失时回填 -> 返回所需字段
```

RecordLock 只串行化同一记录的回源读取，用来合并重复 Mongo I/O。正常玩家请求仍必须按角色稳定路由到同一个
逻辑 game owner，并由同一 Actor 邮箱串行处理；断线重连也应恢复到同一逻辑槽位。RecordLock 和条件回填都不替代
这条集群规则，也不提供分布式并发控制。

存储层仍应保证回填语义本身正确：adapter 使用一条 Lua 原子检查 main 文档是否存在，并在仍不存在时一起写入
main、已知 subKey 和 `@v`。如果检查时 main 已经存在，Lua 不修改任何热数据，Fusion 丢弃 Mongo 候选并重新读取
Redis。Mongo 返回不存在时也要再读 Redis，覆盖恢复工具、高级存储入口或异常双 owner 在回源期间创建热文档的边界。

```text
t1  Redis miss，取得 RecordLock，锁内复查仍 miss
t2  开始读取 Mongo，得到候选 name=cold
t3  另一个受控入口把 Redis 写成 name=hot
t4  条件回填 Lua 发现 main 已存在，返回 inserted=false
t5  Fusion 丢弃 cold，重读并返回 Redis 的 hot
```

Lua 检查和写入不可拆成 `EXISTS` 加普通 Replace 两次调用，否则热写仍可插入两次 Redis 命令之间。带 subKey 的类型
始终保留至少为 `{}` 的 main 文档，因此 mainKey 是这条热记录统一的存在标记；所有派生 key 共用同一 hash tag，脚本
在原生 Redis Cluster 中仍保持同槽原子执行。

RecordLock 以 Redis `PX` 保存租约，运行时只接受至少 `1ms` 的 TTL。Go 的 `time.Duration`
可以表达纳秒，但 Redis 入口使用毫秒整数；边界必须先拒绝会截断为 `0` 的亚毫秒值，不能依赖 Redis 命令错误兜底。
释放 RecordLock 属于已取得资源的清理动作，不能继续复用已经超时或取消的请求上下文。Fusion 使用
`context.WithoutCancel` 保留链路值并隔离请求取消：例如 Mongo 回源返回 `context.Canceled` 时，业务仍收到原错误，
但 Redis `Unlock` 仍有机会立即删除锁，而不是无谓等待 `PX` 租约自然过期。

批量读取先批量查询 Redis，再批量取得 Mongo 候选。每条 miss 在自己的 RecordLock 内二次读 Redis；锁内已命中的记录
直接丢弃旧候选，仍未命中时也必须走同一条件回填 Lua。若 Lua 输给并发热写则重读 Redis，不能因为候选已经批量取得
就无条件 Replace。这样既保留 Mongo 批量 I/O，也让单条与批量路径共享同一正确性规则。

### 热数据 TTL 与 revision

`delayDelTime` 是已确认回写后的热数据目标保留时间，不是 Mongo 删除时间。Mongo 回源产生、尚未承载业务变更的纯净缓存优先使用正数 `delayDelTime`；未配置或为 `-1` 时使用 `dirtyTTL`，不能生成永久 key。Fusion 业务写入后的脏热数据始终使用独立的 `dirtyTTL` 安全窗口。

延迟保存和 TTL 收敛必须：

1. 获取记录锁后，先读取 Mongo 版本基线，再读取当前 `@v`。
2. 读取 Redis 当前热文档，按基线条件回写 Mongo 并递增持久化版本。
3. Lua 比较读取时 revision。
4. revision 未变化才把 main/subKey/version 的 TTL 一起设置为 `delayDelTime`。
5. revision 已变化说明有新写入，保留新写入刷新的 `dirtyTTL` 并重新排保存任务。

第 4 步只在 `delayDelTime > 0` 时执行；零值或 `-1` 表示不缩短，保留 `dirtyTTL` 作为最终兜底。

Fusion 发布侧只有延迟保存一种合法语义，因此 `DelayedSaveQueue.Enqueue` 接收强语义的 `DelayedSave`，不再暴露可填写任意动作的通用操作结构。该接口与业务侧严格有序的 `util.DelayQueue` 无关。持久化 task backend 仍保存 `Action="save"`，因为 worker 需要验证 Redis 中的任务载荷；未知 action 必须在读取任何业务文档前失败，不能默认按保存处理。

`dirtyTTL` 默认 30 天，并在 Fusion CMod 预检阶段对全部生成 TypeSpec 做配置校验：它必须至少为 `1ms`、使用整毫秒，并且严格大于每个 Fusion 类型的 `delaySaveTime` 和 `delayDelTime`。该规则保证正常调度下任务到期和确认后保留窗口都小于安全 TTL；它不是无限期持久化。配置层、TypeSpec、生成 Store 和 runtime 始终传递 `time.Duration`，只有 Redis adapter 在 I/O 边界转换为毫秒整数并调用 `PTTL/PEXPIRE/PSETEX`；txgen、Fusion 与 Redis 复用 `storage.ValidateExpiringTTL`，不能各自截断或取整。Mongo 或 Fusion 延迟任务队列故障时，Scheduler 持续重试并记录错误；若故障超过 `dirtyTTL`，Redis 可以自然淘汰数据。保存任务读取不到对应 Redis 文档时返回 `storage.ErrNotExists` 并保留任务，不能静默视为成功。

### 健康闸门

运行模式的先验条件：

| 模式 | 必须健康的 backend |
| --- | --- |
| Redis | Redis |
| Mongo | Mongo |
| Fusion | Redis 和 Mongo |

启动时探活并 fast-fail；Fusion 还必须完成 Redis/Mongo、延迟任务 backend、分发器和 Scheduler 的完整装配，任何组件缺失都不能启动业务服务。
运行期由真实 I/O 错误和后台探针维护 backend generation。每次业务调用不额外 Ping。

Redis 与 Mongo 的 Adapter/Cluster 构造器都在启动装配阶段校验完整 client、router 和 shard 集合，不能创建一个等待首次
业务 I/O 才失败的半装配对象。变更写入口把 nil 或 typed-nil `change.Source` 视为调用契约错误；有效 Source 明确返回
`HasChanges() == false` 时才执行无 I/O no-op。该区分避免“未提供输入”和“输入确实没有变化”被折叠成同一种成功结果。

Fusion 任一端已知不可用时，新业务读写返回 `ErrStorageUnavailable`，避免继续接受无法可靠持久化的状态。已经存在的延迟任务不被删除，worker 仍可继续重试，Mongo 恢复后正常消费。

只有 driver 能确认的网络、后端超时和服务端通信错误才包装为 `BackendError`。这条防线收敛在
`storage.NewBackendError`：即使 driver 将调用方自己的 `context.Canceled`、`context.DeadlineExceeded`
传入公共入口，它们也只结束当前请求，不能关闭全局健康闸门；RecordKey、变更流、
BSON、JSON、Lua 协议等本地语义错误同样不能把全局 backend 标成不可用。

## 有状态 Mongo 直接写回

WriteBackManager 合并内存变更，定时调用 Mongo Writer；成功后清除脏状态，失败保留并在后续 Flush 重试。
业务响应表示内存事务执行成功，不表示已持久化。异常退出可能丢失最后一次成功写回之后的内存变化；优雅关闭必须等待最终写回。

Mongo 普通更新使用文档级乐观锁：在根对象锁内先读取 `_tx_version`，再生成当前 GD 字段增量，按基线条件更新并原子推进版本，使用 `majority + j:true` 确认。失败保留变更，后续 Flush 重新读取版本与当前变更，不给旧快照换版本重发。

GD 不需要基础设施字段或逐类型绑定；Mongo 不记录世界槽位、实例身份等部署信息。每次后台写回增加一次版本读取，Redis/Fusion 热写不受影响。路由归属与排水负责约束业务执行者，文档版本不能代替集群归属校验。

该机制不提供跨文档事务或跨服务消息必达。可靠业务消息的消费确认与持久化衔接由后续独立设计负责。

## 单机事务

### 职责

`modules/txn` 为有状态单机服务提供：

- 多个 GD 根对象的稳定加锁。
- Store 受控修改的 undo log。
- savepoint 和子 Procedure。
- 提交时最终变脏根对象的包内收集。
- 在修改前调用可选 `CommitParticipant.Prepare`，并在提交或回滚时完成确认。

它不提供：

- 分布式锁。
- 跨进程事务。
- Redis/Mongo ACID。
- 裸 public field 写入的事务回滚。

无状态分布式游戏服务可以完全不使用该模块。

### Procedure 生命周期

```go
type Procedure interface {
    Locks(context.Context) ([]LockTarget, error)
    Process(*Tx) error
}
```

`Locks` 只声明当前 Procedure 直接访问的锁，不要求父过程展开全部子过程。一次根调用：

```text
根 Procedure.Locks
      |
      v
校验、排序、去重根直接锁集
      |
      v
按顺序获取精确 Key 锁
      |
      v
校验 pending 变更契约
      |
      v
创建 Tx 并绑定 Store undo recorder
      |
      v
Process
  |
  +-- tx.Call(child)
  |       |
  |       +-- child.Locks：复用已持有锁，获取新的直接锁
  |       +-- 为 child 建立 savepoint
  |       +-- child 失败只回滚到 savepoint
  |
  +-- 根 error：反向 undo
  +-- 根 success：丢弃 undo
             |
             v
      解除全部 Store recorder
             |
             v
      反向释放根与动态锁
             |
             v
      AfterRollback / AfterCommit
```

AfterCommit 在释放数据锁后执行。它不能回滚已经提交的内存状态；需要可靠投递时应使用项目选择的 outbox 或可靠消息队列。
Procedure 可能在死锁受害者重试中再次执行，因此对象字段应只保存不可变输入，外部 I/O 和消息投递必须登记到 `AfterCommit`。

### LockKey 与 LockTarget

`LockKey{Scope, Value}` 是单机一致性身份。`LockTarget` 额外包含：

- `Order`：锁层级，越小越先获取。
- `Store`：绑定 undo recorder 的根 Store。

例如军团交易：

```text
Order 10: guild:7
Order 20: role:1001
Order 20: role:1002
```

每个 `Scope` 在项目级注册表中占用唯一 `Order`。框架对每次 `Locks` 返回的直接锁批次按 `Order -> Value` 全序获取：先按锁域层级获取大锁和小锁，再在同一锁域内按对象键稳定排序。静态顺序会显著减少循环等待，但运行时子过程可能在父过程持锁后才发现新锁，因此等待图仍负责检测无法提前消除的动态环路。相同 Key 使用不同 Order 是定义错误；相同 Key 绑定不同 Store 也是错误。

生成对象的 `Order` 来自项目级事务锁域顺序注册表。txgen 在生成期把 `scope -> order` 编译进 `LockTarget()`；tx runtime 只消费整数和 `LockKey`，不读取项目 YAML，也不维护锁域名称映射。该生成期注册表与下文进程级精确 Key 锁注册表职责不同：前者定义锁域先后，后者负责每个具体对象键的互斥和生命周期。

若业务在根过程前已经知道完整对象集合，仍应使用 `LockSet` 一次获取，以减少等待和重试。若锁目标只能由运行时筛选结果或子 Procedure 决定，则由 `tx.Call(child)` 动态获取；框架通过等待图保证不会真正进入循环等待。

### 子 Procedure 与不可重入

Go `sync.Mutex` 不可重入。框架也不通过 goroutine ID 或第三方可重入锁改变调用模型：

- 嵌套 `txn.Run` / 根 `txn.Call` 被拒绝。
- 子逻辑使用 `tx.Call(child)`。
- child 声明已经持有的 Key 时只校验 Store 身份，不重复获取。
- child 声明新 Key 时按自己的直接锁批次排序并获取。
- 新锁与根锁一起持有到根事务提交或回滚，符合严格两阶段锁。
- child 业务失败只回滚自己的 savepoint；死锁受害者错误会把根事务标记为必须回滚。

这样可复用 Procedure 不依赖调用方展开实现细节，也不会因为重复声明同一 Key 而等待自己。

### Undo 与 savepoint

Store setter 在绑定 recorder 时先登记反向操作，再修改当前数据和变更状态。undo 闭包只捕获该操作恢复所需的局部旧值和局部变更状态，不复制整个 GD。

`tx.Do` 和 `tx.Call(child)` 进入前记录 savepoint：

```go
type savepoint struct {
    undos         int
    afterCommit   int
    afterRollback int
}
```

子调用失败时，从 undo 尾部反向执行到 savepoint，并截断该调用注册的回调。根调用失败则回滚到空 savepoint。

事务内必须使用传入锁目标对应的同一个 Store 实例。否则 undo recorder 与实际修改对象不一致，框架会拒绝子 Procedure 的 Store 绑定。动态 Store 在锁获取和 Runtime 容量预留成功后才绑定 recorder；若 child 尚未执行就失败，新锁可以立即释放，不会产生半绑定状态。

## 事务精确 Key 锁注册表

### 进程级全局结构

事务精确 Key 锁注册表是进程级全局对象，不属于单个 Procedure：

```go
var transactionLocks lockTable

type lockTable struct {
    shards     [64]lockShard
    entryPool  sync.Pool
    waiterPool sync.Pool
    waitsMu    sync.Mutex
    waits      waitGraph
}

type lockShard struct {
    mu      sync.Mutex
    entries map[LockKey]*lockEntry
}
```

同一进程的所有 `txn.Run`、根 Procedure 和 `txn.WithLocks` 共享它。另一个进程拥有自己的注册表，因此不能提供分布式互斥。

### 64 分片的准确含义

64 个 `lockShard.mu` 只保护注册表 map，不是固定业务锁。真正业务锁是每个完整 LockKey 对应的动态 `lockEntry`：

```text
shard 18
  |
  +-- role:1001 -> entry A -> owner / waiters A
  +-- role:9009 -> entry B -> owner / waiters B
```

即使两个 Key 哈希到同一个 shard，它们只在 reserve/release map 的极短临界区内竞争；事务执行期间分别由 entry A 和 entry B 记录所有权，可以并行。

这与固定分段业务锁不同：

```text
固定分段业务锁：hash(key) -> Mutex，碰撞 Key 共用整段事务锁
当前精确 Key 注册表：hash(key) -> map shard -> map[key] -> 独立 entry
```

分片数量只控制注册表元数据竞争和固定 mutex 成本，不限制可同时存在的业务 Key 锁数量。

### 锁所有权与可取消等待队列

```go
type lockEntry struct {
    mu      sync.Mutex
    owner   TransactionID
    waiters *lockWaiter
    tail    *lockWaiter
    refs    int
}
```

`owner == 0` 表示空闲。无竞争获取只短暂进入该条目的局部 mutex 并设置 owner，不进入全局等待图；不同 Key 的条目互不阻塞。发生争用时，请求者才创建或复用一个 waiter，登记等待边并进入条目的 FIFO 队列：

```text
transaction 18 --等待 role:1002--> transaction 23
                                          |
                                          +-- lockEntry.owner = 23
```

waiter 使用容量一的 `signal` channel 接收所有权转移，同时通过调用方 context 响应取消。等待图按 FIFO 前驱关系建边：队首等待当前 owner，后续 waiter 等待自己的前一个 waiter。释放者只需删除队首等待边、转移 owner 并发送 signal；中间 waiter 取消时，也只需把直接后继改接到自己的前驱。这样既不会保留指向已释放事务的陈旧关系，也避免每次授权遍历整条队列形成 O(n²) 成本。取消和授权同时发生时，条目状态决定唯一结果；若 context 已取消但所有权已经转移，请求者会立即释放该锁，不进入 Procedure。

`signal` 只负责唤醒，不代表锁本身；锁的唯一事实来源是 `lockEntry.owner`。因此死锁诊断不需要猜测 channel 中是否还有 token，也不依赖 goroutine ID。

### reserve、refs 与回收

reserve 流程：

1. 根据完整 LockKey 哈希选择 shard。
2. 短暂获取 shard mutex。
3. 精确查询 `entries[key]`。
4. 不存在时从 pool 取得或创建 entry。
5. `refs++`。
6. 释放 shard mutex。
7. 尝试取得 owner；只有发生争用时才进入等待图和队列。

`refs` 统计已经 reserve 的持有者和等待者。它保证仍有人引用 entry 时不会从 map 删除或回收到 pool；owner 和等待队列由 entry mutex 单独保护。

释放所有权后再在对应 shard 下 `refs--`。归零后删除精确 Key 映射，并把 entry 放入 `entryPool`。争用 waiter 也由 `waiterPool` 复用，稳定争用路径不会持续制造 channel 和节点分配。

### 单次事务持锁结构

全局注册表只负责定位锁。单次事务使用 `heldLockSet` 保存已经取得的 entry：

```go
type heldLockSet struct {
    inline   [4]heldLock
    overflow []heldLock
    count    int
}
```

四把以内的常见锁集直接放在值数组中，不分配 held-lock slice；更多锁才使用 overflow。失败或结束时按逆序释放，确保中途超时不会遗留已持有锁。

### 为什么不使用成熟可重入锁

当前需求是 Procedure 直接锁、动态扩锁、context timeout 和子 Procedure 复用，而不是任意嵌套重入。第三方可重入锁通常需要识别 goroutine 或改变调用模型，也不能自动发现多个根事务之间的动态扩锁死锁。禁止嵌套根事务，并让 child 通过同一个 `Tx` 复用 owner 身份，边界更明确。

### 等待图与技术选型

本轮先比较了四类方案：

1. **根过程静态展开完整子树。** 无运行时死锁检测成本，但运行时筛选玩家、条件分支和可复用 Procedure 必然让父过程知道全部子过程实现细节，破坏开闭原则，因此不再作为核心契约。
2. **Wait-Die / Wound-Wait。** 两者都用事务年龄打破循环；前者让年轻请求者回滚，后者允许老事务使年轻持有者回滚。它们适合高冲突数据库调度，但需要维护年龄和跨执行单元受害者状态，重试率受负载影响，也不能直接给出当前完整 Procedure 等待路径。tx 第一版不采用。
3. **通用图库。** Gonum、`dominikbraun/graph`、`yourbasic/graph` 能提供 DFS、拓扑和环检测，但它们面向任意多出边图，带来通用节点/邻接结构、遍历器或 visited 集合。事务同步执行时一个根事务同一时刻至多等待一把锁，通用结构超出需求。
4. **单出边等待图。** 当前采用。`waitsFor[waiter] = edge{blocker, lock, procedurePath}`；新增边前沿 blocker 链做指针追踪。现有图始终保持无环，所以首次回到 waiter 就得到唯一完整等待环。

等待图本身是无锁纯算法；`lockTable.waitsMu` 在争用边界保护它。无竞争锁不访问 `waitsMu`。发现环时，本次新增等待边不会写入图，请求当前边的根事务成为受害者：

```text
T1 [PGuildTrade -> PGrantItem] --role:1002--> T2
T2 [PRefreshGuild -> PLoadOwner] --guild:7----> T1
                                      |
                                      v
拒绝 T2 的新等待边，返回 DeadlockError.Cycle
```

`DeadlockError.Cycle` 的每个 `DeadlockStep` 都包含 `Waiter`、`Blocker`、`Lock` 和 `ProcedurePath`。`Blocker` 可能是当前锁持有者，也可能是 FIFO 队列中必须先推进的前驱事务；它准确表达等待图中的直接依赖，不把排队者误报为锁 owner。正常链路不构造诊断切片；只有确认成环时才分配完整链。错误可通过 `errors.Is(err, txn.ErrDeadlock)` 和 `errors.As(err, *txn.DeadlockError)` 识别。

标准 `txn.Call` 与 `txn.Runtime.Call` 最多自动重试三次死锁受害者。每次重试前完整 undo、解绑 recorder、释放锁和 Runtime 容量预留，并输出带完整等待链的 warning。`CallWithOptions` 供高级调用方设置 `DeadlockRetries`；零表示不重试。锁等待超时仍返回 context deadline/cancel 错误，不混同为死锁。

框架不会跨 goroutine 强制回滚另一个正在执行的根事务。当前请求者只让自己的根事务失败，符合 Go 同步调用和局部 undo 的所有权边界。Procedure 如果捕获 `tx.Call` 的死锁错误，根事务仍被标记为必须回滚；业务应立即返回该错误，不应继续外部副作用。

### 基准结论

2026-08-26 在 Intel i9-9880H、darwin/amd64、Go 1.26 上对改造前后各运行多轮。无竞争路径保持零分配并有所下降；等待图仅在争用时参与：

| 场景 | 改造前 | 等待图版本 | 分配 |
|---|---:|---:|---:|
| 单 Key 串行 | 267.7-286.1 ns/op | 221.0-264.3 ns/op | 0 allocs/op |
| 四 Key 串行 | 1.049-1.115 us/op | 0.890-1.082 us/op | 0 allocs/op |
| 离散 Key 并发 | 50.0-70.1 ns/op | 48.0-58.1 ns/op | 0 allocs/op |
| 同 Key 高争用 | 447.8-455.5 ns/op | 0.898-1.061 us/op | 0 allocs/op |

同 Key 极限争用约为旧实现的 2.0-2.4 倍，代价来自 owner、FIFO 公平唤醒和等待边维护；这条基准只测锁的取得与释放，没有任何业务处理，因此放大了协调成本。争用路径仍严格串行且稳定零分配。无环等待边的纯算法约 56-67 ns/op、零分配；确认两事务环约 182-209 ns/op、216 B/op、3 allocs/op，分配只用于完整诊断链。根 Procedure 约 1.28-1.42 us/op、1160 B/op、9 allocs/op；child 复用根锁约 1.44-1.70 us/op、1192 B/op、10 allocs/op；child 新增一把动态锁约 2.42-2.66 us/op、1481 B/op、14 allocs/op。第一版接受热点冲突路径的显式成本，以换取动态 Procedure 和完整可诊断等待环；后续只有真实服务 profile 证明同 Key 锁协调占主要耗时，才优化 waiter 协调，不能删除完整等待链。

数字属于特定机器和 revision，后续判断应重跑：

```bash
go test ./modules/txn -run '^$' \
  -bench '^(BenchmarkLockTableAcquireRelease|BenchmarkWithLocks|BenchmarkProcedureCall|BenchmarkWaitGraph)$' \
  -benchmem -count=5
```

不要为了优化注册表而退回“哈希碰撞共用业务锁”的固定 stripe 方案。

## 有状态事务与缓存运行时

`txn.Runtime` 是 Procedure 执行器，只负责锁、undo、savepoint、死锁检测和提交回调。最终变脏根对象在根事务仍持锁时确定，但不作为公共 API 暴露。Runtime 不持有 GD 缓存、存储 writer、Pending 集合或后台任务。

`modules/writeback.Runtime` 是 CModProcedure WriteBack 组件使用的稳定 SPI，组合 `txn.CommitParticipant`、后台生命周期、最终刷盘和实体释放契约。`modules/writeback.Manager` 是 tx 提供的默认实现，项目 WriteBack 组件在启动期创建或注入 Runtime，并由 CModProcedure 把它绑定为 `txn.CommitParticipant`：

1. `Prepare` 在 `Process` 前校验 `LockKey -> Store` 身份、存储准入和容量，并预留根对象。
2. Procedure 回滚时 `Abort` 释放预留；提交时无失败地登记最终变脏根对象。
3. 多次事务继续在同一 Store 状态树上累计，Pending 按根 `LockKey` 去重。
4. Flush 再次取得相同实体锁，在锁内同步访问 Store 与 GD 当前值；写入成功才清理，失败保留。
5. 根对象不再由 Actor 或场景持有时，必须在 Procedure 外调用 `TryRelease`。它取得相同实体锁并在锁内确认 Pending、活跃事务、预留和 Store 状态；只有返回 `true, nil` 才解除身份并允许回收。

默认 Manager 不复制 GD，也不把引用 GD 的变更状态交给无锁异步写队列。同一 Manager 内相同 `LockKey` 不允许绑定不同 Store；对象可以由业务自行创建或加载，不要求经过指定加载函数。Store 不是并发安全容器，业务不能在实体锁外轮询 `HasChanges` 观察后台 Flush。

默认 Manager 不是完整的业务对象缓存，不定义 `LoadRole`、容量淘汰、Actor 归属、业务分片或二级索引。静态 `Config` 只保存调度和容量参数；writer 与写入闸门通过 `writeback.Dependencies` 在 CModProcedure 启动期注入。项目按下面的边界扩展：

1. 业务对象加载、唯一实例和工作集淘汰属于项目 EntityCache；它独立于 WriteBack Runtime，只通过项目 facade 调用 `TryRelease`。
2. 有状态项目使用默认 Manager，按 GD 存储模式选择通用 Writer。特殊生命周期仍可通过 writeback.Runtime SPI 装配。

WriteBackManager 不绑定固定持久化目标。CModProcedure 启动期向项目 Factory 注入窄依赖，项目按已显式选择的服务组合决定 Writer：

| 服务形态 | L3 CMod | WriteBackManager Writer | 提交成功边界 |
| --- | --- | --- | --- |
| 无状态请求服务 | Fusion，不选择 CModProcedure | 无 WriteBackManager；业务调用 `db.Save(role)` | Redis 热写和本地延迟任务登记成功 |
| 有状态逻辑服 | Fusion、CModProcedure | mode-aware Fusion Writer | Procedure 只提交内存；帧 Flush 成功写入 Fusion 后清理 Store |
| 有状态大世界 | Mongo、CModProcedure | tx Mongo 版本条件增量更新 | Procedure 只提交内存；帧 Flush 直接写 Mongo 成功后清理 Store |

项目 Factory 只能取得 Writer 与写入准入，不取得私有装配内核、Mongo client 或生命周期所有权。
CModProcedure 不会自动选择 Fusion 或 Mongo；L4 服务必须显式列出作为目标 Writer 的存储 CMod。

业务负责槽位所有权与对象加载；存储层负责原子写入条件，不持有第二份 GD。

因此依赖方向固定为：

```text
txn.Runtime -> txn.CommitParticipant <- writeback.Runtime SPI
                                  ^
                                  |
                     writeback.Manager 默认实现
                                  ^
                                  |
                    项目 writeback 可选装饰器
```

CModProcedure 不持有具体 `*writeback.Manager`，项目 facade 也不能要求调用方取得该指针。完整替换 Runtime 属于高级扩展面；实现必须保持 `Prepare` 先于业务修改、`Commit` 不失败、失败 Flush 不清理变更、同一实体锁内读取 Store 等契约。

`txgameslg` 的 WriteBack 组件会先保留 facade 所有权，再绑定 CommitParticipant 并启动 Runtime；只有 `Runtime.Start` 成功后才开放业务 facade。开始 Drain 时先关闭 facade 准入并等待已经进入的调用完成，再执行 `Runtime.Shutdown`。`Start` 返回错误时组件会在 `ShutdownFlushTimeout` 内调用同一实例的 `Shutdown` 清理部分启动资源；本次 `Server.Start` 随即进入终止回滚，该 Server 实例不可再次启动。

直接写回按“存储 -> Transaction -> WriteBackManager”启动；排水封口并完成已接纳业务后，WriteBackManager 最终 Flush，成功后才停止依赖。
失败保留 Store 与 pending，按既定有限关闭重试处理。新实例确认槽位归属后加载数据并开放 Actor，不向 Mongo 声明集群所有权；后续写回使用临时读取的文档版本条件。
缺失记录必须由业务创建；存储层不凭空创建场景数据。

### 容量保护

容量保护分为：

- `MaxRootChanges`：单根变更位置超过预算时，只把实际变脏的顶层分支收敛为 Replace。
- `MaxPendingRoots`：可选的待刷根对象数量硬限制。
- `HeapSoftLimitBytes`：进程 Heap 软告警，不拒绝业务。

`MaxRootChanges` 通过 Manager 的 `Source` visitor 装饰器执行。未超预算时，adapter 在构建完整内存文档的同一次遍历中完成计数，随后才执行第一次外部 I/O；超预算时 visitor 返回内部错误，adapter 丢弃尚未写出的临时文档，Manager 先记录当前状态树中实际变脏的顶层分支，再清理细路径并只把这些分支收敛为 Replace。比如 `Items[1].Quantity` 与 `Items[2].Quantity` 超预算后只输出当前完整 `Items`，不会把部分加载对象中未修改的 `Name`、`Power` 零值写入数据库。这个契约要求所有 `storage.ChangeApplier` 都先完整访问变更并处理错误，再进行外部 I/O，不能边遍历边写远端。

HeapAlloc 受整个进程影响，不能作为 Manager 拒绝服务的唯一依据。外部存储不可用时，应由项目健康闸门先拒绝新的持久化业务；已有待刷根对象保留并等待恢复。

## 生命周期

### WaitGroupContext

`lifecycle.WaitGroupContext` 统一提供：

- 根 context 取消信号。
- 后台 goroutine 注册。
- 停止新任务后的等待。
- 子生命周期组织。

优雅关闭不是立即 cancel 一切。正确顺序通常是：

```text
入口排水，停止接受新请求
        |
        v
等待已受理 handler / Procedure / mailbox 工作完成
        |
        v
停止项目服务并执行缓存管理器最终 Flush
        |
        v
停止共享 RPC，排空 Fusion 本地延迟任务登记
        |
        v
取消后台循环并等待退出
        |
        v
关闭 Actor、服务发现、存储 client 等物理资源
```

最终 Flush 使用独立限时 context，不能因为运行 context 已取消而立刻失败。超时必须返回错误并输出当前可观测状态，不能无限等待或静默退出。

控制面排水还有一份独立的服务端 `drainTimeout`。`lifecycle.Ingress` 原地维护已受理但未完成的工作计数，并通过
一次性完成 channel 让等待响应 context；它不为每次等待创建 goroutine。超时错误必须带服务名和 pending 数量，
`drainCoordinator` 将状态置为 `failed` 并记录错误。运维客户端自己的 RPC timeout 只限制客户端等待，不能替代
服务端上限。服务端超时只结束控制面的本次等待，不会重新开放入口，也不会把未完成工作从账本移除；后续显式
`Server.Stop` 仍会使用自己的 context 重新等待 pending 清零。排水失败后不能继续正常滚动发布，避免在仍有已受理
工作时把失败误判成安全关闭。

Actor 宿主在同一锁边界登记准入与 pending，覆盖加载、邮箱排队、处理和卸载。正常关闭先封闭新入口并等待真实工作返回，随后完成业务后台任务和最终持久化，再释放通信资源。超时保留资源供既定有限重试，不撤销已接受工作。

### 排水与关闭的区别

排水是入口策略：不再接受新的外部工作。关闭是资源生命周期：等待已进入工作，停止循环，Flush 并释放资源。

进入 draining 不应全局禁止所有数据库保存或 msgq 下行，否则已经受理的业务无法完成。Agent 上行和通用 RPC 入站通过 admission gate 封口；内部发送方收到 unavailable 后按自己的业务语义重试、补偿或丢弃。

## 分布式服务模块

### 进程运行时装配

Bee 或 Ant Server 是每个物理进程唯一的公开运行时宿主。项目先把 yml、环境变量和命令行转换为强类型
`bee.Config/bee.RunOptions` 或 `ant.Config/ant.RunOptions`；此后由对应 Server 按固定顺序装配并管理：

```text
project config parser
  -> bee.Config / ant.Config
  -> bee.New(...).Build() / ant.New(...).Build()
  -> select services
  -> Storage + shared RPC Host
  -> Discovery / Actor / CModProcedure when configured or required
  -> generated data binding + project facade setup
  -> health workers + selected services
  -> RPC Serve + control ready + discovery running
```

三个 `RunOptions` 都不暴露人工 `InstanceID`。未选择 Discovery 的纯单机进程保留空实例 ID；一旦依赖闭包选择
`CModDiscovery`，Discovery 必须在任何 Service Factory 运行前完成 `workSlot` 申请，并把
`<AppID>-<workSlot>-<ModRevision>` 冻结为本次进程身份。`workSlot` 是租约保护的短期资源，不得用于替代稳定
稳定业务槽位，后者仍由部署控制面维护 assignment 和 generation。

项目不实现自己的 Host 或装配 Runtime。`txgame/server/internal/app` 与 `txgameslg/server/internal/app` 只是
composition root：注册生成数据、安装 `db` facade、注入身份解析/业务分发回调和声明可选业务服务。项目专属 SPI 与业务
逻辑随所属 L4 Service 放在 `server/services/<name>`；`bee/services/agent`、`bee/services/msgq` 及其 Ant 对称门面是 tx
提供的标准服务注册器；项目配置转换会延迟到对应服务真正被当前进程选中时执行，
未选服务的专属配置不能阻止其他服务启动。

`runtime.lifecycle.drainTimeout` 属于所有 L4 服务共享的进程生命周期配置，不由某个 CMod 或服务启用。入口等待与 Fusion 本地 pending 排空分别使用这项单阶段上限，零值使用 5 分钟默认值；txgame 模板显式配置该值，便于发布环境审阅。

每个 `ServiceRegistration` 用 `ServiceSpec` 声明稳定名称与框架能力依赖，并在 Factory 中取得受控 Context、构造 Service；
`Service.Start(context.Context)` 只启动本服务资源。Context 只暴露消费能力，不暴露进程资源所有权；普通业务 Handler 应使用项目 facade。
服务启动失败时先清理失败服务自身的部分资源，再按已启动顺序逆序回滚。回滚会先取消 Service Tasks，再调用幂等
`Stop` 解除 listener 等资源阻塞，最后等待任务退出；正常退出因 Drain 已先封口入口，顺序固定为：

```text
service ingress + actor Delivery admission close
  -> discovery draining
  -> service accepted-work wait + actor mailbox drain
  -> 取消并等待各 Service 自有 Tasks
  -> writeback manager final flush
  -> shared RPC graceful stop
  -> fusion delay-task pending drain
  -> 停止控制面与 Service
  -> project cleanup and generated storage unbind
  -> discovery stopping
  -> root WaitGroupContext cancel and wait
  -> actor shutdown
  -> discovery close
  -> storage close
```

Drain 期间不会提前取消根 `WaitGroupContext`，也不会关闭 msgq 下行所需的共享 RPC Host。只有服务入口和 Actor
业务邮箱完成封口并等待已受理工作后，Stop 才取消并等待各 Service 通过 `Tasks()` 登记的后台任务，然后进入
最终 Flush，成功后才停止项目服务与释放资源。这样 Service 后台任务不能在 CModProcedure 最终刷盘后再次提交内存变更。

正常 Stop 在入口排水或组件 Drain 失败时返回错误，不继续释放资源或注销运行身份，入口也不重新开放。组件 Drain
按逆依赖顺序执行，失败即暂停；再次调用从失败组件继续，不重复已成功的组件。Run 在首次失败后间隔 1 秒重试三次，
各轮记录 AppID、尝试次数和具体错误。全部失败后记录数据丢失风险并返回错误，进程入口打印错误并以非零状态退出，
不再尝试无限关闭；这属于异常退出，不是安全交接成功。直接 Stop 不负责退出进程。启动回滚及运行身份租约丢失的
异常终止允许带错误释放资源，不套用正常更新时保留资源重试的规则。

该有限重试策略是当前 protoActor 选型下的关闭基线，不是要求未来 Actor 运行时永久沿用的兼容契约。
若后续决定切换 GoAkt，须先验证其准入封口、已受理工作排空、Actor 停止与重新分配之间的状态协调，
以及滚动更新和故障条件下的实际保证。只有确认它能从机制上解决对应排水一致性问题，才依据 GoAkt 的
生命周期方案重新设计关闭策略，并移除被替代的代码、测试和说明；当前不提前实现替代方案。
Actor 运行时的状态协调不能自动替代业务最终持久化，刷盘失败、异常退出风险和日志留存仍须独立评估。

实例只有在共享 RPC Host 和可选控制面都成功启动、discovery 成功发布 `running` 后，才把本地
`Context.Instance().Status` 切为 `running`。任一步失败都不会发布 `running`，而会进入启动回滚并转为
`stopping`；不能让服务在控制面尚不可用时提前进入普通路由。Drain 先封口本地服务与 Actor 入口，再向 discovery 发布 `draining` 并同步本地快照；即使远端
状态更新失败，本地入口仍会排水且错误会明确返回，调用方不能把结果当成成功。

### 总体拓扑

当前实现的整合收益、业务 API 与高级 SPI 边界见 [NATS 迁移收口说明](audits/2026-09-20-nats-integration-closeout.md)；以下拓扑仍描述完成态，不混入阶段进度。

架构以两条通信路径组织：基础会话层使用直连 gRPC 承接所有业务服务的高流量上行、邮箱接纳确认和会话下行；实体扩展层使用 Core NATS 承接按实体身份寻址的 RPC，并以 JetStream 可靠任务扩展异常后仍需消费的业务。两者共享 tx 的实体归属、加载准入和本地宿主，不让业务处理传输主题或实例交接细节。

本节描述完成态分层拓扑，不记录建设进度；实现与验收状态单独维护在[归属接线计划](superpowers/plans/2026-09-13-actor-ownership-integration.md)。

![tx 游戏服务集群双通信层拓扑](images/server-topology.png)

[可编辑 SVG](images/server-topology.svg)。图采用分散的服务池布局，基础四类节点统一浅蓝，扩展节点统一浅紫；绿色表示上行、红色表示下行、橙色表示计算 gRPC、紫色表示 NATS；所有上下行相关链路均使用实线，包括集群内部的直连上下行与会话确认；业务服务间计算调用和实体互调使用虚线。上行由 Agent 引出主线后分支指向各目标；Game、SLGWorld、FightRoom 的下行先在图中汇线，再用一个箭头指向 MsgQ。上下行主线相邻布置，汇线仅简化直连关系的画法，不增加中转节点或广播总线。箭头分别表达单向请求或发布，普通 RPC 结果沿同次调用返回；圆点标记有意汇线，其他交叉不连接。框中的多个服务器代表实例集合，不代表固定数量、实例一一配对或消息广播。Core 与 JetStream 是同一 NATS 服务端体系的两种消息能力，图中分框用于区分完成语义，不要求部署两套独立 broker。

#### 基础四类服务与会话数据流

Login、Agent、MsgQ、Game 是基础组合中的四类逻辑服务，可以同进程组合，也可以独立多实例部署；不是 tx 内核硬编码必须启动的四个进程。Login 与 Game 的认证策略、账号角色模型和业务处理由项目侧提供，tx 提供通用服务宿主与通信能力。

1. 客户端通过 HTTP 登录入口完成认证；生产可在入口终止 TLS。Login 返回绑定目标 Agent 的短期票据、SessionID 和公开地址。这里不要求 Login 先通过 RPC 创建 Game Actor。
2. 客户端与所选 Agent 建立 TCP/WebSocket 长连接；Agent 验票并通过 MsgQ 初始化会话。Login 不进入后续每条业务消息的数据路径。
3. Agent 按生成命令及路由键解析实体归属，通过 gRPC 直达 Game、SLGWorld、FightRoom 等命令所属服务，不经 NATS，也不要求 Game 中转。Game 接收长流请求时按流顺序把消息放入对应 Actor 邮箱，并在入队后返回仅供 Agent 推进 Rec 状态的接纳确认；不等待业务执行结果，也不向客户端发送上行确认帧。
4. Game、SLGWorld、FightRoom 等业务服务分别通过 gRPC 直接向 MsgQ 发布下行，不经 NATS，也不要求回到 Game：普通角色响应使用 `PubRole` 扇出到该角色的多个 Session，请求级错误使用 `PubSession` 精确发布。Agent 通过 MsgQ 的 Rec/Ack 接口协调序号，并对远端 MsgQ 每个 Session 建立一条 gRPC 下行读取流，再发送客户端帧；同进程 MsgQ 继续直接读取本地邮箱。

服务池之间可以多对多通信，但某个 Session 的当前 Agent 连接和 MsgQ owner、某个 Actor 的当前归属均是明确的。多 Session 向同一 Role 扇入时由目标实体邮箱串行化，不把 Agent 设成全局串行器。相同 Role 的多个会话下行由 MsgQ 会话机制扇出。

MsgQ 是客户端会话邮箱，正文保存在实例内存中；它不承担 JetStream 的持久化业务任务职责。**直连上下行是所有业务服务共同遵守的边界**：新增实体服务只需接入相同的上行分派与下行发布能力，不因此增加 broker 跳数。可靠任务触发了客户端通知时，任务消费之后的通知仍走业务服务→MsgQ→Agent 的直连下行链路。

#### 实体扩展层与可靠任务

增加 SLGWorld、FightRoom、公会或匹配等服务池时，项目声明实体类型、路由键、业务加载与生命周期策略；tx 维护身份到槽位/实例的解析、容量准入、邮箱及排水，NATS 承担向已选定实例传输消息。订阅不取代 tx 的权威归属，也不把实体随机分配给 Queue Group 中任一消费者。

例如 Role 消耗加速道具后，按 World 的身份调用行军加速；Game 与 SLGWorld 实例池可按各自负载独立扩缩容，不绑定一比一，也不按每实例固定世界数限制扩展。SLGWorld、FightRoom、公会与匹配等是按业务职责扩展服务池的例子。实体扩展不意味着每种业务都必须再建一个服务，基础 Game 也可以承载多个 Kind。

| 路径 | 完成语义 | 适用职责 |
| --- | --- | --- |
| 直连 gRPC 上行 | 按连接顺序等待目标 Actor 邮箱接纳；接纳确认只用于 Agent 内部 Rec 游标，不表示业务完成 | 高频客户端命令，减少 broker 转发流量 |
| 直连 gRPC 下行 | 业务服务直接发布到 MsgQ，Agent 协调 Fetch/Ack 并发送客户端 | Game、SLGWorld、FightRoom 等共用；不经 NATS 或 Game 中转 |
| Core NATS Actor RPC | Tell 本地投递；TellAccepted 仅确认到达 NATS；CallFuture/Call 等待远端执行结果 | 正常运行期间的实体交互，不保证持久化或失败后必达 |
| JetStream 可靠任务 | 发布持久化确认与消费 ACK 分开；业务执行后确认，失败按既定重试/死信策略处理 | 需要利用 MQ 持久化能力跨重启继续消费的任务 |

可靠任务可以复用实体路由和业务分派，但不能简单把普通 RPC 加上持久化开关就宣称可靠完成。既定任务 ACK 不等待 GD 刷盘，也不通过新增完成表或 GD 字段消除执行成功后 ACK 丢失的重复窗口。普通 RPC 的超时也不能证明业务未执行。

#### 高频计算服务也可通过 gRPC 扩展

扩展服务不必全部采用 Actor 模型。Battle 负责战斗公式或独立数值计算，与持有战斗状态和生命周期的 FightRoom 分工不同：Game、SLGWorld、FightRoom 可以直接通过 gRPC 调用 Battle，客户端计算请求也可以由 Agent 的命令适配入口转为同类调用。该高频链路不经 Core NATS 或 JetStream，也不要求先创建 Actor。

现有扩展点是 `CModRPC().Register`：服务在启动期向共享 Host 注册自定义 protobuf service，声明 RPC 与所需 Discovery 能力。调用侧可以使用普通 `rpc.Sender.Call` 的 `ServiceRoute` / `InstanceRoute` 与 `CallReceiver` 接入通用信封，也可以使用独立 protobuf service 和对应客户端。后者的连接/选址装配需由服务组合根明确提供，不能将普通 Sender 当作任意生成客户端的自动工厂；同一 Host 上的通用 DeliveryService 仍只能注册一次。

Battle 的纯计算结果沿同一次 gRPC 返回调用方，不必理解 Session 或承担客户端消息发布。客户端经 Agent 调用时，命令适配负责认证、参数校验及既有下行发布契约，不能把纯计算 Result 冒充邮箱接纳确认。计算服务也可以按业务需要提供独立对外 API，但这不等于可以跳过入口鉴权或直接开放内部 gRPC 端口。此处约定计算服务的扩展方式，不额外实现公式或 Battle 服务。

Battle 集中处理哪些关键计算、前端保留哪些本地计算属于业务侧取舍，本拓扑不要求前端统一经 Battle 获取所有计算结果，也不展开公式复用与精度策略。

#### 共享控制与存储边界

Discovery L2 复用 etcd 的成员、租约、Watch 与槽位 CAS，提供稳定归属和缓存；它不是每条业务消息必经的中转节点。gRPC 与 NATS 两入口共享本地实体宿主，正常交接先关闭旧方准入、等已接受工作及必要持久化完成，再晋升归属，最终注销和释放旧实例。实体发现与传输解耦，归属权不由 NATS 订阅或 gRPC 连接决定。

GD 的 Redis/Mongo/Fusion 管道与通信路径独立。图中 Redis/Mongo 是按职责使用的共享后端示意，不要求为每个服务实例单独建库；Fusion 是存储模式，不是额外部署的数据库服务。

### discovery

`tx/etcd` 是 L1 外部后端适配层，只拥有通用客户端、租约、原子键、快照、Watch、Delete 和 CAS；它不认识
`InstanceID`、`workSlot`、SessionID 或逻辑槽位。`modules/discovery` 是 L2 运行时，组合 `tx/etcd` 并拥有运行身份、
服务目录和逻辑槽位目录的领域语义：

- 用同一进程租约从 `[1, 4095]` 有界抢占 `workSlot`，并以成功创建键的 `ModRevision` 生成
  `<AppID>-<workSlot>-<ModRevision>`。
- 注册自动实例 ID、内部业务 RPC 地址、Actor 地址、项目命名公开地址、version、services 和 status。
- keepalive 失效后自动摘除。
- `ServiceDirectory` 维护本地快照。
- 服务路由只选择 running 实例。
- 精确实例路由可以看到 draining/stopping，用于排空既有绑定。
- `SlotDirectory` 维护固定逻辑槽位的进程内快照，消息路由不读取 etcd。

集群归属统一由本模块的 L2 组件承担：`SlotResolver` 提供缓存路由，`CheckSlotOwner` 执行加载前权威校验，
`SlotOwnership` 编排首次分配和正常交接。首次分配先无副作用容量预检，再 CAS 建立归属；交接先复核当前代次、
预检目标容量，再等待源实例全量排水及持久化成功，最后 CAS 晋升。容量预检和排水通过执行回调接入，组件不依赖 Actor、NATS 或控制面协议。

Actor 保留身份到槽位的绑定、实际加载、容量预留、邮箱和生命周期；framework 执行实例排水与持久化收尾；
NATS/gRPC 只传递执行请求。回调错误立即结束本次操作，不自动改派或重投；低层 SlotStore/SlotRouter 与控制面 CAS SPI 保留。
这是一套进程内复用组件，没有新增权威服务进程或租约。
Discovery Runtime 统一创建归属组件，通过 Ownership 返回已绑定权威存储的能力；Actor 装配不再自行选择存储门面。
control.NewHandoffClient 将控制协议和响应身份校验封装为已有 HandoffController 契约，可直接绑定到交接执行入口，连接所有权不转移。

`CModSessionID` 依赖 `CModDiscovery`，复用其 `workSlot` 和租约有效性，不创建第二份租约，也不向业务暴露
`workSlot`。标准服务组合只有需要发号的 Login 显式声明它；Agent、MsgQ、Game、SLGGame 和 SLGWorld 不声明。
模块和服务在启动回调中可以取得分配器句柄，但只有 Discovery 成功把实例标记为 `running` 后才开放发号；
启动期分配返回 unavailable。租约丢失或排水后分配器立即停止发号，且不能被启动末尾的启用动作重新打开。

Login 是项目侧无状态 HTTP 服务；通过认证和访问检查后，将 SessionID 与业务身份写入绑定 Agent 实例的短期票据。
客户端按 Login 返回的公开地址连接 Agent，Agent 验票建立 Session。新建握手仍校验票据所指的运行 `InstanceID` 与有效期；恢复握手验签后沿用票据中的原主体和 SessionID，不要求目标运行 `InstanceID` 相同或票据仍在新建有效期。客户端意外断线时保留原票据、地址、序号及待发送队列，限次恢复；新 Agent 若在原地址重启、使用同一票据验签密钥且原 MsgQ 邮箱仍存在，MsgQ 将原 Session 重绑到新的 ConnectionID 并续接 Seq/Ack 与未确认下行。原邮箱不存在时恢复失败，不能静默改建新 Session；原地址已变化时，当前客户端不能凭旧地址自动发现新 Agent。重新 Login 则取得新票据、新 SessionID 并重新认证授权，Seq/Ack、未确认下行与旧 Session 无关；是否重新从存储读取玩家 GD 由 Role Actor 与缓存生命周期决定，不由 SessionID 保证。
Login 关闭只停止签发新票据，不拥有或终止 Agent/MsgQ 上已经建立的 Session。业务账号、角色号段与账号角色投影由项目维护。

正常关闭把实例状态推进到 `stopping` 后删除实例键，但只停止租约续租，让 `workSlot` 键按 TTL 自然过期；这样新进程
不能在旧进程退出的同一窗口立即复用短期槽位。由于 SessionID 在成功进入 `running` 前不会开放，启动尚未完成、
从未进入 `running` 的失败回滚必然没有发号，可以主动撤销租约。
持久 `SlotAssignment` 不随实例关闭删除。

实例 ID、endpoint、版本和服务列表在启动时动态构造，不写死在业务 yml。etcd 保存集群控制状态，不保存 GD 或业务地图数据。

固定槽位的数据流分为低频控制面与高频消息面：

```text
控制面 CreateSlot / PromoteSlot
            |
            v
      EtcdSlotStore CAS
            |
            v
       etcd Watch --------------+
                                |
                                v
Agent ResolveSlot -> CachedSlotStore -> SlotDirectory -> ServiceDirectory
                                本地内存          本地内存
```

`EtcdRegistry.Slots()` 是直接访问 etcd 的低级持久 Store；Server 对业务通过 `SlotReader` 暴露组合后的缓存读取能力。缓存门面只把
创建和迁移委托给持久 Store，读取永远不回源。持久提交与 Watch 可见之间允许存在短暂收敛窗口，发布控制面必须观察目标
generation 后再放流。

实例目录与槽位目录属于同一个路由一致性域。Server 启动时等待两份首轮快照，再把实例标记为 `running`；任一 Watch
中断会同时清空实例目录、使槽位目录进入 unavailable，并按固定退避重新获取完整快照。此时路由默认失败，不能使用旧
assignment，也不能临时恢复成逐消息 etcd Get。该选择以不向错误 owner 投递为最高优先级。

Runtime 为每轮 Watch 创建独立快照写入入口。观察到任一 Watch 返回时，先将本轮标记失效并清空两份目录，再取消和等待
另一条 Watch；失效轮次的迟到 Replace 不再发布。快照发布与轮次失效由同一把锁串行化，该锁不参与业务路由读取。
`Runtime.WaitReady` 只接受当前有效轮次的两份快照，不拼接各目录历史上的首次 Ready；这不是跨目录同一 etcd revision 的原子快照承诺。

`SlotResolver` 使用不含 `Services []string` 和 `Endpoints map[string]string` 的 `RouteInstance` 值类型读取内部业务 RPC
地址，避免高频路由为防御性复制切片或地址表而分配内存。命名公开地址只属于低频实例发现元数据，由项目定义名称和用途；
`ServiceDirectory` 在写入和返回完整实例时复制地址表，不能把可变 map 所有权泄露给调用方。`SlotDirectory` 与
`ServiceDirectory` 的纯读取路径使用读锁；只有服务轮询游标、快照替换和失效操作使用写锁，避免并发 Agent 请求被互相串行化。

2026-08-27 使用 Go 1.27.0 在 x86_64 macOS 上运行缓存槽位解析基准：单线程
`BenchmarkSlotRouterCachedResolve` 为 `110.7-115.4 ns/op`；16 路并行
`BenchmarkSlotRouterCachedResolveParallel` 从互斥读写时的 `175.0-201.1 ns/op` 降至
`50.3-59.0 ns/op`。两组结果均为 `0 B/op, 0 allocs/op`，且底层持久 Store 的读取次数为 0。

### rpc

`modules/rpc` 是普通服务/实例 gRPC 投递：

- ServiceRoute：从 running 副本选择目标。
- InstanceRoute：精确投递给已知实例。
- `DeliveryService.Deliver` 返回投递结果，`DeliveryService.Call` 返回同步 `Result`。
- protobuf envelope 的 `payload` 字段承载调用方自己编码的不透明业务载荷。

它不实现 Actor mailbox、会话 Seq/Ack 或业务幂等。发送结果不确定时不能由框架盲目重试；业务必须有独立幂等键。
`CallReceiver` 与 `Receiver` 分别承载同步和单向入口，并都可以用请求级 admission wrapper 封口。`Sender.Send` 与
`Sender.Call` 都先校验 `Delivery`、只解析一次路由、按 endpoint 与当前 InstanceID 复用连接；同址实例重启时关闭旧连接，冷路径拨号完成后复核目录身份。`Sender.Call` 不自动重试，也不偷偷补默认
timeout。客户端只接受 `Accepted=true` 且没有错误码的确认；成功位与错误码同时存在属于矛盾状态，统一按拒绝处理。

普通 RPC Sender、MsgQ 下行发布 Sender 和 Agent 的远端 SessionClientRouter 共用上述连接生命周期。Actor `CallStream`、下行 `PublishStream` 和 Agent→MsgQ `MailboxStream` 在各自实例连接内复用长流；MailboxStream 动态复用多个 Session，初始没有 Session。连接轮换会终止旧流，在途结果未知时不自动重投。三种业务流保留各自的请求关联、错误反馈和取消契约，不用同一套通用消息流协议覆盖。

MailboxStream 的每 Session 接收缓冲有界，慢 Session 缓冲溢出时结束自身读取，不阻塞其他 Session、不代替客户端推进 Ack；未确认下行保留在 MsgQ，按原恢复流程读取。单 Session 的 Context 结束会中断对应 Agent Socket 读写和续期，不等待下一次 Recv；远端读取者退出后按订阅对象清理登记。控制帧处理与下行 Send 分属两个协程，流控不应阻断 Ack/CLOSE。Ack/Renew 遵守调用方 Context；CLOSE 先完成本地取消，再尽力发送远端取消，控制队列满时显式返回错误，远端读取者仍受会话超时约束。PublishStream 关闭最多等待 5 秒，超时取消底层流并返回失败。

`modules/rpc.Host` 实现“每个物理进程一个普通 gRPC Host”：它唯一持有 listener 与 `grpc.Server`，直接接入
`lifecycle.WaitGroupContext`，并负责注册阶段、Serve 和幂等 Stop。Bee/Ant 的私有装配内核创建 Host，让所选服务在启动期
注册 protobuf service，全部注册成功后启动 Host，并把实际 endpoint 发布到服务发现。
同进程的 msgq 与普通 RPC 因此可以共享 endpoint，但保持不同 protobuf service；任一服务不得自行关闭共享 listener。

### BON 解码边界

BON 的无符号长度使用 7-bit 变长整数，Go runtime 与 TypeScript 客户端共同锁定同一 wire format。多字节编码必须
写出移位后的最后 7 位，例如 `128 -> 0x80 0x01`，不能把原始低字节重复写入尾段。

解码器面对客户端输入时先验证物理下界，再分配动态容器：数组每个元素至少需要一个类型字节，文档每个成员至少需要
两字节字符串键和一字节值类型。声明数量超过剩余载荷可表达的上限时立即失败。容器递归深度固定限制为 64，防止小型
恶意载荷通过连续单元素容器消耗运行时栈；该限制独立于 Agent 的整帧字节上限。两端必须用相同边界测试同步演进。
TypeScript reader 使用无原型字典承载动态文档，并只从自有属性读取命令信封和 schema 字段；不能让动态 key 改写
临时对象原型或用继承属性绕过信封校验。

通用 `DeliveryService` 在同一个 gRPC server 中只能注册一次。聚合多个业务服务的进程应由项目提供一个进程级
Receiver，再按稳定消息元数据分发，不能让每个服务重复注册同名协议。Actor Remote 继续使用专用入口；Agent Session
控制协议注册到普通共享 RPC Host，但保持独立 protobuf service，不与业务 Delivery、MsgQ 或 Actor 路由耦合。

### msgq

本地 `roles` 索引由 `RoleID -> mailboxSession 指针集合` 构成，仅用于定位接收邮箱，不增加统一生命周期或共享 Seq/Ack。
初始化成功时在持有 session 锁的条件下发布索引，恢复不重复加入；所有邮箱删除路径通过 `removeLocked` 同步移除角色索引。
角色广播在服务锁内取得本地索引快照后释放服务锁，再逐个取得邮箱锁，与 Topic 广播复用 `broadcastSessions`。
因此不会持有服务锁等待邮箱，也不会将初始化中或已失活邮箱当成有效收件人。新加入者不接收快照取得前的历史消息。
`BroadcastResult` 统一表达 Topic 和角色分发统计；远程角色归属校验仍属于后续运行入口接入范围。

广播的部分入队结果属于通信契约：本地调用同时返回统计和错误；gRPC 错误使用状态详情承载原有
`BroadcastResponse`，客户端还原投递统计和错误类型。不能返回普通响应加非空 gRPC error 并期待响应正文被传送。
健康邮箱的入队不会因其他邮箱溢出而回滚；载荷从广播调用开始按只读共享处理，不以返回成功作为唯一交接条件。
传输断开或超时可能使统计不可获得，此时结果未知，不能据此全组重发。

#### 角色归属与路由缓存基础

`RoleBinding` 表示 `RoleID -> (InstanceID, Generation)`，不代表邮箱或共享生命周期的 Session 集合。
Redis 单 key 原子申请使并发候选得到同一个归属；查询和重复申请不续期，只有持有完整归属的 MsgQ 才能条件续期或释放。
归属 key 有 TTL，以防初始化中断或进程退出后永久残留；释放前必须由运行层排除并发初始化，并确认最后一个可恢复邮箱已终止。
仅凭 CAS 不能证明满足这个释放条件。Generation 是每次申请的唯一令牌，不是 SessionID，也不是集中式递增计数器。

`RoleRouter` 使用 Otter v2 保存有限数量的归属值；服务发现快照不缓存到归属记录内，以便每次定向寻址检查当前实例状态。
冷查询使用 singleflight，并在结束合并前完成缓存填充，避免极快并发查询重复回源。
明确失效与在途加载交接后按完整归属删除；过期通知不能删除后续缓存。热路径不访问 Redis。
路由缓存只用于寻址，不证明目标仍持有邮箱。`PubRole` RPC 携带 RoleID、InstanceID 和 Generation，
接收端在同一读锁内检查本地完整归属和有效期并取得邮箱快照；旧请求不能混入换代后的新邮箱。
缺失、过期或不匹配的归属返回 `ErrSessionChanged`，不在接收热路径查询 Redis，也不转发给其他 MsgQ。
初始化尚未成功的邮箱不在快照内；初始化失败不会留下订阅。MsgQ 宕机后不恢复旧邮箱内容，归属过期后使用新 Session 干净登录。
角色投递与 Topic 广播共用 `BroadcastResponse`，只报告入队统计，不把多个邮箱的独立 Seq 合并为一个序号。
网络超时不触发自动业务重发；消息可能已入队，不能通过重新寻址推断发送失败。

Agent 选址、邮箱初始化和 `Sender.PubRole` 已接入角色归属。Sender 使用有界 RoleRouter 缓存，仅在 `ErrRoleBindingChanged` 明确表示尚未向任何邮箱投递时清理对应版本，重新寻址并重试一次。该标识通过 gRPC ErrorInfo 保留；普通会话错误只清缓存，超时、断连与部分投递不重发。第二次失败原样返回，不重跑业务 Handler。
跨工程真实验收继续按实施计划推进，不能将缓存基准视为端到端链路验收。

邮箱用角色级初始化锁串行协调初始化和归属维护，不把多个 Session 的 Seq/Ack 合并。初始化第一次接管本地角色时先原子申请、再条件续期，
本地租期从请求开始时刻计算，避免把网络耗时额外算入有效期。正常入队与会话活动只读本地租期，不访问 Redis 角色索引。
低频清理循环只枚举本地归属：有邮箱且到达半租期时条件续期；无邮箱时条件释放；超期或明确失去归属时使该角色所有旧邮箱失效。
续期 I/O 错误不会延长本地有效期；最后邮箱释放与新初始化共用角色锁，避免新邮箱继承被并发删除的绑定。
角色维护在初始化分片锁内登记在途续期，锁外执行 Redis 条件续期，返回后重新获取锁，核对停止状态、完整归属及在途请求身份；本地租期已失效时不接纳迟到结果。
同归属重复续期等待同一完成信号，等待者取消不取消原请求。停止或最后邮箱释放可以先于旧续期返回，旧结果不得写回新归属。该锁拆分仅用于维护续期；首次申请和条件释放仍与初始化串行协调，维护循环不并行化。

`modules/msgq` 是客户端会话邮箱，不是所有服务消息的中心总线。它负责：

- `SessionID -> (RoleID, SessionID, ConnectionID, InstanceID)` 精确 binding。
- `RoleID -> 唯一 MsgQ 归属及版本` 投影。
- 上行 Seq 单调递增与旧号去重。
- 下行队列和 Ack。
- 断线重连时重发未确认下行消息。
- 本地 Topic 广播。

远端下行流只复用传输，不改变邮箱出队、Seq/Ack 和重连重放的权威边界。每次读取仍校验当前 `ConnectionID`；Agent 断开后取消流，新连接通过 `Restore` 按已有 Ack 重置发送游标。流式发送不额外等待凑批，也不把 MsgQ 内存下行变成持久化消息。

Redis 精确 binding key 为 `<prefix>:session:v2:{SessionID}`，保存完整四元组；角色归属 key 为
`<prefix>:role:{RoleID}`，保存 InstanceID 和 Generation。二者分别按 SessionID 和 RoleID 选择分片，只保证各自单 key
条件操作，不承诺跨 key 事务。下行正文只保存在 MsgQ 实例内存；实例崩溃允许丢失未确认下行，客户端重连后由业务状态
重同步恢复。

`SessionID` 标识一次逻辑登录会话，`ConnectionID` 标识该会话下的一次物理 TCP/WebSocket 连接。弱网恢复必须命中
同一 `SessionID`，同时分配新的 `ConnectionID` 以拒绝旧连接迟到的 MailboxStream Ack、Topic 管理和 TerminateSession。`Restore=false`
的新建会话复用 RoleID 的 owner，只有无归属时才在 running MsgQ 间轮转提名并原子确定 owner，然后用 `Create NX` 创建精确 binding；现存 SessionID 会明确冲突。
`Restore=true` 的恢复会话只按
SessionID 回到原 running owner，用完整旧 binding 条件 `Rebind` 到新 ConnectionID；失败时不迁移。同 SessionID 恢复
保留 Seq/Ack 与未确认队列，新 SessionID 的干净登录不继承旧消息。

#### 多个 Session 操作同一业务身份

授权的同一 RoleID 多个 Session 可以并存，并路由到同一 Game/Actor。这是通用多会话能力，不定义主、副 Session 或会话优先级。
Borrow 和旁路挂机只是应用场景：前者由授权操作者访问目标角色，后者由外部服务运行行为树并发送行动请求，
玩家自己的 Session 可同时在线观察或操作。世界模拟与权威校验仍在服务端，不因外移行为树决策而取消。
世界请求按业务认可的路由键定位 WorldActor，不以操作者账号替代目标世界身份。
各邮箱独立维护 Seq/Ack 与未确认队列，新增、恢复或终止一个 Session 不影响其他 Session。
`Sender.SendRole` 向该角色全部邮箱异步分发，框架不提供基于会话优先级的登录互斥策略。

`SessionRouter.ResolveMailbox` 有界缓存 SessionID 到 MsgQ InstanceID 的归属，合并冷查询，供 `SendSession` 和显式同步 `PubSession` 使用；不缓存 ConnectionID。
同一 Session 的 MsgQ 归属不迁移，实例丢失后使用新 Session。缓存命中仍核对本地服务发现，明确邮箱失效时清缓存，传输超时不自动重发。
`SessionRouter.ResolveSession` 始终读取当前绑定，Topic 操作保留此边界。它与 `ResolveMailbox`、`RoleRouter.Resolve` 都只读解析，不刷新 TTL。只有当前 ConnectionID 通过 owner 侧校验的连接活动
才更新本地活动时间；业务发布不更新。生产 Redis store 在 `Init`、`Rebind` 或一次成功续期后等待 `TTL / 2`，达到
阈值的第一条合法活动才续期精确 binding，因此高频心跳、上行、Ack 和下行推送不会产生同频 Redis I/O。
续期网络等待不持有邮箱队列锁，同邮箱到期活动合并等待；等待者可独立取消。恢复或终止可与在途续期交错，
续期返回后重新核对活动状态、ConnectionID 与角色归属有效期，旧续期结果不能删除或更新新连接。
默认离线保留窗口为 `180s`，失活终止检查间隔为 `2s`；周期任务只遍历当前 MsgQ 的本地 Session 表，不扫描 Redis key。
超过窗口、积压条数或积压字节上限时终止整个 Session，而不是无限保留未确认消息。

公开业务下行使用 `Sender.SendSession(SessionID, payload)` 与 `Sender.SendRole(RoleID, payload)`：复用指向 MsgQ owner 的 `PublishStream`，进入本地有界发布队列后返回，不等待写流或目标邮箱逐项确认。队列满时等待空位，并受调用方 Context 或流关闭约束；入队后的写流失败只记录错误，不自动重投。关闭发布流时按顺序排空已入队消息。只有旧角色归属被 MsgQ 明确拒绝、该条载荷尚未进入任何邮箱时，拒绝帧才回带原载荷，发送端失效旧缓存并重新寻址，单次投递到新归属；重投结果未知或部分成功时停止，不再次重投。需要明确入队结果的调用方仍可显式使用同步 `PubSession`/`PubRole`，不得将它们的结果语义套用于异步发布。`SendRole` 到达归属 MsgQ 后向该角色的全部会话邮箱扇出，各邮箱独立分配下行 Seq。Topic 是单个 MsgQ 的本地 SessionID 索引：`JoinTopics` 和 `BroadcastTopic` 以 SessionID 作为当前连接或实例路由锚点，跨实例覆盖须由业务分别选择各实例上的 Session。

会话控制的 `Init` 仅在得到合法响应后报告成功。`MailboxStream` 承载每个 Session 的下行推送和 Seq/Ack，同一条流可复用多个 Session；旧连接与过期会话的错误只终止对应逻辑会话。异步发布的写流成功不等于目标邮箱入队，失败或断流时结果可能未知，框架不会自动重发业务载荷。

Agent 的会话来源是显式二选一：配置 `LocalMsgQ` 表示同进程复用，未配置时才通过 discovery 解析远端 msgq。
本地 provider 返回错误时必须让 Agent 启动失败，不能回退远端；否则一次本地装配顺序错误会被误判成跨进程
部署，并把半装配进程带入运行态。

Agent 在调用 MsgQ `Init` 前把连接登记为初始化中，`Init` 成功后、发送 `FrameReady` 前原子晋升为已激活。注册表以
`SessionID` 定位，以 `ConnectionID` 比较删除；旧连接退出不能移除已经恢复的新连接。控制请求会同时覆盖初始化中与
已激活连接：初始化期间收到断开时不发送 Ready；收到终止时，无论 `Init` 成功还是因连接取消返回不确定结果，都会使用
独立的有界上下文按新 ConnectionID 补偿一次精确终止，仍不发送 Ready。同一 SessionID 的并发终止请求共享一个本地操作，
终止期间新登记的初始化连接自动加入；调用方取消只结束等待，不取消后台收尾；任一精确终止的非幂等错误都必须返回，
不能由更早连接的成功结果掩盖。`DisconnectSession` 只关闭物理连接，保留
MsgQ 邮箱并允许恢复；`TerminateSession` 使用登记时保存的完整 SessionHeader 精确终止 MsgQ 逻辑 Session，再关闭物理连接。

所有独立 Agent 都在共享 RPC Host 上注册同一控制服务。调用方通过 `SessionControlBroadcaster` 读取服务发现内存快照，
向全部运行中 Agent 并行广播；未持有目标 Session 的实例幂等返回未处理。至少一个实例确认处理即表示命中；全部未命中且
存在调用错误表示结果不确定。该控制链路不查询 Redis owner，不认识 MsgQ，也不依赖 Kubernetes 或特定负载均衡器。

### Agent

`modules/agent` 终止客户端 TCP/WebSocket 连接，处理 frame、握手身份、Seq/Ack 和路由描述。它不承载具体 game handler，也不要求所有命令先经过 game 中转。
标准 `agentservice.Service` 在配置 `WebSocketListen` 时复用同一个 `http.Server`：根路径 `/` 固定保留给 WebSocket Upgrade，
非根路径在请求级 admission 通过后才进入项目 `HTTPHandler`；未配置 `HTTPHandler` 返回 `404`，排水或停止后返回 `503`。
如果未配置 `WebSocketListen`，当前阶段不会创建“纯 HTTP” Agent 服务。

生成的 `CmdDescriptor` 提供静态 CommandID、目标服务、Actor kind 和 route key 类型；项目 resolver 根据认证身份解析 Kind/Key，实例归属由 tx 统一解析。

连接入口可以是真实客户端，也可以由项目扩展为本地驱动或旁路模拟连接。tx 提供基础网络 Agent，同时保持 dispatcher 接口开放。

项目通过自身 `services/agent.HandlerProvider` 在组合根注入认证与 Actor 路由。Provider 接收当前
Bee 或 Ant Context，使用 CModActorDirect 完成直连；稳定分区或业务槽位在 Kind 声明，Provider 不组装发现连接或 PID。

`IdentityResolver` 只返回 `protocol.SessionPrincipal`，其中包含 `AccountID`、`BorrowAccountID`、`ServerID`、`RoleID`、
`SessionID`、`AccessMode` 和 `CanCreateRole`。Agent 负责生成正 `ConnectionID`，校验并构造 `protocol.SessionIdentity`；
项目 resolver 不能从 ticket 或客户端数据指定 `ConnectionID`。普通模式不携带 `BorrowAccountID`；借用模式必须携带正
`BorrowAccountID`。这些字段描述当前身份契约，不构成独立的会话生命周期；目标身份的读写权限与建角许可由业务决定，框架不拦截业务协议。各 Session 生命周期独立。

业务 Handler 使用 `ctx.Identity()` 读取可信身份：`AccountID` 是认证操作者；目标账号在 `BorrowAccountID != 0` 时为该值，
否则为 `AccountID`；`RoleID` 是操作的目标角色，`SessionID` 区分请求来自哪个会话，`ConnectionID` 区分其当前物理连接。
因此可区分不同账号的授权访问，但不能仅凭 SessionID 或 AccessMode 判断真人、管理员、挂机服务。
尤其当两个 Session 使用同一账号、同一访问方式时，当前公共身份字段没有额外的“挂机来源”标签；需要这种分类时由项目认证/授权上下文提供，
不能信任请求正文中自报的管理员或旁路标志。业务不需要维护主、副会话关系。

通用 Actor Dispatcher 在调用 Messenger 前以 `DispatchRequest` 的可信 `SessionIdentity` 和当前上行序号覆盖 Delivery 的
`Identity` 与 `UpSeq`。项目 Actor resolver 仍只负责目标、消息类型、命令号、路由键和载荷，不能伪造身份或序号。
Actor protobuf 在跨进程投递时保留完整身份和 `UpSeq`；内部 Actor 调用可以同时省略二者，只出现其中一个的 Delivery 无效。

`Dispatcher` 在目标 Actor 邮箱接纳消息后返回 `AcceptedUpSeq`；该值只供 Agent 内部确认已入队前缀，不回传客户端。Game/Actor 在后续处理命令时，使用可信身份中的 RoleID 调用 `PubRole` 共享普通业务响应；请求级错误使用 SessionID 调用 `PubSession` 精确发布。业务消息和 System_Error 都作为独立 MsgQ 下行，新 SessionID 不会继承旧会话的下行。通用 Actor Dispatcher 使用直连 `Enqueue`，不等待业务完成。

Agent 核心不依赖项目生成协议，因此不会自行构造业务错误包，也不负责项目 HTTP 路由、JSON/BON 绑定、自动重试或
通用 HTTP Router。项目 Game/Actor 适配层负责把 Handler 错误转换为生成的
`System_Error{RequestSeq, Code, Message}` 并直接发布到发起 Session；这类结果不会终止连接。
`Code` 使用 `int32` 跨 Actor protobuf 边界传递，`0` 表示没有错误，`CallErrorCodeInternal=-1` 表示未分类内部错误，
正数空间由项目错误码表分配。`CallErrorCodeDispatch=-2` 标识 Actor 解码、Handler 注册和投递模式等分派契约失败；
目标 Actor 在解码或分派阶段识别该错误时，Actor 适配层同样生成 `System_Error`，连接继续处理后续请求。握手、Frame、Seq/Ack、
目标解析、邮箱接纳失败、Actor 服务不可达、传输错误和其他基础设施错误仍沿 Agent 错误返回链关闭连接，
不能为了“不断连”把损坏协议状态或不可确定的投递结果吞成请求失败。

连接上存在两条独立序列：客户端为 `FrameUpMsg` 分配 Seq，服务端为 `FrameDownMsg` 分配 Seq。每条连接按 socket 读写顺序处理帧，Seq 只要求递增，不要求相邻两条相差 1；旧号不会重复分派。帧中的 Ack 只由客户端发送，用于确认已经处理的服务端下行：`FrameUpMsg.Ack` 或 `FrameAck.Ack` 推进 MsgQ 裁剪游标。`FrameReady.Ack` 和 `FrameDownMsg.Ack` 固定为零，服务端不发送上行 `FrameAck`。

Agent 将上行提交给 Game 长流后推进本地游标，不等待远端邮箱接纳或业务完成。每次上行提交及客户端 Ack 都经共享 `MailboxStream` 异步同步 MsgQ，不等待逐项回执；没有 Agent Rec lease 或定时低频游标同步。Game 长流服务端在接收循环中同步入队，保证同一条 Agent 流上的消息按序进入 Actor 邮箱；邮箱 worker 可以继续独立消费，Agent 不等待业务执行。

客户端不保存未确认 UpMsg 队列；发送失败或连接中断后的请求结果可能未知，框架不会自动重投。恢复连接复用原 Session，只恢复 MsgQ 中未确认下行。msgq 的新建与恢复语义严格分离：新建总是安装空会话，恢复必须命中同一 SessionID。只有 `Init` 成功后 Agent 才发送 `FrameReady`；客户端在收到它之前保持连接中状态。恢复后新命令继续使用客户端本地递增的 Seq，不回拨也不重投旧命令。`Rec` 对小于等于已记录最大值的旧 Seq 幂等返回；普通响应与 `System_Error` 均由 Game/Actor 直接发布到 Session 邮箱。

客户端成功处理下行后，优先在下一条上行捎带最大已处理下行 Seq；若持续没有上行，参考客户端每 5 秒检查一次未发送的 Ack 并发送独立 `FrameAck`。MsgQ 据 Ack 裁剪队列；两套示例的单会话 1024 条与 8 MiB 配置上限保持不变，触限按现有溢出处理。定时确认不承诺任何时刻的精确积压量，容量评测在上下行主链路验收后进行。

### 会话隔离边界

新 Login 分配新 SessionID，Agent 的游标属于该连接处理过程，不按 RoleID 在进程间共享；返回原 Agent 也不会沿用旧 Session 的游标。Restore 则保留 SessionID、MsgQ owner 和未确认下行，通过完整 binding CAS 更新 ConnectionID。MsgQ 拒绝旧连接的 Rec/Ack、读取和终止操作，并唤醒旧订阅使 Agent 断开旧 socket。

这不是有到期、续租和跨服务 fencing 的 Agent 租约；它不撤销已提交到 Game 的旧上行，也不承诺两条跨 Agent 连接间的全局顺序。客户端不应同时用两条 socket 提交同 Session 的新命令。正常排水由既有准入与完成屏障管理，不为意外退出增加上行重放。角色归属租约和会话 TTL 继续由 MsgQ/Redis 管理，下行序号仍由 MsgQ 分配。

### Actor

默认后端由 tx 本地实体宿主、L2 Discovery 归属和 Core NATS 构成。公开 Kind 工厂统一接收 BehaviorFactory，不再区分 Virtual/Fixed 构造入口，不暴露 ProtoActor PID、Props 或 ActorSystem。

```text
Identity(Kind, Key)
  -> Kind.SlotMapping（默认 v1 1024 分区，或业务稳定映射）
  -> SlotDirectory + ServiceDirectory 缓存
  -> assignment(RuntimeInstanceID, Generation)
       -> Agent: gRPC 共享 Endpoint
       -> Actor 间: NATS 实例主题
  -> 同一本地实体宿主 -> 同一实体邮箱 -> Behavior.Receive
业务下行 -> gRPC -> owner MsgQ -> Agent -> 客户端
```

NATS 不决定实体归属，也不按实体创建订阅。首次缺少 assignment 时，L2 Ownership 执行容量预检与 CAS；已有归属不随新成员入环变化。懒加载前单独读取权威槽位，不能用 Watch 缓存替代。默认分区只共享 owner，每个实体仍独立加载、串行处理、卸载和预留容量；世界与 seasonWorld 可共享 slgworld 服务池并保留独立业务槽位。

实例通过 `actor.Config.Capacity` 声明全部本地 Kind 共用的数量上限。默认 `activation` 模式统计初始化、运行及清理中的 Actor，清理完成才释放；`assignment` 模式用于独立实体槽位，统计持久归属，卸载或 GD 缓存淘汰不释放名额，同一世界重新加载复用名额。独立 slgworld 实例可配置 Maximum=1，承载任意首个被分配的世界，不绑定 worldId；临时房间可按业务估算配置 Maximum=1000。Initializations 独立限制同时加载数量，不给初始化设置统一超时。

默认 EtcdSlotStore 在创建或晋升归属时原子检查稳定名额，交接前先预检目标容量。有限容量的冷分配读取现有目录快照并执行范围比较与单键 CAS，不维护第二份权威计数表；普通消息路由不扫描目录。该路径成本随目录规模增长，后续按真实规模验证。此数量限制不等于内存保证，不负责自动扩 Pod 或资源采样；GD 缓存释放仍由 cache manager 管理。

ServiceSpec.ActorKinds 构成应用目录，Kind 名称应用内唯一；仅选中服务执行本地工厂。CModActor 依赖 RPC 和 Discovery，绑定共享接收入口与 NATS，服务 ready 后才发布 running。连接和关闭权限在框架内，业务通过 CModActor 或 CModActorDirect 的窄接口使用。

### Actor mailbox

Tell 完成寻址后仅确认本地投递；TellAccepted 确认到达当前连接的 NATS 服务端，不确认持久化或目标收取。CallFuture 返回异步 Future，Call 同步等待相同结果。等待超时不撤销已接受业务，也不自动重投。普通结果必须有 MessageType；Agent 上行通过直连 `CallStream` 单向投递，不等待逐项邮箱确认，业务下行独立走 MsgQ。

有界邮箱将准入与生命周期登记放在同一边界；关闸后拒绝新消息，已接受的初始化、排队和处理继续完成。远端错误经 Result 协议传播，普通 Tell 不增加执行回执。远程 pending 由响应、Context 到期或连接关闭清理，不为每个 Future 常驻等待 goroutine。Agent 先使用归属目录检查 running 状态，不再维护依赖 PID 的第二份短期排水缓存。

Behavior.Receive 接收公开 Delivery，返回 Result/error；Context.Post/After/Deactivate 用于本地串行工作与业务自定义卸载。工厂、OnInitAfter、OnActivateBefore、OnActivateAfter 不附加统一 deadline；回调完成时取消 Context。Receive、卸载和权威查询保留 CallbackTimeout（默认 30 秒），具体加载 I/O 使用自身期限。调用方等待或排水超时不会提前释放尚未完成的加载与容量。

正常交接先关闸、排空及必要持久化，PrepareHandoff 返回带源实例身份的确认，控制面再逐槽 CAS 晋升，最后停止旧实例。Stop 本身不推断替代 owner。故障接管继续由运维处理，不能以成员离线证明旧实例不再执行。

### Actor 空闲钝化与 EntityCache 淘汰

缓存淘汰属于运行期维护，不是停服清缓存流程。后台任务的 context 经项目释放回调、写回 SPI 传给实体锁等待；
取消后结束维护，不再领取新候选，取消失败不会解除写回身份。正在执行的 Procedure 不被抢占，必要的持久化收尾
仍按 写回 的既有生命周期完成，不等待清理无变更的内存对象。

Actor 空闲钝化和 GD EntityCache 淘汰是两条独立生命周期：

```text
Actor Kind 连续无消息
  -> tx 私有钝化消息
  -> 停止当前实体 Actor
  -> 同一实体宿主在后续消息到达时惰性重建

GD EntityCache 达到容量或 access expiry
  -> Otter 删除 policy token 并登记候选
  -> writeback.Manager.TryRelease 在同一实体锁下检查活跃事务、预留、pending 和 Store 变更
  -> 释放成功且候选代次未变化
  -> 删除 GD 引用
```

Kind 显式 `WithIdlePassivation` 优先于运行时策略，宿主在同一激活内维护空闲期限，卸载与消息准入共用锁边界。清理完成后才移除本地实体，下次请求按同一稳定 assignment 懒加载；卸载不修改持久槽位。运行时只消费本地注册 Kind 的策略。

有状态模板为 role 和 world bucket 分别创建 EntityCache。`values` map 是业务对象身份的唯一权威，Otter v2.3.0
只保存指向同一 entry 的 policy token，并使用 adaptive W-TinyLFU 管理准入、热度和按条目数的软容量。默认
`idleTimeout=0s`；正值使用 access expiry。统计、Loader、refresh、weight 和 persistence 扩展均未启用。

启用 access expiry，或容量接近上限、Otter 频率 sketch 开始工作后，命中路径在共享读锁边界内推进原子 generation
并记录 policy 热度；在此之前 Otter v2.3.0 本身不会消费读热度，EntityCache 也不承担无效果的 policy 查询和代次更新。
Otter 原子删除回调不取得 EntityCache 主锁，不调用存储、实体锁或
WriteBack，只按 key 合并候选并唤醒 worker。每个缓存只有一个 release worker 和一个 timer；worker 在所有缓存锁外调用
项目 `TryRelease`，暂时不可释放或检查失败时按 `100ms` 起步、倍增到 `5s` 封顶的固定退避重试。释放成功后仍须核对
entry 指针和 generation；回调登记后发生的访问、对象替换或重新加载都会让旧候选失效。脏对象不会为了满足容量数值被
强制删除，因此 `values` 暂时超过 `maximumSize` 属于正确状态。生命周期取消时 worker 停止 Otter 后台任务；最终持久化
仍由 WriteBackManager Drain 完成。

Actor Runtime 不依赖 GD、Store、Procedure或 WriteBackManager，也不在超时回调执行存储 I/O。连接断开不直接停止
Actor；普通心跳命令经过 RoleActor mailbox 即可续期。心跳不读取 GD 时，不延长 EntityCache 中 GD 的空闲时间。

钝化不提高消息可靠级别。正在卸载的激活拒绝新准入，清理完成后才可重建；TellAccepted 仅确认 NATS 到达，不能检测业务已卸载或不允许重建。需要业务结果时使用 Call，不自动重试结果未知的请求。

2026-08-29 命名迁移后的 EntityCache
串行命中在关闭 idle 淘汰时为 `19.37-19.81 ns/op`，启用 idle 淘汰时为 `96.95-100.2 ns/op`；同一预热 key 的
16 路并行命中为 `49.89-50.33 ns/op`，三者均为 `0 B/op, 0 allocs/op`。该组数据是 Otter 接入前基线；接入后的
同机 10 次 A/B 中，默认 `idleTimeout=0s` 的串行命中由中位 `20.46 ns/op` 降至 `19.59 ns/op`（`-4.23%`），
16 路并行命中由 `89.26 ns/op` 降至 `57.60 ns/op`（`-35.47%`）；可选 access expiry 命中由 `107.3 ns/op`
升至 `175.3 ns/op`（`+63.30%`）。三项继续保持 `0 B/op, 0 allocs/op`，几何均值变化 `+0.30%`。profile 显示
可选 expiry 的额外开销来自每次访问更新时间和 Otter maintenance 调度；默认关闭该策略，不把该成本施加给容量工作集。

2026-08-25 使用 Go 1.26.5 在 x86_64 macOS 上运行 `BenchmarkPendingCallsRoundTrip`，单次本地
`add -> resolve -> Await` 约为 `0.78-0.86 us/op, 408 B/op, 6 allocs/op`。该基线包含 Future、完成通知、
`context.AfterFunc` 和结果只读复制，不包含 Actor Remote 网络 I/O。当前不为这条低于网络开销的边界引入对象池；
只有实际 profile 证明 pending 生命周期成为瓶颈时，才评审更复杂的复用方案。

ProtoActor 后端及其性能对照已退役；当前实体宿主使用稳定归属、直连上行与 Core NATS Actor RPC。

### control 与滚动发布

`modules/control` 提供受控运维接口：

- Status。
- Drain。
- WaitDrained。
- PromoteSlot。

运维平台负责滚动节奏和实例替换；tx 不主动操作 Kubernetes。正常发布流程：

```text
旧实例标记 draining
       |
       v
入口不再分配新工作
       |
       v
Ingress 等待已受理工作
       |
       v
最终 Flush
       |
       v
取消 WaitGroupContext 并等待后台循环退出
       |
       v
旧实例 stopping / 删除实例键并停止续租
       |
       v
新 running 实例以 CAS 提升逻辑槽位 generation
```

固定槽位不能在排水窗口随机改派。异常宕机和网络分区下的自动安全接管需要独立 fencing 设计，不属于正常优雅发布。

## 跨 goroutine 数据所有权

Go 没有只读 slice。所谓“零拷贝广播”依赖显式所有权转移：

```go
result, err := mailbox.PubSession(ctx, sessionID, payload)
```

成功后：

- 调用方不得修改编码载荷。
- 不得 `append`。
- 不得归还 buffer pool。
- 消费方只能读取。

本地 Topic 广播的多个队列条目可以共享同一份只读正文。跨进程 gRPC 在序列化边界自然复制。网络写入采用 header/body 分段写，不在发送 goroutine 给共享编码载荷追加包尾。

如果调用方无法遵守所有权转移，边界必须复制。不能把共享可变引用称为 immutable。

## 可选业务工具

`util` 面向业务层提供可选工具，不属于 Store、Fusion、事务 Runtime 或服务进程装配。当前保留的两个核心组件有各自独立的限界上下文：

- `util.EventBus` 是进程内事件总线。普通异步事件允许多个协程并发执行；只有显式 transactional async 才按订阅者串行。它不保证 Store、Procedure 或跨服务消息的执行顺序。
- `util.DelayQueue` 是单个业务场景内的严格有序延迟命令队列。同到期任务按加入顺序由单 worker 执行并支持取消。它不是 Fusion 延迟任务队列，也不是可靠消息队列。

文档提到后者时必须写成“场景延迟队列”或完整的 `util.DelayQueue`；Fusion 使用“延迟保存任务”“热数据淘汰任务”和“Fusion 延迟任务队列”，不能统称为 DelayQueue。

## 错误、日志与可观测性

### 错误分类

框架错误大致分为：

- 调用契约错误：非法 LockKey、未知 Op、nil document。
- 数据协议错误：损坏 JSON、未知 Lua 返回值、路径类型冲突。
- 外部 backend 错误：Redis/Mongo/etcd/gRPC 网络或服务错误。
- 生命周期错误：排水拒绝、context 取消、Flush 超时。

只有明确的 backend 通信错误才能改变项目健康状态。错误链必须保留 `errors.Is/As` 可识别性。

### panic 边界

外部服务波动不能 panic。Store 在 view 模式下执行持久化写、事务使用错误 Store 等纯编程违约，可以在开发期快速暴露；公共网络和存储 API 仍优先返回明确 error。

事务 Process panic 时框架会执行 undo、解绑 recorder、释放锁和 rollback hook，然后重新 panic；它不是用于吞掉业务 panic 的恢复边界。项目应在请求最外层设置统一恢复和日志策略。

### 日志

`tx/log` 是基于 `slog` 的最小门面。库输出结构化事件，不持久化错误历史，也不直接调用云日志 API。默认 Handler
分为两类：本地 `console` 把时间、级别和实际调用位置放在左侧，右侧只保留一个 JSON 事件体；测试和生产 `json`
让整行成为 JSON Lines。两种格式共享相同的 `message` 和业务属性语义，且一条事件只写一次。容器日志 Agent 或
LogListener 负责旁路采集、批量上传、重试和留存，外部日志服务异常不会进入游戏请求链路。

日志门面显式构造 `slog.Record` 并记录业务调用点，避免包装函数使 `source` 固定指向 `tx/log/log.go`。Handler 在写出
前完成整行编码；console writer 串行提交完整行，不能让并发协程的 JSON 内容交错。项目仍可替换默认 slog Handler，
但不得在业务请求中同步上传远端日志。

Actor 生产后端不再启动 ProtoActor，也不保留独立 Gossip 日志级别；宿主、传输与业务继续遵守现有结构化日志边界。

网络消息日志遵循“最接近完整语义的一层记录一次”：

- `modules/agent` 拥有物理连接和逻辑会话生命周期，因此只记录连接建立、会话就绪、正常关闭和异常关闭。它不认识生成
  命令，也不能逐条记录 `UpMsg/DownMsg`，否则项目层再次记录业务结果时会产生重复日志。身份解析成功后的生命周期日志
  同时记录 `roleID`、逻辑 `sessionID` 和物理 `connectionID`；握手前的连接建立日志只能记录当时已经生成的 `connectionID`。
- `modules/msgq` 拥有 Seq、Ack 与下行积压状态，因此只在 Debug 记录成功提交后的 `Init/Rec/PubSession/Confirm` 状态，不在锁内
  执行日志输出，不记录载荷内容，也不为无消息的 `Fetch` 长轮询制造日志。恢复日志同时记录前后 `connectionID`；队列
  溢出淘汰和超时清理记录完整的 `roleID/sessionID/connectionID`；旧物理连接被栅栏拒绝时记录请求方与当前
  `sessionID/connectionID`，用于关联同一逻辑会话的多条物理链路。
- 项目 Agent Dispatcher 同时拥有会话身份、生成命令描述和最终 Actor 结果，是玩家协议访问日志的唯一业务切面。无状态
  服务模板使用 Info 便于线上请求排查；高频有状态服务模板使用 Debug，生产环境按需打开。主动推送在项目发布门面记录。

该边界保证 Bee/Ant 门面不写死项目日志级别，也避免 Actor 通用执行器把玩家协议与服务间投递混为一类。

高频错误需要限频，例如 Actor mailbox overflow；持续失败任务需要包含类型、记录键、attempt 和错误链，但不把无限错误字符串写回 Redis。

## 性能分级与验证

### 路径等级

| 等级 | 示例 | 目标 |
| --- | --- | --- |
| L0 | 变更路径拼接、frame header 编解码、队列游标 | 零分配或接近零分配 |
| L1 | Store setter、事务加锁、文档构建、Agent/RPC 转发 | 分配和复制有明确上限 |
| L2 | 配置解析、服务注册、错误构造、启动装配 | 优先清晰和正确 |

零分配不是所有代码的统一目标。大值类型复制、复杂 pool 生命周期和 `unsafe` 可能比一次受控分配更差。

### 必跑验证

Actor 测试按正式实现的职责放置，不再保留 `internal/actortrial` 试验包：

| 位置 | 覆盖边界 |
| --- | --- |
| `internal/actorhost` | 邮箱、激活、生命周期及本地调用基准 |
| `internal/actorrpc` | 正式 NATS/gRPC 传输、跨进程调用、断线不重投、容量竞争与分区路由 |
| `modules/discovery` | 归属 CAS、交接结果未知及并发交接唯一获胜者 |
| `internal/framework` | 正式 Server 排水、真实事务写回后的交接、etcd 租约失效关闭 |

旧 Agent 完成 ACK 适配、独立 SlotHost 控制协议及新旧试验传输互通用例已删除；历史审计中的路径和数据仅代表当时版本。测试环境变量暂沿用现有入口，可在本机 NATS、etcd、Redis 上运行：

```bash
TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222 \
TX_ACTORTRIAL_ETCD_ENDPOINT=127.0.0.1:2379 \
TX_ETCD_ENDPOINT=127.0.0.1:2379 \
TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379 \
go test ./internal/actorhost ./internal/actorrpc ./internal/framework ./modules/discovery -count=1
```

未设置依赖地址时，真实依赖用例会跳过，不能将普通 `go test` 成功视为这些场景已验收。

核心变更至少运行：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

热点优化在同一机器和 Go 版本运行前后 benchmark：

```bash
go test ./tests -run '^$' -bench '<目标>' -benchmem -count=5
```

必要时再使用：

```bash
go test ./... -gcflags='all=-m=2'
go test ./tests -run '^$' -bench '<目标>' -memprofile /tmp/tx-mem.pprof
go tool pprof /tmp/tx-mem.pprof
```

### 性能优化边界

- 不依据另一个机器的绝对 ns/op 判断回归。
- pool 必须证明减少了稳定分配，且不会扩大对象生命周期。
- 结构体字段合并要测复制成本和 cache locality。
- 反射只在真实动态或低频边界使用；生成期可确定的关系应生成强类型代码。
- 为 benchmark 添加的 API 不能扩大生产导出面。

## 扩展模块的方法

新增 module 或 backend 前回答：

1. 它唯一负责什么？
2. 哪些职责明确不属于它？
3. 普通业务是否需要直接看到它？
4. 是否能用已有基础接口组合，而不是新增定制接口？
5. 它的失败会不会错误清理 Store 变更状态、丢队列或改变健康状态？
6. 它是否引入新的跨 goroutine 所有权？
7. 是否已有成熟库能承担通用部分？

成熟依赖优先用于数据库驱动、网络协议、服务发现和标准编解码。变更追踪、Store、Fusion 和事务锁顺序承载 tx 的一致性语义，只有候选库完整表达这些契约时才替换。

## 文档维护规则

- README 记录公共 API、配置、使用示例和调用方契约。
- 本文记录模块职责、内部算法、并发模型和技术选型。
- 实施计划只记录尚未完成的任务和待决策项，不复制稳定契约，也不保存已完成阶段的操作清单。
- 审计标准记录检查方法，审计报告记录特定 revision 的证据。
- 已淘汰方案不写入 README；如果理解当前设计必须知道取舍，只在本文简要说明原因。
- 行为变更必须同时检查 README 和本文是否需要更新。
- 文档、GoDoc、代码注释和模板注释必须复用“限界上下文与固定术语”中的名称；同一个词跨上下文有不同含义时必须补充限定词。
- `快照`只描述快照差分、服务发现快照等真实副本；StoreView 统一称为“视图”。`载荷`只描述编码后或协议传输的数据；StoreView 不是载荷。
- `投影`只在数据库读取字段投影等已有领域术语中使用。Store 到 `Path + Op` 的过程称“同步遍历”，生成 `change.Document` 的过程称“变更文档构建”。
- benchmark 数字必须注明日期、机器或链接到审计报告，不能表现成跨环境性能承诺。

## 槽位读取与高级管理 API

业务通过 ant.Context.CModSlots 取得 SlotReader，使用 SlotResolver 解析 running 目标或在 World loader 中校验归属。SlotStore 嵌入 SlotReader 并保留 CreateSlot/PromoteSlot；SlotRouter 保留原有高级读写 API，读取复用 SlotResolver。game 侧高级组件和控制面可由组合根显式注入完整 SlotStore，不转移其连接生命周期。不会把只读上下文作为获取写入能力的入口，也不把 Go 接口收窄当作安全沙箱。
