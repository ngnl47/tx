# Bee、Ant 与 Core 框架边界设计

## 目标

本次重构在同一个 `tx` 仓库和 Go module 内建立三条明确边界：

- **Core** 提供稳定、正交、可独立测试的 L0-L2 算法、契约、backend 和运行时组件。
- **Bee** 组合 Core 组件，向无状态游戏后端提供 L3 框架门面。
- **Ant** 组合 Core 组件，向有状态游戏后端提供 L3 框架门面。

`txgame` 只使用 Bee，`txgameslg` 只使用 Ant。两套项目仍可组装 agent、msgq、game、world 等不同 L4 服务；
Bee 和 Ant 描述的是整个游戏后端采用的运行模型，不是单个服务进程是否碰巧持有少量本地状态。

本次设计优先解决以下问题：

1. Core 不再被无状态和有状态两种框架策略反向污染。
2. CMod 固定属于 Bee 或 Ant，不再与底层 Component 混为同一层级。
3. 业务项目不再直接依赖大而全的 `server.Server` 和包含大量可选 `nil` 的 `server.Context`。
4. L4 Service 在启动前获得经过校验的强类型依赖，Handler 和领域代码不接触框架装配上下文。
5. `StateRuntime` 按真实职责收敛为 Ant 的 `CModProcedure`。
6. Bee 与 Ant 的公共 API 保持同构，维护人员切换项目时不需要重新学习一套框架入口。
7. 迁移过程中每个阶段都保持可编译、可测试；最终不保留第三套兼容入口。

## 非目标

- 本次不拆分 Git 仓库、Go module 或发布版本。
- 本次不改变 GD、Store、StoreView、变更文档、Fusion、Actor、Procedure、WAL 的既有业务语义。
- 本次不引入运行期插件、反射扫描、服务定位器或依赖注入容器。
- 本次不让 Bee 依赖 Ant，也不让 Ant 依赖 Bee。
- `bee` 和 `ant` 只作为框架命名空间，不继续衍生动物相关术语。

## 固定术语

| 术语 | Go 对应结构 | 职责 |
| --- | --- | --- |
| Core | `change`、`storage`、`modules/*`、`component` 等包 | 提供 L0-L2 基础能力，不决定应用运行模型 |
| Component | `component.Component` | 可复用运行时组件或生命周期节点，不具有 CMod 业务语义 |
| Component Host | `component.Host` | 解析依赖图并执行校验、启动、回滚、排水和停止 |
| CMod | `bee.CMod`、`ant.CMod` | Bee 或 Ant 中具有稳定身份的 L3 能力门面 |
| CModID | `bee.CModID`、`ant.CModID` | 同一框架命名空间内的稳定 CMod 身份 |
| Service | `bee.Service`、`ant.Service` | 可由 `--services` 选择的 L4 运行单元 |
| Service Registration | `bee.ServiceRegistration`、`ant.ServiceRegistration` | Service 的静态规格与延迟构造工厂 |
| Context | `bee.Context`、`ant.Context` | 只在 Service 构造边界读取已装配能力的受控视图 |
| Runtime | Actor、Transaction、Cache 等组件的执行实例 | 持有运行状态和生命周期，不等同于框架门面 |
| Builder | `bee.Builder`、`ant.Builder`、`ant.CModProcedureBuilder` | 增量收集强类型配置，最终一次性 `Build` |
| Manager | `cache.Manager` 等 | 长期管理一组对象、队列或调度状态 |
| Host | RPC Host、Component Host | 持有物理监听器或一组被托管单元的生命周期 |
| Directory | `discovery.Directory` | Watch 后的本地只读服务目录 |
| Resolver | 路由输入到唯一目标的解析器 | 不负责在多个候选中做策略选择 |
| Router | 在候选目标之间执行负载、哈希或绑定策略 | 不持有目标业务状态 |

架构层级、性能热度和审计严重度继续分别使用 `L0-L4`、`H0-H2`、`P0-P3`，不可混用。

## 依赖架构

目标依赖方向固定为：

```text
txgame                         txgameslg
   |                               |
   v                               v
 bee                              ant
   |                               |
   +---------------+---------------+
                   v
              Core Component
                   |
                   v
          backend / transport / contract
```

强制规则：

- Core 包不能 import `github.com/ngnl47/tx/bee` 或 `github.com/ngnl47/tx/ant`。
- Bee 不能 import Ant，Ant 不能 import Bee。
- `txgame` 不能 import Ant，也不能继续 import 旧 `tx/server`。
- `txgameslg` 不能 import Bee，也不能继续 import 旧 `tx/server`。
- txgen 和生成的 GD/Store 代码只依赖 Core 契约，不依赖 Bee 或 Ant。
- 同一个进程只能由一个框架门面拥有根生命周期；不允许同时启动 `bee.Server` 和 `ant.Server`。

这些规则由基于 `go list -deps -json` 的架构测试锁定，不靠文档约定人工检查。

## 包结构

目标包结构如下：

```text
tx/
├── bee/                    # 无状态框架门面、Bee CMod 和 Service Host
├── ant/                    # 有状态框架门面、Ant CMod 和 CModProcedure
├── component/              # 中立依赖图与生命周期宿主
├── change/                 # 变更状态树、视图和文档投影
├── lifecycle/              # WaitGroupContext、Ingress 和准入契约
├── meta/                   # 生成类型元数据
├── modules/
│   ├── actor/
│   ├── agent/
│   ├── cache/
│   ├── discovery/
│   ├── msgq/
│   ├── rpc/
│   └── txn/
├── protocol/               # 帧和命令协议
├── storage/                # 存储契约、Redis、Mongo 和 Fusion
└── wal/                    # WAL 契约与 backend
```

现有 `tx/server` 只在迁移期继续作为调用方。Bee 与 Ant 迁移完成后直接删除该公共入口，不新增别名包、不保留
deprecated wrapper，也不让它成为 Core 的第四层门面。

## Core Component Host

Core 只认识中立组件，不出现 `CMod`、Bee、Ant、Storage、Actor 或 Procedure 业务语义。建议公开的高级 SPI 为：

```go
package component

type ID string

type Spec struct {
	ID       ID
	Requires []ID
	After    []ID
}

type Component interface {
	Spec() Spec
	Start(Context) error
}

type Validator interface {
	Validate(Context) error
}

type Drainer interface {
	Drain(context.Context) error
}

type Stopper interface {
	Stop(context.Context) error
}
```

`component.Context` 只暴露当前组件的只读根上下文和独立 `lifecycle.Tasks`，不提供能力注册表和动态类型查询。
Bee/Ant 通过私有适配器把 `CMod` 转换为 `component.Component`；CMod 的强类型依赖在构造时捕获，Component Host
不负责运行期依赖注入。

标准 agent/msgq 的监听、邮箱和 Redis adapter 作为中立实现保留在 Core `modules` 下。Bee 与 Ant 分别在自己的
`services/agent`、`services/msgq` 包提供同名、同签名的薄 Registration；薄层只转换 Context 和 CMod 依赖，不复制
网络、邮箱或 Redis 算法，也不允许 Ant import Bee。

`bee.CModID` 与 `ant.CModID` 是两个独立定义的强类型，内部适配时才转换为 `component.ID`。它们不能定义成同一个
Core 类型的别名，避免项目把 Ant 能力误传给 Bee Builder 后仍能通过编译。

Component Host 固定执行：

```text
校验 Spec
  -> 解析 Requires/After
  -> 稳定拓扑排序
  -> 全部 Validate
  -> 顺序 Start
  -> 失败时逆序回滚
  -> 逆序 Drain
  -> 取消并等待每个组件的 Tasks
  -> 逆序 Stop
```

`Requires` 会把目标加入依赖闭包；`After` 只在目标已经被选择时调整顺序。重复 ID、缺失依赖、依赖环和未知状态
默认失败，并输出完整依赖路径。

## Bee 框架门面

Bee 面向权威状态位于 Redis、Mongo 或其他外部系统的无状态游戏后端。进程实例可替换、可横向扩缩容；会话粘滞、
Actor 邮箱顺序和业务幂等仍按各自契约保证，“无状态”不等于消息无序。

Bee 提供：

```go
bee.Server
bee.Builder
bee.Config
bee.Context
bee.CMod
bee.CModID
bee.CModSpec
bee.CModContext
bee.Service
bee.ServiceSpec
bee.ServiceRegistration
```

内置 CMod 至少包含：

```go
bee.CModRedisStorage
bee.CModMongoStorage
bee.CModFusionStorage
bee.CModRPC
bee.CModDiscovery
bee.CModActor
```

Bee 不提供 `CModProcedure`、内存权威对象缓存、逻辑槽位或 WAL checkpoint 调度。无状态 Handler 修改 GD 后继续通过
项目 facade 调用 `db.Save(role)` 或 `db.SaveChange(role)`。

典型入口：

```go
app, err := bee.New(config).
	Services(
		agent.Registration(config.Agent),
		msgq.Registration(config.MsgQ),
		game.Registration(config.Game),
	).
	Build()
```

## Ant 框架门面

Ant 面向进程内存持有运行期权威状态的 SLG/MMO 后端。实体采用稳定归属，业务修改通过 Procedure 完成，提交后的
脏根对象由 Cache 组件登记并按策略刷盘；可选 WAL 用于需要可靠追加和 checkpoint 的状态管线。

Ant 提供与 Bee 一致的基础门面形态，并增加：

```go
ant.CModProcedure
ant.CModProcedureBuilder
ant.CModProcedureContext
ant.CModProcedureCache
ant.CModProcedureWAL
```

Ant 仍可组装 agent、msgq 等服务；这些服务在 Ant 项目中不自动获得 Procedure，只有显式声明 `CModProcedure` 的 L4
服务才能取得该能力。这样 `txgameslg` 可以保持一个完整、独立的项目，同时不会让 Bee 与 Ant 互相依赖。

典型入口：

```go
procedure, err := ant.NewCModProcedureBuilder().
	Cache(cacheComponent).
	WAL(walComponent).
	Build()
if err != nil {
	return nil, err
}

app, err := ant.New(config).
	CMods(procedure).
	Services(
		agent.Registration(config.Agent),
		msgq.Registration(config.MsgQ),
		slggame.Registration(config.SLGGame),
		slgworld.Registration(config.SLGWorld),
	).
	Build()
```

## CMod 命名规则

CMod 相关公开符号统一以 `CMod` 开头，使代码补全可以按能力命名空间检索：

```go
type CModID string
type CMod interface { /* ... */ }
type CModSpec struct { /* ... */ }
type CModContext interface { /* ... */ }

const CModActor CModID = "actor"

func NewCModActor(...) (CMod, error)
```

其他枚举值采用“类型名作为逻辑前缀”的规则：

```go
type Placement uint8

const (
	PlacementVirtual Placement = iota + 1
	PlacementFixed
)
```

`Err*`、`New*`、`With*`、`Is*` 和 `Has*` 按 Go 既有惯例保留。私有实现可以使用紧凑名称，不为代码补全制造冗长符号。

## Bee 与 Ant API 对称性

Bee 和 Ant 面向不同运行模型，但不提供两套不同的使用习惯。共有能力必须采用相同的类型名、方法名、参数顺序、
生命周期语义和错误规则：

```go
bee.New(config).CMods(...).Services(...).Build()
ant.New(config).CMods(...).Services(...).Build()

bee.Server.Run(ctx, options)
ant.Server.Run(ctx, options)

bee.CModActor
ant.CModActor

bee.Context.CModActor()
ant.Context.CModActor()
```

固定规则：

- `Builder`、`Server`、`Config`、`RunOptions`、`Context`、`CMod*`、`Service*` 的共有部分保持同名同义。
- Redis、Mongo、Fusion、RPC、Discovery 和 Actor 等共有 CMod 在两个命名空间中提供对称构造方法和 accessor。
- `Start`、`Run`、`Drain`、`Stop` 的顺序、幂等性、错误合并和超时行为保持一致。
- Ant 只增加有状态能力，例如 `CModProcedure`、逻辑槽位和相应配置；不能重定义共有能力的语义。
- Bee 不为追求表面完全一致而暴露无意义的 Transaction、Cache、WAL checkpoint 或 LogicalSlot 空入口。
- 示例项目中的同类 L4 Service 使用相同的 `Registration -> Factory -> Dependencies -> Service` 组织方式。

通过公开 API 对称性测试比较两个 package 的共有符号和关键方法签名。允许 Ant 存在增量符号，但同名共有符号若签名或
语义不同，测试必须失败。这样维护人员从 txgame 切换到 txgameslg 时，主要变化只有 import、配置和额外的
`CModProcedure` 组合。

## Service 延迟构造与强类型注入

L4 Service 分为静态注册信息和运行实例。框架必须在创建外部资源前读取静态 `ServiceSpec`，但只有所需 CMod 全部
启动后才能构造 Service，因此使用以下契约：

```go
type ServiceFactory func(Context) (Service, error)

type ServiceRegistration struct {
	Spec    ServiceSpec
	Factory ServiceFactory
}

type Service interface {
	Start(context.Context) error
}

type ServiceDrainer interface {
	Drain(context.Context) error
}

type ServiceStopper interface {
	Stop(context.Context) error
}
```

启动流程：

```text
读取全部 ServiceSpec
  -> 选择 --services
  -> 展开 CMod 闭包
  -> 校验并启动 CMod
  -> 为每个 Service 创建独立 Context/Ingress/Tasks
  -> 调用 Factory(Context)
  -> 得到只持有强类型依赖的 Service
  -> 调用 Service.Start(context.Context)
```

`bee.Context` 和 `ant.Context` 只允许在 Factory 内读取已选择的能力。每个 CMod accessor 返回“窄能力接口 + error”，
不返回合法但含义不明的 `nil`：

```go
actor, err := runtime.CModActor()
if err != nil {
	return nil, err
}
storage, err := runtime.CModFusionStorage()
if err != nil {
	return nil, err
}
```

若 `ServiceSpec.Requires` 未声明对应 CMod，accessor 必须失败并指出 Service 名、请求能力和完整已选择能力集合。存储
accessor 还必须返回按 Redis、MongoDB、Fusion 拆开的窄 reader/writer；writer 在第一次 I/O 前校验 TypeSpec 模式，
不能因为同进程其他 Service 选择了更多后端而越权。声明检查只发生在启动构造边界，模式检查只发生在写入边界，
都不进入 Handler 或 Actor 邮箱热路径。

Service 包随后把能力收敛为项目自己的依赖结构：

```go
type Dependencies struct {
	Actor   actor.Messenger
	Storage storage.ChangeApplier
}

func Registration(config Config) bee.ServiceRegistration {
	return bee.ServiceRegistration{
		Spec: ServiceSpec(),
		Factory: func(runtime bee.Context) (bee.Service, error) {
			actorRuntime, err := runtime.CModActor()
			if err != nil {
				return nil, err
			}
			storageRuntime, err := runtime.CModFusionStorage()
			if err != nil {
				return nil, err
			}
			return New(Dependencies{
				Actor:   actorRuntime,
				Storage: storageRuntime,
			}), nil
		},
	}
}
```

Handler、Procedure 和领域模块不得保存 `bee.Context`、`ant.Context`、CModID 或 Component Host。高级用户扩展新的
CMod 时仍通过 Bee/Ant 的 CMod SPI，而不是取得 Core Host 所有权。

本设计不使用反射填充 `Dependencies`，也不使用 `Get(type)` 服务定位器。依赖缺失在 Service Factory 阶段返回 error，
不会拖延到第一条业务消息才暴露。

## CModProcedure

`StateRuntime` 的真实职责是 Procedure 单机事务及其提交后的缓存与持久化管线，因此目标名称固定为 `CModProcedure`。

```text
Procedure
  -> 锁排序、等待图、undo、savepoint
  -> CommitParticipant
  -> Cache 登记脏根对象
  -> 分帧 Flush
     -> Fusion Writer
     -> 或 WAL -> checkpoint -> Mongo
```

边界：

- `CModProcedure` 不管理 Actor、实体路由或网络连接。
- Transaction Runtime 只负责单机事务语义，不直接执行存储 I/O。
- Cache 组件负责接收已提交根对象、去重、按帧调度和最终 Flush。
- WAL 是 Cache 持久化管线的可选组件，不是启用 Procedure 的前提。
- `CModProcedure` 不隐式选择 Fusion、Mongo 或 WAL backend；L4 Service 必须显式声明实际使用的存储 CMod。
- 无状态 Bee 不提供该能力，也不生成空实现。

Builder：

```go
procedure, err := ant.NewCModProcedureBuilder().
	Cache(cacheComponent).
	WAL(ant.NewCModProcedureMongoWAL("slgworld")).
	Build()
```

第一版继续允许 WAL 只对指定 L4 服务启用。未命中的服务只启动 Procedure 与 Cache，不创建 WAL backend。

## 配置边界

Bee 和 Ant 分别拥有面向使用者的强类型配置，不共享一个包含全部可选字段的大配置对象：

```go
bee.Config
ant.Config
```

二者可以复用 Core 组件配置类型，例如 Redis route table、Mongo route table、Actor Config、Discovery Config 和
Lifecycle Config，但不能通过 import Bee/Ant 复用框架配置。

固定规则：

- tx 不读取 YAML、环境变量或命令行。
- txgame 把 debug/test/release YAML 转换为 `bee.Config`。
- txgameslg 把独立 YAML 转换为 `ant.Config`。
- `--services`、InstanceID、InstanceVersion、Endpoint 和 LogicalSlot 仍属于部署启动参数。
- Bee Config 不出现 Transaction、Cache、WAL checkpoint 和 LogicalSlot 配置。
- Ant Config 可以包含这些有状态能力，但未被本轮 CMod 闭包选择的配置不做语义校验、不创建资源。

## 生命周期与错误语义

Bee 和 Ant 的 `Server` 对外保持一致生命周期：

```go
Run(context.Context) error
Start(context.Context, RunOptions) error
Drain(context.Context) error
Stop(context.Context) error
```

固定顺序：

```text
解析 Service/CMod
  -> Validate 全部 CMod
  -> Start CMod
  -> Factory 构造 Service
  -> Start Service
  -> 发布 Context 与 Running 状态

停止外部准入
  -> Drain Service
  -> 等待 Service Ingress
  -> Stop Service
  -> 逆序 Drain CMod
  -> 取消并等待 Component Tasks
  -> 逆序 Stop CMod
```

约束：

- 每个 Service 的 `Context` 只在全部 CMod 启动成功后创建，并且只传给该 Service 的 Factory；它不是
  `Server.Context()` 一类可由外部长期取得的全局对象。
- Start 失败必须清理当前 Service、此前 Service 和已经启动的 CMod。
- Drain/Stop 必须幂等；并发 Stop 等待同一次关闭结果。
- Redis、Mongo、etcd、gRPC 等服务波动返回 error，不得导致业务进程 panic。
- 未知状态、缺失能力和未处理返回值默认失败，不能静默成功。
- 排水和关闭期间必须保持依赖可用，直到使用它们的上层 Service/CMod 完成收尾。

## 迁移策略

迁移按以下阶段执行，每个阶段都必须保持全部受影响工程可测试：

### 阶段 1：提取 Core Component Host

- 从现有 `server` 提取纯依赖图和生命周期宿主到 `component`。
- 现有 Server 临时改为使用 Component Host，公共 API 暂不变化。
- 用现有启动、回滚、排水、关闭测试证明行为不变。

### 阶段 2：建立 Bee 并迁移 txgame

- 建立 Bee Config、CMod、Server、Context 和 ServiceRegistration。
- Bee CMod 组合现有 Core 组件，不复制 Redis/Mongo/Fusion/RPC/Discovery/Actor 算法。
- 迁移 txgame 配置转换、组合根和 L4 服务。
- txgame 完成后禁止 import `tx/server` 和 `tx/ant`。

### 阶段 3：建立 Ant 与 CModProcedure

- 建立 Ant 对应门面并复用 Core 组件。
- 将 StateRuntime 契约和实现迁移、重命名为 CModProcedure。
- 保持 Procedure、Cache、WAL、checkpoint 和关闭排空语义不变。
- 对 CModProcedure 运行事务、Cache、WAL 的定向测试与 benchmark。

### 阶段 4：迁移 txgameslg

- 迁移独立配置、组合根、agent/msgq/slggame/slgworld 注册。
- slggame 验证 `Procedure -> Cache -> Fusion`。
- slgworld 验证 `Procedure -> Cache -> WAL -> checkpoint -> Mongo`。
- txgameslg 完成后禁止 import `tx/server` 和 `tx/bee`。

### 阶段 5：删除旧 Server 并收敛文档

- 删除旧 `tx/server.Server`、巨型 Context、CMod 定义和过渡测试。
- 删除迁移中只为旧入口服务的适配代码，不保留兼容别名。
- 更新 tx、txgen、txgame、txgameslg 的 README、design 和实施状态。
- 将本规格中的目标架构、包边界、API 对称性、Service 注入和 CModProcedure 说明整理进 `docs/design.md`；
  `docs/design.md` 只描述最终实现和技术取舍，不叙述重构前的包结构、旧名称或迁移过程。
- 搜索并清理 `StateRuntime`、旧 CMod 名称和旧 import 路径。

## 验证标准

### 架构

- Core、Bee、Ant、txgame、txgameslg 的 import 方向满足本设计约束。
- txgen 生成产物不依赖 Bee 或 Ant。
- `tx/server` 最终不存在公共业务入口。
- Bee API 中不存在 Transaction、Cache、WAL checkpoint 或 LogicalSlot。
- Ant 的 Procedure 能力统一以 `CModProcedure*` 命名。
- Bee 与 Ant 的共有公开 API 通过对称性测试，Ant 只增加有状态能力。

### 行为

- Bee 的 Redis、Mongo、Fusion、RPC、Discovery、Actor 功能行为不变。
- Ant 的 Procedure 回滚、动态子过程、死锁等待图和提交参与者行为不变。
- Cache Flush 失败保留变更，成功后才清理 Store。
- WAL 追加成功后才清理本帧变更，checkpoint 可异步推进。
- 启动失败、运行期 backend 波动和关闭超时都返回可诊断 error，不 panic、不静默成功。

### 性能

- Service Context 和 CMod 解析只发生在启动路径，不进入业务热路径。
- Service 构造完成后只持有强类型窄接口，不做 CModID 查询或类型断言。
- Component Host 依赖解析不要求零分配，但 Actor 投递、Store 标脏、变更文档生成和 Fusion 保存基准不得退化。
- CModProcedure 重命名和迁移前后 Transaction、Cache Flush、WAL Append benchmark 保持同一数量级；显著退化必须先定位原因。

### 测试

每个阶段至少执行：

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
```

关键阶段额外执行：

- `go test -race` 覆盖 Component Host、生命周期、Procedure、Cache 和 Actor/Discovery 目录。
- 使用真实 Redis `127.0.0.1:6379`、Mongo `127.0.0.1:27017`、etcd `127.0.0.1:2379` 验证应用链路。
- `git diff --check`、公开符号审计、旧术语搜索和架构 import 测试。

## 最终成功条件

完成后，普通使用者只需要理解以下入口：

```go
bee.New(config).Services(...).Build()
ant.New(config).CMods(procedure).Services(...).Build()
```

Core 维护者可以独立演进算法、backend 和 Runtime；Bee/Ant 维护者只通过稳定 Core API 组合框架能力；项目业务
只在 L4 Service Factory 中接触一次框架 Context，运行期 Handler 和领域代码只依赖自身的强类型 `Dependencies`。
