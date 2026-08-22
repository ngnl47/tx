# tx 设计文档

本文面向 `tx` 的维护者、adapter 作者、代码生成器维护者，以及需要扩展底层能力的高级使用者。它解释框架为什么这样拆分、各模块内部如何协作、哪些状态可以共享、哪些边界必须失败，以及性能优化应以什么证据推进。

普通业务开发者应先阅读仓库根目录的 [README](../README.md)。README 是公共行为和使用方式的事实来源；本文是内部结构和技术选型的事实来源。当前进度见 [实施计划](IMPLEMENTATION_PLAN.md)，质量要求见 [架构审计标准](architecture_audit_standard.md)，某次审计的数字和证据见 `docs/audits/`。

## 阅读路径

本文按“系统职责 -> 数据内核 -> 存储 -> 单机事务 -> 分布式模块 -> 性能与维护”逐步深入：

1. 初次维护 `tx`：阅读“设计目标”“系统边界”和“依赖方向”。
2. 修改 GD、Store 或生成契约：阅读“数据对象与 Store”“生成式变更内核”。
3. 修改 Redis、Mongo 或 Fusion：阅读“存储抽象”和“存储后端”。
4. 修改 Procedure 或有状态运行时：阅读“单机事务”和“事务锁注册表”。
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

`tx` 的最高优先级不是“所有路径都最快”，而是以下顺序：

1. 不产生脏数据，不把失败伪装成成功。
2. 业务 API 清晰、简洁、强类型，框架可以代理的细节保持黑盒。
3. 数据内核与存储、网络、并发调度保持单向依赖。
4. 高频路径以 benchmark、race、逃逸分析或 profile 为依据优化。
5. 低频路径优先正确性和可维护性。

由此得到几条贯穿全部模块的规则：

- 未识别状态默认失败，不默认成功。
- 存储成功前不能清理 dirty 状态。
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
- 同步原语以职责命名。事务锁表使用 `contextMutex`，内部容量为一的 channel 只叫 `permit`，调用方不感知 channel 实现。
- 时间、容量和字节配置在名字中写清单位或口径，例如 `TTLSeconds`、`MaxBacklogMessages`、`HeapSoftLimitBytes`。
- JSON、BSON、Msgpack 属于字段命名体系；Redis、Mongo 属于使用这些名称的存储边界，不为存储边界重复生成字段名身份。

公开名称一旦让调用者误解职责，应直接修正并同步生成代码和文档；当前工程不为未发布的中间名称保留兼容别名。

## 系统边界

### 三个工程的职责

```text
txgen 输入 Go 源文件
        |
        v
txgen 编译期扫描和生成
        |
        v
txgame/server/gen/data 与 gen/cmd
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

- 是生产工程组织方式和集成验证模板。
- 读取 yml、环境变量和命令行参数，构造具体 client 与实例配置。
- 提供 `db.Save(role)` 之类业务 facade。
- 组织 `services/*` 微服务和 `domain/modules/*` 业务模块。

### API 分层

同一个能力可能有三层入口：

| 层级 | 面向对象 | 示例 | 约束 |
| --- | --- | --- | --- |
| 业务层 | handler 和业务模块 | `db.Save(role)` | 不暴露 writer、Store、RecordKey 和根 context |
| 扩展层 | 项目装配和 adapter | `storage.Save(ctx, writer, role.Store())` | 显式表达存储依赖和生命周期 |
| 内核层 | 框架维护者和高级扩展 | `VisitChanges`、`txn.WithLocks` | 调用方必须理解所有权和并发契约 |

新增 API 时先判断它属于哪一层。不能因为测试方便，把内核对象泄露到业务层；也不能为了隐藏一切，阻断高级使用者实现 adapter 的可能。

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
lifecycle <- discovery / rpc / msgq / agent / actor / control / txn runtime
```

关键约束：

- `change` 不能依赖 `storage`，否则纯变更算法会被具体 I/O 语义污染。
- `storage` 通用契约不能依赖 Redis 或 Mongo adapter。
- `modules/actor` 不代表 GD，不依赖 Store、Fusion 或 `txn.Runtime`。
- `modules/txn` 可以依赖 `change` 和通用 `storage` 契约，但不能依赖具体 backend。
- 项目业务可以组合 Actor、Procedure 和存储 runtime，模块之间不反向耦合。

全局 registry、回调和 `any` 也可能形成隐式反向依赖。审查依赖时不能只看 import cycle。

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

生成 resolver 负责 `FieldID -> go/json/bson/msgpack name` 的静态映射。Redis、Mongo、Provider 在各自边界选择对应命名体系，运行时不通过反射重新推导生成期已经知道的关系。

字段编号从 1 开始连续分配，因此生成代码用私有定长表保存 Go 名和 JSON 名，通过 FieldID 直接索引。业务侧需要把 Go 字段名传给 `LoadFields`、`FindOptions` 等接口，所以生成 `GD*Field<Field>` 公开常量；JSON、BSON、MessagePack 名只在序列化和存储边界使用，不额外导出逐字段常量。BSON、MessagePack 仅为与 JSON 不同的字段生成覆盖分支，例如 Mongo `_id` 或协议短名。该布局避免按每个字段展开四套重复 switch，同时保持无反射、零运行时字符串转换和零分配。

生成代码还会基于上述名称表和紧凑字段类型表构建一份包内私有的只读 `TypeSpec`。生成的 Load、Find、Delete、SaveChange 和类型注册路径直接复用该静态元数据，不在每次 I/O 前重新展开字段描述。公开的 `GD*TypeSpec()` 与 `Store.TypeSpec()` 则通过 `meta.CloneTypeSpec` 返回隔离副本，调用方修改字段切片、存储指针、字段配置 map 或 Mongo 索引时不会污染生成包的静态规范。

`CloneTypeSpec` 只负责深拷贝可变成员；`TypeRef` 的去空格和完整名称补齐属于 Registry 注册边界。所有接收私有静态 `TypeSpec` 的 tx 内部 adapter 必须把它视为只读输入，不能修改其中的 map、slice 或指针成员。

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

它可以被 JSON、BSON、MessagePack 编解码，可以直接读取 public field，也可以用于 snapshot diff。GD 不负责：

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

### Store 树

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

叶子 Store 只保存自己的局部字段状态；集合 Store 保存发生变化的 key/index 及对应子 Store，父级通过 `notifyDirty` 只记录“这个分支需要访问”。调用根 Store 的 `VisitChanges(visitor)` 时，框架才递归访问 dirty 分支、追加集合 key/index，并得到：

```text
Items[1001].Quantity
```

因此 Store 树确实分层保存局部变更状态，但不会在每次修改时遍历整棵树，也不会让根 Store 长期保存一份已经展开的全路径副本。所谓“冒泡”是 dirty 通知：子节点通知父集合对应 key/index 已变更；真正的路径拼接发生在输出边界。根 Store 只递归访问 dirty 索引，使用 `JoinPathLimited` 组合路径。生成 Store 的状态在记录时已经按字段或 key 唯一化，不需要输出后再做第二次压缩。父子关联属于 Store 路径关系，不代表 GD 对象只能有一个父节点。

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

Store 拦截模式：

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
    └── GDRoleItemsCollectionStore          集合 Store
        ├── ops[1001] = OpReplace           集合元素槽
        └── dirtyChildren[1002]
            └── GDItemStore
                └── ops[Quantity] = OpSet   子对象字段槽
```

Store 不保存第二份业务数据，也不保存每次操作的 value、时间或顺序。`GDRoleStore.Value`、集合 Store 的 `parent` 和子 Store 的 `Value` 都指向当前 GD 数据；Store 只保存“哪些持久化位置需要按什么语义重新读取”。

### 限界上下文与固定术语

后续设计、评审和文档使用下表术语。一个词只在对应上下文内表达一种职责：

| 限界上下文 | 固定术语 | 对应 Go 结构或方法 | 精确定义 |
| --- | --- | --- | --- |
| 数据上下文 | 根 GD | 生成的 `GD*`，例如 `GDRole` | 一个可持久化聚合的业务数据对象，也是当前值的唯一事实源 |
| 数据上下文 | 当前值 | `GD*` 的字段 | 输出边界实际读取的最终业务值，不是 Store 中保存的副本 |
| 追踪上下文 | 主 Store | `GD*Store`、`GD*.Store()` | 绑定根 GD、允许受控修改并可交给持久化入口的 Store |
| 追踪上下文 | Store 状态树 | `GD*Store`、直接指针子 Store、集合 Store | 按生成类型和持久化路径组织的变更状态；树节点不保存父路径字符串 |
| 追踪上下文 | 字段槽 | `GD*Store.ops[fieldID-1]` | 顶层或子对象普通字段的最后变更语义 |
| 追踪上下文 | 集合元素槽 | `CollectionStore.ops[key/index]` | map key 或 slice index 本身的最后 Replace/Delete 等语义 |
| 追踪上下文 | 脏子节点索引 | `CollectionStore.dirtyChildren` | 只标识哪些缓存子 Store 需要遍历，不保存路径或 value |
| 追踪上下文 | 脏通知 | `dirtyHook`、`notifyDirty`、`markChildDirty` | 把子 Store 加入父级待遍历索引或通知运行时；不是完整变更向根复制 |
| 加载上下文 | 已加载字段状态 | `loadedFields`、`LoadedFields()` | 描述部分加载结果，与字段是否变更相互独立 |
| 遍历上下文 | 变更源 | `change.Source` | 由 `HasChanges` 和 `VisitChanges` 组成的同步只读访问契约 |
| 遍历上下文 | 结构化路径 | `change.Path`、`PathNode`、`PathKey`、`FieldID` | 使用字段编号和强类型 key/index 表达的当前位置 |
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
| 容量上下文 | 容量收敛 | `storage.CollapsibleChangeStore`、`CollapseChanges()` | 单根变更位置超预算后，清理细粒度状态并改为可更新顶层字段 Replace |
| 输出上下文 | 边界文档构建 | `Document(kind)`、`change.BuildDocument`、Redis/Mongo document builder | 转换字段名、解析当前值并形成目标边界需要的文档 |
| 输出上下文 | 独立视图 | StoreView | 保存捕获时刻的稀疏值和路径状态，不再引用可变根 GD；完成累计后按只读视图使用 |
| 输出上下文 | 编码载荷 | BON/JSON/BSON `[]byte` | 由视图、变更文档或其他独立对象编码而成；发布后按只读载荷使用 |
| 发布上下文 | 只读发布 | `msgq.Pub` 等异步所有权交接 | 发布成功后调用方和消费方都不得修改、追加或复用载荷内存 |
| 持久化上下文 | 保存成功清理 | `storage.Save`、`Store.ClearChanges()` | 存储成功后递归清理 Store 状态，不修改 GD 数据 |
| 持久化上下文 | 保存失败保留 | `storage.Save` 的错误路径 | I/O 或边界构建失败时保留全部状态，供后续重试 |
| 事务上下文 | 撤销记录 | `change.UndoRecorder`、`change.UndoFunc` | 恢复事务前 GD 值和 Store 状态；与帧内合并职责独立 |

“帧”在这里表示一次业务合并窗口，不限定为 20ms，也不是网络 `Frame`。Store 状态本身是按位置归一化的集合，遍历顺序不构成业务契约；`Capture` 的调用顺序和 `Merge(next)` 的参数顺序都具有“后批次覆盖前批次”含义。

### 生成 Store 的常驻状态

生成 Store 是持久化和下行变更构建共用的变更内核。txgen 已经知道每个类型的字段数量、字段 ID、集合类型和子对象类型，因此生成代码直接保存规范化状态：

```go
type GDRoleStore struct {
    Value *GDRole
    ops   [10]change.Op
    // generated child/collection stores...
}

type GDRoleItemsCollectionStore struct {
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
- `View(configure)` 只在回调期间复制这些规范化状态作为 selector；捕获完成后返回独立的 StoreView，不返回复制的 Store 树。

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

集合内部按路径创建的子 Store 使用集合自己的 `stores` 绑定，不会把共享的子 GD 强行绑定到某一条父路径。这样同一个 `*GDItem` 可以出现在多个角色或多个集合中，每条路径仍独立记录 dirty 状态。

### 同步遍历、捕获与帧内合并

主 Store 的 `VisitChanges(visitor)` 在调用栈内遍历规范化状态，按值传递 `Path + Op`。visitor 必须在回调内完成处理，不能把主 Store 或根 GD 指针交给无锁异步队列。Redis、Mongo、Fusion 与事务 Runtime 直接消费这一契约。

`View(configure)` 分成三个同步阶段：

1. 没有 configure 时直接遍历主 Store；有 configure 时，`selectionCopy()` 才复制 `ops`、集合元素槽、dirty 索引和必要子 Store，形成临时 selector。
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

这使同一路径在一个周期内修改多次时无需保存多个中间值。生成 Store 的字段槽、集合状态和子 Store 索引已经只保留最终 dirty 语义。

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

因此 Redis/Mongo/Fusion adapter 与 `txn.Runtime` 直接消费 `Source`。旧的 `Build*WithDeltas` 与显式批次类型已经删除，`storage.Save` 不得重新引入中间批次层。

2026-08-22 删除显式批次 API 与过渡适配层后，使用 Go 1.26.5 在同一台 x86_64 macOS 设备复测：`VisitChanges1024` 为约 `0.082-0.088 ms/op, 0 B/op, 0 allocs/op`，`BuildDocument1024` 为约 `0.389-0.399 ms/op, 212 KB/op, 2996 allocs/op`，`MergeAndBuildFrameDocument` 为约 `0.167-0.174 ms/op, 37 KB/op, 17 allocs/op`。txgame 的真实生成结构边界中，Redis patch 为约 `0.873-0.928 ms/op, 359 KB/op, 4691 allocs/op`，Mongo update document 为约 `0.988-1.041 ms/op, 330 KB/op, 5717 allocs/op`。这些结果与删除前的同步遍历路径处于同一量级，说明精简公开 API 和运行时中间层没有引入可见性能回退。

同日增加公开 `Path` 合法性防线后，先消除边界、`FullPath` 与生成 resolver 的重复扫描，再以相同 1024 key 场景复测：Redis 为约 `0.920-0.992 ms/op, 359 KB/op, 4691 allocs/op`，Mongo 为约 `0.984-1.029 ms/op, 330 KB/op, 5716 allocs/op`。结果仍处于原有波动区间且分配次数未增加；完整节点校验只在 I/O/文档边界执行一次，生成 resolver 仅保留固定数组深度护栏。

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
- `DeleteApplier`：显式删除。
- `RecordLocker`：记录级互斥。
- `RecordRevisionReader` / `RevisionDeleteApplier`：版本保护删除。

Redis、Mongo 和 Fusion 通过组合这些接口表达能力。公共 API 可以保持 `storage.Save`，而不是暴露 `ApplyInsert/ApplyUpdate/ApplyReplace` 组合给业务。

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

只要策略整体没有成功，就不能清理 Store。Fusion 中 Redis patch 成功但延迟任务入队失败时，调用仍返回错误并保留变更状态，下一次重试允许重复写 Redis并再次登记任务。

### 文档返回值严格性

Loader 返回值必须成对成立：

- `exists=true` 时 document 非 nil。
- `exists=false` 时 document 必须 nil。

Replacer：

- 空 map 是合法空对象。
- nil map 没有明确写入语义，返回 `ErrNilDocument`。

未知 Lua 返回值、损坏 JSON、无有效变更、字段路径无法解析等都返回错误。不能把“不知道发生了什么”归类为成功。

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

### 为什么不是 Redis Hash 或 RedisJSON

当前默认选择普通 Redis string JSON：

- 所有 Redis 环境都支持，不依赖模块。
- Lua 可以原子合并 main/subKey 文档和版本。
- 与参考生产路径和云 Redis 代理兼容。

代价是 Lua 局部 patch 需要解码和重新编码对应 JSON 文档。RedisJSON/ReJSON 作为未来可选 backend 评估，不改变通用 Store 和变更遍历契约。

### Lua patch

Redis Lua 在对应 main/subKey 文档内应用 set/delete/clear/replace。局部路径缺少父对象时可以创建正确的中间 table；如果已存在父节点类型与路径要求冲突，返回错误，不把异常标量自动替换成容器。

严格失败是为了暴露数据来源问题。运行时自动修复异常拓扑可能掩盖旧代码或人工写库造成的数据损坏。

### Cluster 路由

同一记录的 main、subKey、`@v` 和 `@lock` 必须共享 hash tag。云代理或单节点可直接执行 MGET；本地原生 Redis Cluster 在客户端按 slot/hash tag 分组后多次 MGET。

Lua 的 `KEYS[]` 是 Redis 命令显式标记的 key 参数。任意一次 Lua 调用涉及的 key 必须同 slot；云代理不能分析脚本内部任意字符串并替调用方拆分执行。

## Mongo 后端

Mongo 文档主键必然是 `_id`，但业务 `idField`、`routeBy` 和 BSON `_id` 绑定承担不同职责：

- BSON `_id`：Mongo 文档物理主键字段。
- `idField`：框架生成 RecordKey 和过滤条件所需业务字段。
- `routeBy`：选择 Mongo endpoint/shard 的路由字段。

同一字段可以同时承担这些角色，也可以分开。生成期必须校验 `_id` 绑定唯一，避免 routeBy/idField 与其他 `_id` 字段产生冲突。

局部变更最终转换为 Mongo `$set` / `$unset` 文档。Replace 复杂对象时，边界读取当前完整值并整体写入该路径；不能按子字段 set 后遗留旧 BSON 字段。

Mongo 写入不是 setter 热路径，优先使用官方 driver 的 BSON tag 映射保证正确性；只有 benchmark 证明必要时才为具体字段生成更低层编码。

## Fusion 后端

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
Redis 同步 patch + @v 递增
      |
      v
Redis 延迟队列登记记录定位
      |
      +---- 当前请求成功并清理 Store 变更状态

到期 worker
      |
      v
claim task -> 读取 Redis 当前完整文档 -> Replace Mongo -> complete
```

延迟任务不保存 GD 快照，也不保存每次变更。十分钟内修改同一角色多次，任务到期只读取一次 Redis 最终状态。

### at-least-once

claim、lease、retry 和进程重启决定任务是 at-least-once。Mongo 回写必须幂等：同一 RecordKey 的当前 Redis 文档重复 Replace，只能得到相同或更新后的终态，不能重复发奖或累计统计。

重试原因写日志，不写入 Redis error hash，避免不可控错误文本占满内存。退避达到最大值后持续按封顶间隔重试，不能因次数耗尽丢弃持久化任务。

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
        +-- miss -> 读 Mongo 全文 -> 带 TTL 回填 Redis -> 返回所需字段
```

二次读取避免两个实例同时 miss 时，较晚返回的旧 Mongo 快照覆盖已经回填的新 Redis 数据。

批量读取先批量查询 Redis，再批量取得 Mongo 候选。每条 miss 在自己的 RecordLock 内二次读 Redis；锁内已命中的记录丢弃旧 Mongo 候选，仍未命中才回填。这保证正确性优先，同时保留 Mongo 批量 I/O。

### 热数据删除与 revision

`delayDelTime` 是 Redis 热数据 TTL，不是 Mongo 删除时间。延迟热数据删除必须：

1. 读取当前 `@v`。
2. 读取 Redis 当前热文档并回写 Mongo。
3. Lua 比较读取时 revision。
4. revision 未变化才删除 main/subKey/version。
5. revision 已变化说明有新写入，保留热数据并重新排队。

显式业务删除则在同一 RecordLock 下立即删除 Redis 和 Mongo，并与延迟保存串行，避免旧热数据重新写回 Mongo。

### 健康闸门

运行模式的先验条件：

| 模式 | 必须健康的 backend |
| --- | --- |
| Redis | Redis |
| Mongo | Mongo |
| Fusion | Redis 和 Mongo |

启动时探活并 fast-fail；运行期由真实 I/O 错误和后台探针维护 backend generation。每次业务调用不额外 Ping。

Redis 与 Mongo 的 Adapter/Cluster 构造器都在启动装配阶段校验完整 client、router 和 shard 集合，不能创建一个等待首次
业务 I/O 才失败的半装配对象。变更写入口把 nil 或 typed-nil `change.Source` 视为调用契约错误；有效 Source 明确返回
`HasChanges() == false` 时才执行无 I/O no-op。该区分避免“未提供输入”和“输入确实没有变化”被折叠成同一种成功结果。

Fusion 任一端已知不可用时，新业务读写返回 `ErrStorageUnavailable`，避免继续接受无法可靠持久化的状态。已经存在的延迟任务不被删除，worker 仍可继续重试，Mongo 恢复后正常消费。

只有 driver 能确认的网络、超时和服务端通信错误才包装为 `BackendError`。RecordKey、变更流、BSON、JSON、Lua 协议等本地语义错误不能错误地把全局 backend 标成不可用。

## 单机事务

### 职责

`modules/txn` 为有状态单机服务提供：

- 多个 GD 根对象的稳定加锁。
- Store 受控修改的 undo log。
- savepoint 和子 Procedure。
- 提交后脏目标收集。
- 与 `txn.Runtime` 配合的延迟 Flush。

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

一次根调用：

```text
Procedure.Locks
      |
      v
校验、排序、去重完整锁集
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
  |          |
  | error    | success
  v          v
反向 undo    丢弃 undo
  |          |
  +----解除 recorder----+
             |
             v
         反向释放锁
             |
             v
     AfterRollback / AfterCommit
```

AfterCommit 在释放数据锁后执行。它不能回滚已经提交的内存状态；需要可靠投递时应使用项目选择的 outbox 或可靠消息队列。

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

框架按 `Order -> Scope -> Value` 全序获取。所有事务遵守同一排序，避免循环等待。相同 Key 使用不同 Order 是定义错误；相同 Key 绑定不同 Store 也是错误。

锁集合必须在根事务前完整声明。若锁依赖查询结果，先只读查询、构建 LockSet、获取全部锁，再在锁内重新校验前置条件。运行中动态扩张锁集可能破坏全序，不受支持。

### 子 Procedure 与不可重入

Go `sync.Mutex` 和当前 channel 信号量都不可重入。框架不通过 goroutine ID 实现可重入锁：

- 嵌套 `txn.Run` / 根 `txn.Call` 被拒绝。
- 子逻辑使用 `tx.Call(child)`。
- child 可以声明根锁集的子集。
- `tx.Call` 只校验 Key 和 Store，不重新获取锁。
- child 引入新锁时返回包含 Procedure 路径的错误。

这样可复用 Procedure 不依赖第三方可重入锁，也不会在事务中等待自己。

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

事务内必须使用传入锁目标对应的同一个 Store 实例。否则 undo recorder 与实际修改对象不一致，框架会拒绝子 Procedure 的 Store 绑定。

## 事务锁注册表

### 进程级全局结构

事务锁注册表是进程级全局对象，不属于单个 Procedure：

```go
var transactionLocks lockTable

type lockTable struct {
    shards [64]lockShard
    pool   sync.Pool
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
  +-- role:1001 -> entry A -> token A
  +-- role:9009 -> entry B -> token B
```

即使两个 Key 哈希到同一个 shard，它们只在 reserve/release map 的极短临界区内竞争；事务执行期间分别持有 token A 和 token B，可以并行。

这与固定分段业务锁不同：

```text
固定分段业务锁：hash(key) -> Mutex，碰撞 Key 共用整段事务锁
当前精确 Key 注册表：hash(key) -> map shard -> map[key] -> 独立 entry
```

分片数量只控制注册表元数据竞争和固定 mutex 成本，不限制可同时存在的业务 Key 锁数量。

### channel 二元信号量

```go
type lockEntry struct {
    token chan struct{}
    refs  int
}
```

`token` 是容量 1 的 buffered channel。创建时放入一枚 token：

```text
[token]  空闲
[]       已被持有
```

获取：

```go
select {
case <-ctx.Done():
    return ctx.Err()
case <-entry.token:
}
```

释放：

```go
entry.token <- struct{}{}
```

选择 channel 而不是直接 `sync.Mutex` 的原因是锁等待必须响应 context timeout。token 和取消可能同时 ready，因此获得 token 后再次检查 `ctx.Err()`；如果已经取消，立即归还 token，不进入 Procedure。

channel 不提供可重入语义，也不承诺业务级严格 FIFO。正确性来自同 Key 单 token、稳定多锁顺序和超时失败释放。

### reserve、refs 与回收

reserve 流程：

1. 根据完整 LockKey 哈希选择 shard。
2. 短暂获取 shard mutex。
3. 精确查询 `entries[key]`。
4. 不存在时从 pool 取得或创建 entry。
5. `refs++`。
6. 释放 shard mutex。
7. 再等待 entry token。

`refs` 统计已经 reserve 的持有者和等待者，不记录 goroutine 所有权。它保证仍有人引用 entry 时不会从 map 删除或回收到 pool。

释放时先归还 token，再在对应 shard 下 `refs--`。归零后删除精确 Key 映射，并把 entry 放入 `sync.Pool`。pool 复用 channel，避免短生命周期离散 Key 每次分配 channel。

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

当前需求是完整锁集、稳定排序、context timeout 和子 Procedure 复用，而不是任意嵌套重入。第三方可重入锁通常需要识别 goroutine 或改变调用模型，还不能自动解决动态扩锁死锁。禁止嵌套根事务并让 child 复用根锁集，边界更明确、诊断路径也更可靠。

### 基准结论

2026-08-21 审计基准显示，分片精确 Key 注册表、entry pool、四锁内联和单锁快路径使锁内核单 Key、四 Key 和离散并发 Key 达到零分配；共享同 Key 仍严格串行。数字属于特定机器和 revision，后续判断应重跑：

```bash
go test ./modules/txn -run '^$' \
  -bench '^(BenchmarkLockTableAcquireRelease|BenchmarkWithLocks)$' \
  -benchmem -count=5
```

不要为了优化注册表而退回“哈希碰撞共用业务锁”的固定 stripe 方案。

## 有状态事务 Runtime

`txn.Runtime` 是 Procedure 的标准运行时搭档，不是项目额外实现的通用缓存框架。它负责：

- 调用 Procedure。
- 从 `Tx.DirtyTargets()` 登记需要 Flush 的根对象。
- 多次事务之间累计同一 Store 的变更状态。
- 定时在精确 Key 锁下 Flush。
- 写入失败时保留 pending 对象并重试。
- 关闭时配合 WaitGroupContext 完成最终限时 Flush。

Runtime 不复制 GD，也不异步保存脱离对象的变更值。Flush 必须再次取得对象锁，在锁内同步访问 Store 当前变更和 GD 当前值，写入成功后清理。

### 容量保护

容量保护分为：

- `MaxRootChanges`：单根路径条目超过预算时收敛为顶层 Replace。
- `MaxPendingRoots`：可选的待刷根对象数量硬限制。
- `HeapSoftLimitBytes`：进程 Heap 软告警，不拒绝业务。

`MaxRootChanges` 通过 Runtime 私有的 `Source` visitor 装饰器执行。未超预算时，adapter 在构建完整内存文档的同一次遍历中完成计数，随后才执行第一次外部 I/O；超预算时 visitor 返回内部错误，adapter 丢弃尚未写出的临时文档，Runtime 将 Store 收敛为顶层 Replace 后重新构建。这个契约要求所有 `storage.ChangeApplier` 都先完整访问变更并处理错误，再进行外部 I/O，不能边遍历边写远端。

HeapAlloc 受整个进程影响，不能作为 Manager 拒绝服务的唯一依据。外部存储不可用时，应由项目健康闸门先拒绝新的持久化业务；已经 pending 的对象保留并等待恢复。

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
取消后台循环并等待退出
        |
        v
最终限时 Flush
        |
        v
关闭外部 client 和撤销服务注册
```

最终 Flush 使用独立限时 context，不能因为运行 context 已取消而立刻失败。超时必须返回错误并输出当前可观测状态，不能无限等待或静默退出。

### 排水与关闭的区别

排水是入口策略：不再接受新的外部工作。关闭是资源生命周期：等待已进入工作，停止循环，Flush 并释放资源。

进入 draining 不应全局禁止所有数据库保存或 msgq 下行，否则已经受理的业务无法完成。Agent 上行和通用 RPC 入站通过 admission gate 封口；内部发送方收到 unavailable 后按自己的业务语义重试、补偿或丢弃。

## 分布式服务模块

### 总体拓扑

```text
Client
  |
  v
Agent -----> msgq session mailbox
  |
  +---- command route ----> game / room / battlefield service
                                |
                                +---- gRPC service call
                                |
                                +---- ProtoActor PID delivery

etcd
  +---- service instance lease/watch
  +---- actor cluster provider
  +---- fixed logical slot assignment
```

模块化意味着这些服务可以同进程启动，也可以独立部署。进程内组合不能改变跨进程语义；例如 msgq payload 的只读所有权规则在两种部署下都成立。

### discovery

`modules/discovery` 使用 etcd lease/watch 管理服务实例事实：

- 注册实例 ID、endpoint、version、services 和 status。
- keepalive 失效后自动摘除。
- `ServiceDirectory` 维护本地快照。
- 服务路由只选择 running 实例。
- 精确实例路由可以看到 draining/stopping，用于排空既有绑定。

实例 ID、endpoint、版本和服务列表在启动时动态构造，不写死在业务 yml。etcd 保存集群控制状态，不保存 GD 或业务地图数据。

### rpc

`modules/rpc` 是普通服务/实例 gRPC 投递：

- ServiceRoute：从 running 副本选择目标。
- InstanceRoute：精确投递给已知实例。
- payload 使用 protobuf envelope 承载业务 BON bytes。

它不实现 Actor mailbox、会话 Seq/Ack 或业务幂等。发送结果不确定时不能由框架盲目重试；业务必须有独立幂等键。

### msgq

`modules/msgq` 是客户端会话邮箱，不是所有服务消息的中心总线。它负责：

- `roleID -> session/connection/msgq instance` 绑定。
- 上行 Seq 连续性。
- 下行队列和 Ack。
- 断线重连时重发未确认消息。
- 本地 Topic 广播。

Redis 只保存带 TTL 的 owner 绑定；下行正文保存在 msgq 实例内存。实例崩溃允许丢失未确认下行，客户端重连后由业务状态重同步恢复。

### Agent

`modules/agent` 终止客户端 TCP/WebSocket 连接，处理 frame、握手身份、Seq/Ack 和路由描述。它不承载具体 game handler，也不要求所有命令先经过 game 中转。

生成的 `CmdDescriptor` 提供静态 CommandID、目标服务、Actor kind 和 route key 类型；项目 resolver 根据身份和业务映射决定具体 PID 或实例。

连接入口可以是真实客户端，也可以由项目扩展为本地驱动或旁路模拟连接。tx 提供基础网络 Agent，同时保持 dispatcher 接口开放。

### Actor

`modules/actor` 基于 ProtoActor-go 和官方 etcd Cluster Provider。它提供两类定位：

- Virtual Actor：按 `(kind, identity)` 由集群定位，适合无状态或可恢复实体。
- Fixed PID：业务已经知道 endpoint 和 actor ID，适合固定归属实体。

Actor 间跨实例通信是网状直连，不经过 msgq 中继。msgq 只负责客户端会话链路。

有状态 SLG/MMO 实体不能因为成员变化随机漂移。业务使用逻辑槽位和 generation 固定宿主；节点重启期间该槽位短暂不可用，恢复后原位重建。无状态轻量 Actor 才允许成员哈希变化后惰性重建。

### Actor mailbox

默认 Actor mailbox 有界且非阻塞：满时丢弃新消息，并在目标实例限频记录 overflow。普通 `Tell` 只表示本地 runtime 接受投递，不确认远端业务完成。

`Confirm` 使用 RequestFuture 等待目标 Actor 取出消息并返回 Acceptance：

- 确认的是接收，不是业务成功。
- 必须带 deadline。
- 不应成为所有消息的默认路径。
- 业务结果仍使用显式请求/响应协议。

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
WaitGroupContext 等待已受理工作
       |
       v
最终 Flush
       |
       v
旧实例 stopping / 撤销 lease
       |
       v
新 running 实例以 CAS 提升逻辑槽位 generation
```

固定槽位不能在排水窗口随机改派。异常宕机和网络分区下的自动安全接管需要独立 fencing 设计，不属于正常优雅发布。

## 跨 goroutine 数据所有权

Go 没有只读 slice。所谓“零拷贝广播”依赖显式所有权转移：

```go
result, err := mailbox.Pub(ctx, requestWithPayload)
```

成功后：

- 调用方不得修改 payload。
- 不得 `append`。
- 不得归还 buffer pool。
- 消费方只能读取。

本地 Topic 广播的多个队列条目可以共享同一份只读正文。跨进程 gRPC 在序列化边界自然复制。网络写入采用 header/body 分段写，不在发送 goroutine 给共享 payload 追加包尾。

如果调用方无法遵守所有权转移，边界必须复制。不能把共享可变引用称为 immutable。

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

`tx/log` 是基于 `slog` 的最小门面。库输出结构化事件，不持久化错误历史。项目可以替换默认 slog handler，实现级别、输出格式、日志中心和功能 logger。

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
5. 它的失败会不会错误清理 dirty、丢队列或改变健康状态？
6. 它是否引入新的跨 goroutine 所有权？
7. 是否已有成熟库能承担通用部分？

成熟依赖优先用于数据库驱动、网络协议、服务发现和标准编解码。变更追踪、Store、Fusion 和事务锁顺序承载 tx 的一致性语义，只有候选库完整表达这些契约时才替换。

## 文档维护规则

- README 记录公共 API、配置、使用示例和调用方契约。
- 本文记录模块职责、内部算法、并发模型和技术选型。
- 实施计划记录当前状态、待办和待决策，不作为长期 API 手册。
- 审计标准记录检查方法，审计报告记录特定 revision 的证据。
- 已淘汰方案不写入 README；如果理解当前设计必须知道取舍，只在本文简要说明原因。
- 行为变更必须同时检查 README 和本文是否需要更新。
- benchmark 数字必须注明日期、机器或链接到审计报告，不能表现成跨环境性能承诺。
