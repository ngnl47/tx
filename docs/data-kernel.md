# GD、Store 与变更内核

入口与模块关系见 [设计总览](design.md#数据元数据与变更追踪)，实际调用见 [API 参考](api-reference.md#变更追踪与同步遍历)。本篇说明生成器和运行时共同遵守的内部契约。共享对象的锁、快照与事务边界另见 [实体访问](runtime-access.md)。

包结构：`meta/` 保存类型契约，`change/` 保存路径与操作，`anno/`、`astscan/` 提供生成期解析基础；生成的 GD/Store 属于应用 `server/gen/data/`，不写入 tx。

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

### 物理布局与成本边界

生成 Store 用字段槽和集合 key/index 状态保存变更，同位置重复修改只保留一份状态；边界通过 `VisitChanges` 同步遍历，不构造中间批次。Redis/Mongo/Fusion 与 WriteBackManager 直接消费 `Source`，需要后端文档时才分配字符串路径和 map。

`Path` 按值传递仍有固定复制成本。优化须同时测量路径大小、访问频率、key 类型完整性及文档构建，不为零分配提前引入 unsafe 或自研值联合类型。`Path.FieldNameAt` 允许边界只读取所需字段，完整路径校验在 I/O/文档边界执行。

GD 的运行状态按需保存在 sidecar；首次独立根写入可能增加一次分配，不能只用大对象批量场景衡量收益。Store 子路径按实际访问建立，脏索引只遍历发生变化的节点；不同 key 持续增加仍会消耗内存，由写回预算及应用反压控制，setter 不执行隐式 I/O。

性能回归使用 txgen 的 `BenchmarkGeneratedStoreDirtyRuntime` 与 `BenchmarkGeneratedStoreLeafChanges1024`；当前文档不保留旧物理布局对照或生成源码体积阈值。

### StoreView 生成状态树

StoreView 不使用通用完整 `Path` 集合。txgen 已知每个 GD 类型的字段 ID、集合 key/index 类型和生成对象嵌套关系，因此为每个类型生成私有状态树。例如 `GDRole` 的普通字段操作保存在定长 `ops` 中，`Items` 使用 `map[int32]change.Op` 保存元素整体操作，并使用 `map[int32]*viewStateGDItem` 按需保存叶子字段状态。状态树的结构与 Go 类型一致；只有真正发生变更的集合 key 和子对象才分配节点。

`Record`、`Contains`、`Capture` 和 `Merge` 都直接访问这些强类型槽位。同位置后写覆盖、父路径吸收和 Clear/Delete 后子写提升在生成状态树内完成；`VisitChanges` 才递归组合定长 `change.Path` 并交给输出边界。整数 map key 从公开 `PathKey` 转回实际 Go key 前必须做无损范围校验，例如 `math.MaxInt64` 不能截断成 `int32(-1)` 后误命中已有状态或业务值。

集合状态、字段 undo、加载位图和读取装配由生成包基础层复用，类型专属 record/copy/visit 路径保持静态展开。保留捕获、合并和文档构建基准，不以生成源码行数或字节数限制实现。

