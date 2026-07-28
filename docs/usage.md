# tx 使用手册

本文记录 `github.com/ngnl47/tx` 当前已实现并验证的运行时能力。手册会随实现持续维护；仍未决策的行为会明确标注。

## 定位

`tx` 是运行时基础库，负责：

- 解析和保存类型元数据。
- 追踪字段级变更。
- 将 `ChangeSet` 落地到 Redis、Mongo 或 Fusion 策略。
- 提供 Redis 延迟任务 backend 和 Fusion 后台回写所需组件。

生成代码依赖 `tx`，`tx` 不依赖生成器工程。

## 类型注解

type 级注解支持 marker 和带参数两种形态。无参数时，下面两种写法等价：

```go
// @genData
// @genData:``
```

带参数时，参数统一写在反引号内。单值写法会默认映射到 `value` 参数：

```go
// @comment:`"角色"`
// @comment:`""`
```

上面两行会分别解析为 `Params["value"] = "角色"` 和 `Params["value"] = ""`。多参数写法使用 Go struct tag 语法：

```go
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
// @genEnum:`gcgc:"true" gcvo:"true"`
```

解析后会统一得到 `anno.Directive`。marker 注解的 `Params` 和 `Flags` 为空；带参数注解的键值会进入 `Params`。

### field tag 解析规则

field 级注解来自 Go struct tag，外层遵循 Go 原生 struct tag 规则：

```go
Items map[int32]*GDItem `comment:"拥有物品" json:"items,omitempty" redis:"subKey:items"`
```

- 外层 tag 项之间用空格分隔。
- 每项是 `key:"value"` 形式。
- value 必须是双引号包起来的字符串。
- `anno.ParseStructTag` 会先通过 Go 标准库读取已注册注解名对应的 value，再解释该 value。

内置 storage 相关 field tag 的 value 使用紧凑语法：

```go
redis:"subKey:items,fullUpdate"
mongodb:"index,unique,expireAfter:0"
```

- 逗号 `,` 分隔多个片段。
- `key:value` 表示参数，进入 `Params[key] = value`。
- 没有冒号的片段表示 flag，进入 `Flags[flag] = true`。
- 片段两边空格会被裁剪，空片段会跳过。
- field tag 的单值不会自动映射到 `Params["value"]`；type 注解的单值规则只作用于 type 级注解。

字段名类 tag 不属于内置注解解析结果，由 AST 扫描直接读取。Go 字段名是框架内部字段身份；
`json`、`bson`、`msgpack` 分别决定对应后端的物理字段名。`alias` 只是输入侧语法糖，如果写了
`alias:"rid"`，会同时作为 JSON、BSON、Msgpack、Redis 的默认字段名；写了 `alias` 的字段不能再写
`json`、`bson` 或 `msgpack`。不写 `alias` 时，各后端 tag 可以各自命名，例如
`json:"roleId,omitempty" bson:"_id,omitempty" msgpack:"roleId,omitempty"`。

### type 注解字段表

| 注解 | 字段 | 必填 | 取值 | 说明 |
| --- | --- | --- | --- | --- |
| `@storage` | `mode` | 是 | `redis`、`mongo`、`fusion` | 选择存储模式。`redis` 只写 Redis；`mongo` 只写 Mongo；`fusion` 使用 Redis 热数据加 Mongo 延迟回写。 |
| `@storage` | `delaySaveTime` | fusion 建议必填 | 秒数字符串或 Go duration | Redis 热数据变更后，经过多久进入 Mongo 回写。别名：`saveTime`、`delaySave`。 |
| `@storage` | `delayDelTime` | Redis/Fusion 热数据建议必填 | 秒数字符串、Go duration 或 `-1` | Redis 热数据 TTL。写入后会续期；不表示 Mongo 删除延迟。别名：`delTime`、`delayDelete`。 |
| `@storage` | `memoryCache` | 否 | 秒数字符串或 Go duration | 预留本地内存缓存时间配置，当前 runtime 不把它作为核心路径。 |
| `@redis` | `routeType` | 是 | 字符串 | 路由配置名，用来在 yml 路由表中选择 Redis endpoint。别名：`shardType`、`config`。 |
| `@redis` | `routeBy` | 是 | Go 字段名 | 路由字段，决定 Redis endpoint 与 Redis Cluster hash tag。别名：`shardBy`。 |
| `@redis` | `idField` | 是 | Go 字段名 | 业务记录主键字段，用于生成记录 key。别名：`id`。 |
| `@redis` | `key` | 是 | 字符串 | Redis key 前缀，例如 `role`，最终 key 类似 `role:{1001}`。 |
| `@redis` | `storeType` | 否 | 字符串 | 预留存储类型标记。 |
| `@redis` | `omitempty` | 否 | flag 或布尔字符串 | 空文档写入策略标记。 |
| `@mongodb` | `routeType` | 是 | 字符串 | 路由配置名，用来在 yml 路由表中选择 Mongo endpoint。别名：`shardType`、`config`。 |
| `@mongodb` | `routeBy` | 是 | Go 字段名 | 路由字段，决定 Mongo endpoint。别名：`shardBy`。 |
| `@mongodb` | `idField` | 是 | Go 字段名 | 业务记录主键字段；runtime 会通过该字段的 `bson` tag 推导 Mongo filter 字段。别名：`id`。 |
| `@mongodb` | `collection` | 是 | 字符串 | Mongo collection 名称。别名：`c`、`col`。 |
| `@mongodb` | `enableSharding` | 否 | flag 或布尔字符串 | 标记该 collection 开启分片语义；当前主要进入元数据。 |

常见 type 注解组合：

```go
// 只存 Redis，适合纯热数据或可丢弃缓存。
// @storage:`mode:"redis" delayDelTime:"2592000"`
// @redis:`routeType:"war" routeBy:"RoleId" idField:"ID" key:"legion_war_record"`
type GDLegionWarRecord struct {
	ID     int64 `json:"id" bson:"id" msgpack:"id"`
	RoleId int64 `json:"roleId" bson:"roleId" msgpack:"roleId"`
}
```

```go
// 只存 Mongo，适合不需要 Redis 热数据的记录。
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"friend" routeBy:"RoleId" idField:"RoleId" collection:"friend_records"`
type GDFriendRecord struct {
	RoleId int64 `json:"roleId" bson:"roleId" msgpack:"roleId"`
	Name   string `json:"name" bson:"name" msgpack:"name"`
}
```

```go
// Fusion：Redis 保存热数据，Mongo 延迟回写。
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
// @redis:`routeType:"role" routeBy:"RoleId" idField:"RoleId" key:"role"`
// @mongodb:`routeType:"role" routeBy:"RoleId" idField:"RoleId" collection:"roles"`
type GDRole struct {
	RoleId int64  `json:"roleId" bson:"_id" msgpack:"roleId"`
	Name   string `json:"name" bson:"name" msgpack:"name"`
}
```

### field tag 字段表

| tag | 字段或 flag | 取值 | 说明 |
| --- | --- | --- | --- |
| `comment` | 单值 | 任意字符串 | 字段说明，只作为元数据和生成代码注释使用。 |
| `json` | 字段名 | 字符串或 `-` | JSON 字段名。`-` 表示 JSON 忽略该字段。 |
| `json` | 选项 | `omitempty`、`string` 等 Go JSON 选项 | 必须写在字段名之后，例如 `json:"name,omitempty"`。`json:",omitempty"` 属于歧义写法，生成器会拒绝。 |
| `bson` | 字段名 | 字符串、`_id` 或 `-` | BSON 字段名。`_id` 表示绑定 Mongo 文档主键；`-` 表示 BSON 忽略该字段。 |
| `bson` | 选项 | Mongo Go Driver 支持的选项 | 例如 `omitempty`、`inline`。匿名嵌入结构在 JSON 下默认内联，BSON/Msgpack 如果需要内联需显式配置。 |
| `msgpack` | 字段名 | 字符串或 `-` | Msgpack 字段名，cmd Provider 下行变更文档使用该名称。 |
| `msgpack` | 选项 | Msgpack 编码器支持的选项 | 例如 `omitempty`、`inline`。 |
| `alias` | 单值 | 字符串 | 输入侧语法糖：同时作为 JSON、BSON、Msgpack、Redis 的默认字段名。不能和 `json`、`bson`、`msgpack` 混用。 |
| `redis` | `subKey` | 字符串 | 将该字段从 Redis 主文档拆到独立 key，例如 `role:{1001}:items`。 |
| `redis` | `ignore` | flag 或布尔字符串 | Redis 文档忽略该字段。 |
| `redis` | `fullUpdate` | flag 或布尔字符串 | 标记该字段按整字段更新，适合集合字段避免 key 级 path 膨胀。 |
| `mongodb` | `index` | flag 或布尔字符串 | 为该字段生成 Mongo 索引元数据。 |
| `mongodb` | `unique` | flag 或布尔字符串 | 索引唯一约束，通常和 `index` 一起使用。 |
| `mongodb` | `ignore` | flag 或布尔字符串 | Mongo 文档忽略该字段。 |
| `mongodb` | `order` | `asc`、`desc` 或数字字符串 | 索引方向。 |
| `mongodb` | `indexType` | 字符串 | 索引类型，例如文本索引等。 |
| `mongodb` | `weight` | 数字字符串 | 文本索引权重。 |
| `mongodb` | `expireAfter` | 秒数字符串 | Mongo TTL 索引过期时间；命中后删除整条文档，不是删除单个字段。别名：`expire`。 |

常见 field tag 组合：

```go
// 普通字段：三个后端字段名一致。
Name string `comment:"名字" json:"name,omitempty" bson:"name,omitempty" msgpack:"name,omitempty"`
```

```go
// 业务主键直接绑定 Mongo _id。
RoleId int64 `comment:"角色ID" json:"roleId" bson:"_id" msgpack:"roleId"`
```

```go
// Redis 大字段拆分 subKey，避免普通字段变更时重写大集合。
Items map[int32]*GDItem `comment:"物品" json:"items,omitempty" bson:"items,omitempty" msgpack:"items,omitempty" redis:"subKey:items,fullUpdate"`
```

```go
// Mongo 索引字段。
Name string `comment:"名字" json:"name" bson:"name" msgpack:"name" mongodb:"index"`
```

```go
// Mongo TTL 字段。注意：TTL 到期删除整条 Mongo 文档。
ExpireAt int64 `comment:"过期时间" json:"expireAt" bson:"expireAt" msgpack:"expireAt" mongodb:"index,expireAfter:0"`
```

```go
// 输入侧 alias 语法糖，等价于同时使用 roleId 作为 json/bson/msgpack 字段名。
RoleId int64 `comment:"角色ID" alias:"roleId"`
```

非法或不推荐写法：

```go
// 非法：alias 不能和 json/bson/msgpack 混用。
RoleId int64 `alias:"rid" json:"roleId"`

// 非法：json 选项前必须显式写字段名。
Name string `json:",omitempty"`

// 非法：json 选项列表不能有空片段。
Name string `json:"name,omitempty,"`

// 非法：Store 类型是框架运行时类型，不能作为输入结构字段。
Role *GDRoleStore `json:"role"`
```

`@mongodb.idField` 使用 Go 字段名，tx runtime 会通过该字段的 `bson` tag 得到 Mongo 过滤字段。
业务 `idField` 可以直接绑定到 Mongo `_id`，也可以作为普通 BSON 字段保存。

以 `RoleId = 1001`、`Name = "alice"` 为例，常见组合如下：

| 配置组合 | Mongo 文档结构 | runtime 过滤条件 | 说明 |
| --- | --- | --- | --- |
| `idField:"RoleId"`，字段写 `bson:"_id"` | `{"_id": 1001, "name": "alice"}` | `{_id: 1001}` | Mongo 物理主键直接使用业务 ID。 |
| `idField:"RoleId"`，字段写 `bson:"roleId"` 或缺省生成 `bson:"roleId"` | `{"_id": ObjectID, "roleId": 1001, "name": "alice"}` | `{roleId: 1001}` | Mongo `_id` 和业务 ID 分离，runtime 按业务字段查。 |

因此，`bson:"_id"` 是 Mongo 序列化层的字段绑定；`mongodb.idField` 是 tx 存储框架层的业务记录身份。
两者配合后由 runtime 统一推导过滤字段，不再需要额外开关。

txgen 会在生成阶段校验 `bson:"_id"` 的归属：同一 data 类型最多只能有一个字段声明
`bson:"_id"`；如果存在，该字段必须是 `mongodb.idField` 指向的 Go 字段。`routeBy` 只负责
路由取值，不决定 Mongo 物理主键字段。

## 日志

`tx/log` 基于标准库 `log/slog` 提供最小 logger 注入入口：

```go
func SetDefault(logger *slog.Logger)
func Default() *slog.Logger

func Debug(msg string, args ...any)
func Info(msg string, args ...any)
func Warn(msg string, args ...any)
func Error(msg string, args ...any)

func DebugContext(ctx context.Context, msg string, args ...any)
func InfoContext(ctx context.Context, msg string, args ...any)
func WarnContext(ctx context.Context, msg string, args ...any)
func ErrorContext(ctx context.Context, msg string, args ...any)

func NewLogger(name string) Logger

func (l Logger) Debug(msg string, args ...any)
func (l Logger) Info(msg string, args ...any)
func (l Logger) Warn(msg string, args ...any)
func (l Logger) Error(msg string, args ...any)

func (l Logger) DebugContext(ctx context.Context, msg string, args ...any)
func (l Logger) InfoContext(ctx context.Context, msg string, args ...any)
func (l Logger) WarnContext(ctx context.Context, msg string, args ...any)
func (l Logger) ErrorContext(ctx context.Context, msg string, args ...any)
```

默认返回 `slog.Default()`；传入 `nil` 会恢复默认 logger。实际项目可在启动期调用 `log.SetDefault(logger)` 注入自己的 `slog.Logger`，并通过 `slog.Handler` 对接文件日志、云日志或第三方日志实现。

默认调用方式：

```go
log.Error(
    "delay task failed, retry scheduled",
    "taskType", task.Type,
    "taskID", task.ID,
    "action", task.Action,
    "attempts", attempts,
    "err", err,
)
```

需要按功能派生日志时，可以在项目中显式定义包级变量：

```go
var RoleLog = log.NewLogger("role")

RoleLog.Error("role save failed", "roleId", roleID, "err", err)
```

`NewLogger` 返回轻量代理，不是固定绑定的 `*slog.Logger`。代理值本身可以作为包级变量复用；
每次输出时都会读取当前 `Default()`，再附加 `logger=<name>` 属性派生一次日志调用，避免包级变量早于项目日志初始化时绑定到旧 logger。
因此，项目启动后调用 `log.SetDefault(logger)`，此前已经声明的 `RoleLog` 也会使用新的 default logger。

## 元数据与字段名

`meta.TypeSpec` 使用 Go 字段名作为内部字段身份；`TypeSpec.Fields` 的 key、`idField`、`routeBy`、
`LoadFields()` 入参和 `ChangeDocument()` 默认输出都使用 Go 字段名。`meta.Field` 同时保存
`JSONName`、`BSONName`、`MsgpackName`、`RedisName`，运行时适配器在靠近具体后端时再转换字段名。

例如：

```go
RoleId int64 `json:"roleId,omitempty" bson:"_id,omitempty" msgpack:"roleId,omitempty"`
```

Store 内部变更文档使用 `RoleId`；Redis 写入时使用 `roleId`；Mongo filter/update 使用 `_id`；
cmd Provider 下行 body 使用 Msgpack 名 `roleId`。

## ChangeSet

`ChangeSet` 表示一次保存周期内“哪些字段需要被落地或下发”。业务代码通常不直接操作
`change.Tracker`，而是通过生成的 `GD*` 代理方法或 `GD*Store` 方法标脏。

### 常用业务写法

最常见的写法是直接操作数据对象。`GD*` 会懒加载自己的 Store，并代理到 Store setter：

```go
role := data.NewGDRole()
role.SetRoleId(1001)
role.SetName("alice")
role.SetPower(470001)

doc := role.ChangeDocument()
```

这时 `ChangeDocument()` 使用 Go 字段名输出后端无关文档：

```go
change.Document{
	Set: map[string]any{
		"RoleId": int64(1001),
		"Name":   "alice",
		"Power":  int64(470001),
	},
	Unset: map[string]struct{}{},
}
```

如果用于 cmd 下行，生成代码会走 Msgpack 字段名：

```go
body := role.MsgpackChangeDocument()
```

例如 `HeadImg` 的 tag 是 `msgpack:"hi,omitempty"`，那么下行变更文档里使用 `hi`，不是 Go 字段名
`HeadImg`。

### Set、Touch、Replace、Unset

生成 Store 每个字段通常会有四类方法：

| 方法 | 是否修改内存字段 | ChangeSet 语义 | 常见用途 |
| --- | --- | --- | --- |
| `SetName(value)` | 是 | set | 日常业务修改字段。 |
| `TouchName()` | 否 | set 当前内存值 | 字段值已经被业务直接改过，需要补标脏；或 viewStore 只想把当前字段加入下行。 |
| `ReplaceItems(value)` | 是 | replace | 整体替换 map/slice/struct 指针字段。 |
| `UnsetName()` | 是，设置为 Go 零值 | unset | 从 Redis/Mongo 文档中移除字段。 |

日常业务优先使用 `Set*`：

```go
role.SetName("bob")
role.SetPower(role.GetPower() + 100)
```

如果业务直接修改了字段，需要手动 `Touch*` 补标脏：

```go
role.Name = "bob"
role.TouchName()
```

如果只想把字段加入下行变更，但不想污染主 Store，可以创建 viewStore：

```go
view := role.NewViewStore()
view.TouchName()
view.TouchPower()

body := view.MsgpackChangeDocument()
```

这适合广播或响应中“额外携带一些未修改字段”的场景。主对象的落库 Store 不会因此多出脏字段。

### 零值和删除

`Set*` 设置零值表示“把字段写成零值”，不是删除字段：

```go
role.SetName("")
role.SetPower(0)
```

对应变更文档：

```go
change.Document{
	Set: map[string]any{
		"Name":  "",
		"Power": int64(0),
	},
}
```

落到 Redis/Mongo 后，字段仍然存在，只是值为 `""` 或 `0`。这和 `omitempty` 不冲突：`omitempty`
主要影响完整对象序列化时是否省略零值；`ChangeSet` 的 set 语义会明确保留这次设置的零值。

如果业务语义是“从持久化文档里删除这个字段”，使用 `Unset*`：

```go
role.UnsetName()
```

对应变更文档：

```go
change.Document{
	Set:   map[string]any{},
	Unset: map[string]struct{}{"Name": {}},
}
```

Redis 写入时会把 patch 中该字段设为 JSON null，由 Lua merge 删除字段；Mongo 写入时会转成 `$unset`。

### 集合字段和路径标脏

集合字段可以整字段标脏，也可以标记内部路径。第一版推荐业务上优先整字段标脏，降低编码心智负担：

```go
role.Store().UpdateItems(func(items map[int32]*data.GDItem) {
	item := items[1001]
	if item == nil {
		item = &data.GDItem{ItemId: 1001}
		items[1001] = item
	}
	item.Quantity += 3
})
```

`UpdateItems(fn)` 会在 map 为 nil 时先初始化 map，执行闭包后记录整字段 `Items`。后续 Redis 中如果
`Items` 配置了 `redis:"subKey:items"`，只会更新
`role:{1001}:items` 这个 subKey 文档，不会重写主文档。

也可以手动修改后补 `TouchItems()`：

```go
role.Items[1001].Quantity += 3
role.TouchItems()
```

路径级标脏适合框架内部或少数非常明确的局部更新场景：

```go
role.TouchItemsPath(change.Path{"1001", "quantity"}, int64(3))
```

对应路径是：

```text
Items.1001.quantity
```

如果手动使用路径级标脏，业务需要自己保证 map key 存在、值类型正确、路径和最终文档结构一致。否则代码看起来省 IO，
但很容易把复杂度转移到业务层。

### 覆盖规则

`ChangeSet` 同一目标多次写入时，只保留最后一次：

```go
role.SetName("alice")
role.SetName("bob")
```

最终只记录：

```go
Set["Name"] = "bob"
```

父路径会覆盖子路径，避免大集合 path 爆炸：

```go
role.TouchItemsPath(change.Path{"1001", "quantity"}, int64(3))
role.TouchItems()
```

最终只保留整字段：

```text
Items
```

反过来，如果已经标记了整字段，再追加子路径，子路径会被忽略：

```go
role.TouchItems()
role.TouchItemsPath(change.Path{"1001", "quantity"}, int64(3))
```

最终仍然只保留：

```text
Items
```

完整路径 API 也遵守相同规则：

```go
var tracker change.Tracker
_ = tracker.Touch(change.Path{"items", "1001", "quantity"}, int64(3))
_ = tracker.Touch(change.Path{"items"}, items)
```

最终只保留 `items`。

### AllTouch 和 ClearChanges

`AllTouch()` 表示整对象需要全量落地。它会清空已有字段级 `ChangeSet`：

```go
role.SetName("bob")
role.TouchItemsPath(change.Path{"1001", "quantity"}, int64(3))
role.Store().AllTouch()
```

执行后不再保留 `Name` 或 `Items.1001.quantity` 这些局部记录。后续保存路径应根据 `AllDirty()` 走整文档替换。

`ClearChanges()` 或 Store 的 `AllUnTouch()` 用于保存成功、加载完成或主动放弃本轮修改后清空脏标记：

```go
if err := role.Store().ApplyUpdate(ctx, writer); err != nil {
	return err
}
role.ClearChanges()
```

生成的加载方法在成功读取后会自动清空脏标记，避免“刚加载出来的数据”被误认为需要保存。

### ChangeSet 到后端文档

`change.Tracker` 底层记录的是：

- `KindSet`：设置字段值。
- `KindReplace`：整体替换字段值，常用于 map、slice、struct 指针。
- `KindUnset`：删除字段。
- `FieldID`：生成代码提供的稳定字段编号，避免热路径反复保存字段名字符串。
- `Path`：字段内部的动态尾路径，例如 map key 或嵌套字段名。

生成代码会传入 `FieldResolver`，把字段编号解析为字段名：

```text
FieldID(fieldGDRoleName) + Path{} -> Name
FieldID(fieldGDRoleItems) + Path{"1001", "quantity"} -> Items.1001.quantity
```

变更落地和下行投影不会对整个数据对象做全量 `json.Marshal`、`bson.Marshal` 或
`msgpack.Marshal`。`ChangeSet` 只遍历本保存周期内的脏字段，通过生成的 resolver 或
`TypeSpec` 转成目标后端字段名，再生成局部 patch/update document。若某个脏字段本身是
map、slice 或 struct 指针，具体字段值在最终编码时仍会交给对应 JSON/BSON/Msgpack 编码器展开。
完整对象读取、回源或整文档替换路径才会依赖序列化库按 tag 映射字段。

## Redis 存储模型

Redis 使用 string JSON 文档模型。

当前主线选择普通 Redis string 存储 JSON 文档，并通过 Lua 在写入时合并或替换文档。RedisJSON/ReJSON
可以作为后续可选 backend 单独评估，但不是当前默认路径；引入前需要确认目标云 Redis、测试环境和
Redis Cluster 行为都支持对应 JSON 命令。

主 key 保存没有 `subKey` 的字段：

```text
role:{470001} -> {"roleId":470001,"name":"alice","power":47}
```

带 `redis:"subKey:items"` 的字段保存到独立 key：

```text
role:{470001}:items -> {"items":{"1001":{"quantity":3}}}
```

Redis Cluster 下使用 hash tag 保证同一条记录的主文档和 subKey 位于同一 slot：

```text
role:{id}
role:{id}:items
```

当 `routeBy != idField` 且 `RecordKey.ShardKey` 存在时，hash tag 使用路由字段：

```text
arena:{roleId}:id
```

## RecordKey

`storage.RecordKey` 描述一条记录的存储定位：

```go
type RecordKey struct {
    TypeName string
    ID       any
    ShardKey any
}
```

存储定位只作用于当前被读写的根对象。一个带存储元数据的数据类型如果被嵌套在其他对象字段中，运行时会把它当作普通内联文档处理，不会因为子类型也可独立存储而自动生成或访问子记录。

- `ID` 来自 `idField`。
- `ShardKey` 来自 `routeBy`，仅在 `routeBy != idField` 时需要。
- 批量读取必须保证每条记录携带正确路由字段，才能构造正确 `RecordKey`、Redis endpoint 和 hash tag。

## RouteTable

`storage.RouteTable` 是 Redis 和 Mongo 共用的运行时路由表，可同时作为 `redis.Cluster` 和 `mongo.Cluster` 的 router 使用。

### 直接用代码构造

小型测试或没有配置文件时，可以直接写 Go 代码：

```go
router := storage.NewRouteTable(map[string][]storage.RouteRule{
    "role": {
        {Min: 1, Max: 999999, Shards: []string{"role-a", "role-b"}},
        {Min: 1000000, Shards: []string{"role-c"}},
    },
    "global": {
        {Shards: []string{"main"}},
    },
})
```

路由规则：

- 先按 `routeType` 找到规则列表。
- 再按 `shardKey` 命中 `min/max` 区间；`min <= 0` 视为 `1`，`max <= 0` 视为无限大。
- 多 shard 规则用 `routeBy % len(shards)` 选择 endpoint。
- 单 shard 规则允许字符串主键，用于全局表或单分片表。

以上配置表示：

| routeType | shardKey | endpoint |
| --- | --- | --- |
| `role` | `1001` | `role-b`，因为 `1001 % 2 = 1` |
| `role` | `1000000` | `role-c` |
| `global` | `"season"` | `main` |

### 从 yml 配置构造

`storage/config.RouteTableConfig` 用于承接项目 yml 解析后的 Redis/Mongo 路由配置。项目负责读取文件和处理 include，随后把 yml 反序列化到该类型：

```yaml
main: "redis://main"
role:
  - min: 1
    shards:
      - "redis://role-a"
      - "redis://role-b"
rank:
  - "redis://rank-a"
  - "redis://rank-b"
arena:
  - min: 1
    max: 999999
    shards:
      - "redis://arena-a"
      - "redis://arena-b"
  - min: 1000000
    shards:
      - "redis://arena-c"
```

支持的 routeType 值形态：

- 标量 endpoint：归一化为单 shard 规则，例如 `main: "redis://main"`。
- endpoint 数组：归一化为一个多 shard 规则，例如 `rank: ["redis://rank-a", "redis://rank-b"]`。
- 规则数组：保留 `min/max/shards` 区间规则，例如上面的 `arena`。

配置对象可直接构造运行时路由表：

```go
var redisRoutes config.RouteTableConfig
if err := yaml.Unmarshal(body, &redisRoutes); err != nil {
	return err
}
table, err := redisRoutes.RouteTable()
if err != nil {
	return err
}
endpoints := redisRoutes.Endpoints()
```

`Endpoints()` 返回配置中出现过的 endpoint URI 集合；当前 key 和 value 都是 URI，运行时装配可按 URI 创建并复用客户端。

tx 提供官方 driver 的薄适配层，业务工程仍负责读取本地配置、创建具体 client，再传给 tx runtime：

```go
redisClient := txgoredis.NewClient(goRedisClient)
mongoDB := txmongodriver.NewDatabase(mongoDriverDatabase)
```

对应 import 路径：

- `github.com/ngnl47/tx/storage/redis/goredis`
- `github.com/ngnl47/tx/storage/mongo/mongodriver`

## Redis Route/Cluster Adapter

`redis.Cluster` 按 `routeType` 和 `storage.RecordKey.ShardKey` 路由到具体 Redis adapter。

- 单条写入、替换、删除和读取会先选择 endpoint，再调用对应 adapter。
- 批量读取会先按 endpoint 分组，每个 endpoint 内仍使用客户端层原生 `MGET` 读取 main/subKey keys，然后按输入顺序还原结果。
- 默认策略贴合云 Redis 代理能力，不在 endpoint 内继续按 slot 拆分；本地原生 Redis Cluster 可显式打开 `LoadOptions.ClusterSafe` 调试模式。
- 写入和整文档替换直接接收 `storage.ApplyOptions`，不再暴露 Redis 专用 `ApplyOptions` 别名；`redis.LoadOptions` 仍是 Redis 批量读取的专用选项。

不同 Redis 环境下的批量读取策略：

- Redis 单节点：直接使用客户端层原生 `MGET`。
- 云 Redis 代理或云集群 endpoint：默认直接使用客户端层原生 `MGET`，由云代理负责跨 slot 分发和结果聚合。
- 本地原生 Redis Cluster 或直连 cluster 节点：显式设置 `LoadOptions.ClusterSafe: true` 后，运行时会按 hash tag 分组后多次 `MGET`；当前默认路径不做该拆分。

### 场景一：本地单节点 Redis

yml 可以只写一个 endpoint：

```yaml
role: "redis://127.0.0.1:6379/15"
global: "redis://127.0.0.1:6379/15"
```

初始化时把所有 endpoint 映射到同一个 go-redis client 即可：

```go
routeTable, err := redisRoutes.RouteTable()
if err != nil {
	return err
}

client := redis.NewClient(&redis.Options{
	Addr: "127.0.0.1:6379",
	DB:   15,
})

redisCluster := txredis.NewCluster(routeTable, map[string]any{
	"redis://127.0.0.1:6379/15": txgoredis.NewClient(client),
})
```

批量读取时不需要特殊选项：

```go
docs, exists, err := redisCluster.LoadDocuments(ctx, GDRoleTypeSpec(), keys)
```

### 场景二：云 Redis 代理或云集群 endpoint

生产环境通常会把一个 endpoint 暴露为云代理地址。yml 可以按业务 routeType 配置多个云 endpoint：

```yaml
role:
  - "redis://role-cache-a.example.com:6379"
  - "redis://role-cache-b.example.com:6379"
rank: "redis://rank-cache.example.com:6379"
```

初始化时每个 endpoint 创建一个 go-redis client，并交给 `redis.Cluster`：

```go
routeTable, err := redisRoutes.RouteTable()
if err != nil {
	return err
}

shards := map[string]any{}
for endpoint := range redisRoutes.Endpoints() {
	opt, err := redis.ParseURL(endpoint)
	if err != nil {
		return err
	}
	shards[endpoint] = txgoredis.NewClient(redis.NewClient(opt))
}

redisCluster := txredis.NewCluster(routeTable, shards)
```

默认批量读取会先按 endpoint 分组，再在每个 endpoint 内使用原生 `MGET`。云代理能识别 `MGET` 的参数都是 key，
可以在代理层完成跨 slot 分发和聚合。不要把跨记录批量读取包进自定义 Lua：云代理不能可靠判断 Lua 的
`ARGV` 中哪些值是 key，也不能安全拆分一个要求原子执行的脚本。

### 场景三：本地原生 Redis Cluster 或直连节点

本地 debug 如果直连原生 Redis Cluster 节点，普通跨 slot `MGET` 可能报错。此时仍使用同一份 yml，但批量读取时显式打开
`ClusterSafe`：

```go
docs, exists, err := redisCluster.LoadDocumentsWithOptions(
	ctx,
	GDRoleTypeSpec(),
	keys,
	txredis.LoadOptions{ClusterSafe: true},
)
```

`ClusterSafe` 会在 endpoint 内继续按 hash tag 分组，多次执行 `MGET` 后按输入顺序还原结果。它适合本地原生 Redis Cluster
和问题排查；生产云 Redis 默认不需要打开，避免不必要的客户端侧拆分。

### routeBy、idField 和 hash tag

`idField` 表示业务主键，`routeBy` 表示路由字段。两者相同时，`RecordKey.ID` 就足够生成 Redis key：

```text
// @redis:`routeType:"role" routeBy:"RoleId" idField:"RoleId" key:"role"`
// roleId = 1001
role:{1001}
role:{1001}:items
```

两者不同时，生成代码需要把 `routeBy` 字段也放进 `RecordKey.ShardKey`，让主文档和 subKey 使用同一个 hash tag：

```text
// @redis:`routeType:"arena" routeBy:"RoleId" idField:"ID" key:"arena"`
// id = 90001, roleId = 1001
arena:{1001}:90001
arena:{1001}:90001:members
```

这样同一条记录的 main key、subKey key 和版本 key 能落到同一个 Redis Cluster slot；批量读取多条记录时，不同记录仍可能落到不同 slot。

## Mongo 存储模型

Mongo adapter 当前支持：

- `ApplyChangesWithOptions`：将 `ChangeSet` 转成 Mongo update 文档。
- `LoadDocument`：按记录键读取单条文档，可带 projection 字段。
- `LoadDocuments`：按记录键批量读取并保持返回顺序。
- `ReplaceDocument`：整文档替换，供 Fusion 延迟保存回写使用。
- `ApplyDelete`：显式物理删除接口，由业务代码决定是否调用。
- `mongo.Cluster`：按 `routeType` 和 `storage.RecordKey.ShardKey` 路由到具体 Mongo adapter；批量读取会按 shard 分组并按输入顺序还原结果。

Mongo adapter 通过统一 filter 抽象调用底层 client：默认 filter 是 `{_id: id}`；配置
`idField` 时 filter 字段由 `spec.BSONName(idField)` 推导。Mongo update 会遵守 `storage.ApplyOptions.Upsert`：
生成 Store 的 Insert/Replace 路径会启用 upsert，Update 路径会关闭 upsert，避免普通更新意外创建缺失文档。

Mongo TTL index 的字段级 `expireAfter` 会删除整条文档，不是删除单个字段。生成阶段应限制同一类型最多一个 TTL 字段。

## 通用文档接口

`tx/storage` 提供 Redis、Mongo、Fusion 和生成代码共用的文档接口：

- `DocumentLoader`：按 `TypeSpec + RecordKey` 读取单条文档。
- `BatchDocumentLoader`：按 `TypeSpec + []RecordKey` 批量读取文档。
- `DocumentReplacer`：按 `TypeSpec + RecordKey` 整文档替换。
- `DocumentReplacerWithOptions`：整文档替换并接收 `storage.ApplyOptions`，用于需要 TTL/upsert 等写入语义的路径。

这些接口是 Store 读取、Fusion 回源、Redis 热数据回填和延迟保存回写的共同边界。具体后端仍负责把
`RecordKey` 转成自己的底层定位方式，例如 Redis key/hash tag 或 Mongo filter。

## Fusion 流程

Fusion 策略的当前保存路径：

1. 业务变更生成 `ChangeSet`。
2. Redis 同步写入热数据。
3. Redis delayed task backend 写入延迟保存任务。
4. Scheduler 到期 claim 任务。
5. StorageTaskHandler 根据任务定位重新读取 Redis 热文档。
6. Handler 清理 nil 删除标记后 replace Mongo 文档。
7. 任务成功后通过 Lua complete 删除该任务在 Redis 队列中的状态。

延迟任务队列使用 bucket hash tag，支持多个服务实例并发消费。语义是 at-least-once，Mongo 回写必须保持存储幂等。
任务失败重试时，Scheduler 会输出结构化错误日志；失败原因不写入 Redis 任务状态，避免错误字符串占用队列内存。
Scheduler 循环中的 claim、complete、retry 等 backend 错误也会输出结构化错误日志；服务关闭导致的 context 结束不会额外记录错误。
Fusion delayed task queue 和 handler 都只接受保存 action；delete 或其他未知 action 会返回错误。入队时必须带有明确的类型和记录 ID，避免无效任务进入 Redis 队列；handler 消费端也会拒绝缺少记录 ID 的任务，防止手工写入或遗留异常任务落到存储层。任务类型优先来自 `DelayedOperation.Type`，为空时使用 `DelayedOperation.Key.TypeName`。Fusion 延迟保存按类型、记录 ID、route/shard key 和 action 生成一条合并任务，不暴露额外 suffix，避免同一记录产生多条重复保存任务。延迟保存任务不携带 ChangeSet，回写时重新读取 Redis 当前热数据。删除必须走显式删除接口，不进入延迟队列。
Scheduler 的 `Start`、`RunOnce` 和 `Schedule` 在自身或 backend 未配置时都是 no-op，便于本地测试或按环境关闭后台回写组件；生产环境仍应配置 Redis delayed task backend。

生产环境的延迟回写 handler 只负责存储同步。由于任务可能因为网络抖动、进程重启或租约恢复被再次执行，Mongo 回写路径应按“同一记录的当前 Redis 热文档 replace 到 Mongo”设计为存储幂等操作。比如同一条 `GDRole` save 任务执行两次，结果应只是 Mongo 中 `roleId=1001` 的文档被相同或更新后的热数据覆盖两次；队列中不保存 `GDRole` 的中间态快照，也不按每次字段变更生成事件日志。

运维侧应关注 Scheduler 错误日志、重试次数和队列积压时间。如果某类任务持续失败，优先排查对应类型的 Redis 热文档是否能正常反序列化、Mongo 路由/过滤条件是否正确、以及目标集合是否可写。队列中的失败原因不会写入 Redis 状态；需要更完整的告警和指标时，应通过项目自己的日志、监控或后续可插拔 observer 接入。

Fusion 读取会先查 Redis，未命中时回源 Mongo。默认回源会从 Mongo 全量读取文档，写回 Redis 热数据并使用 `delayDelTime` 续期，然后按请求字段返回结果；该路径要求 `delayDelTime` 必须大于 0，避免生成没有 TTL 的 Redis 热数据。显式使用 `LoadOptions.DontLoadToRedis` 时，才直接从 Mongo 按请求字段读取且不回填 Redis，适合低频只读场景。

部分字段读取的优化主要发生在 Redis 热数据层：运行时按 main/subKey key 粒度减少 Redis 读取，随后按字段过滤返回文档；Redis 和 Mongo 中保存的仍是全量数据。只请求 subKey 字段时，Redis 可以只读取对应 subKey 文档，不要求主文档同时命中。

Fusion 显式删除不会进入延迟队列。删除路径应先删除 Redis 热数据，再立即删除 Mongo 文档；如果只绕过 Fusion 删除 Mongo，而 Redis 热数据和延迟保存任务仍存在，后续保存任务可能按 Redis 当前数据重新写回 Mongo。

## TTL 与删除

`delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写。Fusion 延迟保存队列使用这个时间。

`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据；写入后会续期。显式传入 `ApplyOptions.TTL` 时，以显式值为准。Fusion 默认 Mongo 回源并回填 Redis 时必须配置该值。

`delayDelTime` 不代表 Mongo 删除延迟，也不会触发 Mongo 删除。业务是否执行 Mongo 物理删除，仍由显式 delete 接口和业务代码决定。

## 测试

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./... -count=1
```
