# tx 使用手册

本文记录 `github.com/ngnl47/tx` 当前已实现并验证的运行时能力。手册会随实现持续维护；仍未决策的行为会明确标注。

## 定位

`tx` 是运行时基础库，负责：

- 解析和保存类型元数据。
- 追踪字段级变更。
- 将 `ChangeSet` 落地到 Redis、Mongo 或 Fusion 策略。
- 提供 Redis 延迟任务 backend 和 Fusion 后台回写所需组件。

生成代码依赖 `tx`，`tx` 不依赖生成器工程。

## 快速接入流程

业务工程通常按下面顺序接入运行时：

1. 由生成工程输出 `server/gen/data` 和 `server/gen/cmd`。
2. 业务服务读取本地 yml，解析 Redis/Mongo routeType 路由表。
3. 业务服务创建 go-redis、MongoDB driver client，并用 tx 的薄适配层包装。
4. 用 Redis/Mongo cluster adapter 组装 Fusion strategy。
5. 将 Fusion strategy 注册给生成包的默认 reader。
6. handler 选择一条持久化主线：`BeginChange/SaveChange` 用于直接字段修改，`Store/SaveStore` 用于 setter/Store 拦截式标脏。
7. 后台 Scheduler 消费 Redis 延迟任务，把 Redis 热数据回写 Mongo。

一个简化的启动伪代码如下：

```go
func BootstrapStorage(ctx context.Context, cfg AppConfig) (*fusion.Strategy, error) {
	redisCluster, err := buildRedisCluster(ctx, cfg.Storage.Redis)
	if err != nil {
		return nil, err
	}
	mongoCluster, err := buildMongoCluster(ctx, cfg.Storage.MongoDB)
	if err != nil {
		return nil, err
	}
	delayQueue, err := buildRedisDelayQueue(ctx, cfg.Storage.Redis)
	if err != nil {
		return nil, err
	}

	store := &fusion.Strategy{
		Redis:         redisCluster,
		RedisLoader:   redisCluster,
		RedisDeleter:  redisCluster,
		RedisReplacer: redisCluster,
		MongoLoader:   mongoCluster,
		MongoDeleter:  mongoCluster,
		MongoQueue:    delayQueue,
	}

	data.RegisterStorageRuntime(runtimeReaders{
		redis:  redisCluster,
		mongo:  mongoCluster,
		fusion: store,
	})
	return store, nil
}
```

`runtimeReaders` 是业务工程自己的小适配结构，只需要满足生成包里的 `StorageRuntime` 接口：

```go
type runtimeReaders struct {
	redis  storage.DocumentLoader
	mongo  storage.DocumentLoader
	fusion storage.DocumentLoader
}

func (r runtimeReaders) RedisReader() data.DefaultReader  { return r.redis }
func (r runtimeReaders) MongoReader() data.DefaultReader  { return r.mongo }
func (r runtimeReaders) FusionReader() data.DefaultReader { return r.fusion }
```

如果 reader 同时实现 `storage.BatchDocumentLoader`，生成包会把它注册成默认批量 reader。Redis/Mongo/Fusion 的 cluster adapter 均实现批量读取接口。

handler 中常见写法：

```go
func RenameRole(ctx context.Context, store *fusion.Strategy, roleID int64, name string) error {
	role, exists, err := data.FindGDRoleByID(ctx, roleID)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrNotExists
	}

	role.SetName(name)
	if err := role.SaveStore(ctx, store); err != nil {
		return err
	}
	return nil
}
```

读取走默认 reader，写入仍显式传入 runtime。这样业务代码能清楚看到哪些操作会落地，避免“修改内存对象后自动保存”的隐式副作用。
上例使用 `SaveStore`，表示保存数据对象当前主 Store 中记录的变更，并在保存成功后清空本轮脏标记。
如果业务需要显式持有 Store，例如高频对象在内存中维护变更队列、定时合并后再落地，也可以直接使用
`role.Store().ApplyUpdate(ctx, store)` 这类 Store 写入入口，并由调用方决定何时 `ClearChanges()` 或重新累计下一轮变更。

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
生成代码会额外提供两类名字入口：`GDRoleGoFieldName` 这类常量表示 Go 字段名，供部分读取和
业务字段选择使用；`GDRoleFieldName(field, backend)` 这类函数按 `go/json/bson/msgpack` 返回
字段名本体。Redis、Mongo、Provider 是使用方策略：Redis 当前选择 JSON 名，Mongo 当前选择
BSON 名，Provider 下行当前选择 Msgpack 名。

例如：

```go
RoleId int64 `json:"roleId,omitempty" bson:"_id,omitempty" msgpack:"roleId,omitempty"`
```

Store 内部变更文档使用 `RoleId`；完整 JSON 序列化使用 `roleId`；Redis 写入当前也选择 JSON
名 `roleId`；Mongo filter/update 选择 BSON 名 `_id`；cmd Provider 下行 body 选择 Msgpack 名
`roleId`。

示例：

```go
field := data.NewGDRoleStore(nil).FieldIndex(data.GDRoleGoFieldRoleId)

data.GDRoleFieldName(field, change.NameBackendGo)      // "RoleId"
data.GDRoleFieldName(field, change.NameBackendJSON)    // "roleId"
data.GDRoleFieldName(field, change.NameBackendBSON)    // "_id"
data.GDRoleFieldName(field, change.NameBackendMsgpack) // "roleId"
```

## ChangeSet

`ChangeSet` 表示一次保存周期内“哪些字段需要被落地或下发”。生成 Store 当前处于迁移期，
会同步记录旧 `ChangeSet` 和新 `DeltaSet`。业务代码通常不直接操作 `change.Tracker`，而是通过
生成的 `GD*` 代理方法或 `GD*Store` 方法标脏。

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
	Clear: map[string]struct{}{},
}
```

如果用于 cmd 下行，生成代码会把 Store 里的 `DeltaSet` 转成 Msgpack 字段名文档：

```go
body := role.MsgpackChangeDocument()
```

例如 `HeadImg` 的 tag 是 `msgpack:"hi,omitempty"`，那么下行变更文档里使用 `hi`，不是 Go 字段名
`HeadImg`。

`Deltas()` 返回压缩后的 `change.DeltaSet`；如果传入的存储适配器实现了 `storage.DeltaApplier`，
生成的 `ApplyInsert`、`ApplyUpdate`、`ApplyReplace` 会优先走 Delta 直连入口。旧 `ChangeSet`
仍作为未迁移适配器的兜底路径。

```go
role := data.NewGDRole()
role.SetName("alice")
role.SetName("bob")

deltas := role.Store().Deltas()
// 只保留最后一次 Name = "bob" 的 DeltaSet 记录。
```

`DeltaSet` 也可以在边界转成 `change.Document`。转换会先复制再压缩，不会污染 Store 里原始的
变更队列：

```go
doc := role.Store().Deltas().DocumentWithResolver(
	change.FieldNameResolverFunc(data.GDRoleFieldName),
	change.NameBackendMsgpack,
	role.Store(),
)
// 如果 HeadImg 的 msgpack 字段名是 hi，则 doc.Set["hi"] 是当前头像值。
```

### Set、Touch、Replace、Clear

生成 Store 每个字段通常会有四类方法：

| 方法 | 是否修改内存字段 | ChangeSet 语义 | 常见用途 |
| --- | --- | --- | --- |
| `SetName(value)` | 是 | set | 日常业务修改字段。 |
| `TouchName()` | 否 | set 当前内存值 | 字段值已经被业务直接改过，需要补标脏；或 viewStore 只想把当前字段加入下行。 |
| `ReplaceItems(value)` | 是 | replace | 整体替换 map/slice/struct 指针字段。 |
| `ClearName()` | 是，设置为 Go 零值 | clear | 从 Redis/Mongo 文档中移除字段。 |

在 Delta 路径里，普通字段清零和集合清空会被区分：

```go
role.ClearHeadImg()
// 普通字段：DeltaSet 只记录 HeadImg 需要写入；边界从 Store 当前值读取到 Go 零值。

role.TasksStore().Clear()
// map/slice：DeltaClear Tasks，表示容器仍存在，但内容为空。
```

如果业务确实需要删除 map key 或 Mongo 字段，后续集合 Store 会使用 `DeltaDelete` 表达，例如
`Items[1001]` 被删除时，对 Mongo 边界可转换为 `$unset: {"items.1001": ""}`。

日常业务优先使用 `Set*`：

```go
role.SetName("bob")
role.SetPower(role.GetPower() + 100)
```

生成代码同时支持两条持久化主线。休闲游戏的无状态请求可以使用快照 diff：

```go
if err := role.BeginChange(); err != nil {
	return err
}

role.Name = "bob"
role.Items[1001].Quantity = 7

if err := role.SaveChange(ctx, fusionDB); err != nil {
	return err
}
```

高频有状态对象优先使用 Store 拦截式标脏：

```go
role.SetName("bob")
role.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 7})

view := role.Store().View() // 保存前复制一份下行视图

if err := role.SaveStore(ctx, fusionDB); err != nil {
	return err
}
```

`BeginChange/SaveChange` 和 `Store/SaveStore` 在同一个变更周期内互斥；`NewViewStore()` 与 `store.View()` 只用于下行/广播视图，不参与持久化模式。

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

如果业务语义是“清空字段并从持久化文档里移除这个字段”，使用 `Clear*`：

```go
role.ClearName()
```

对应变更文档：

```go
change.Document{
	Set:   map[string]any{},
	Clear: map[string]struct{}{"Name": {}},
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

`ClearChanges()` 或 Store 的 `AllUnTouch()` 用于加载完成、主动放弃本轮修改，或显式 Store 写入成功后手动清空脏标记。
普通请求链路可以使用 `SaveStore` 自动清理：

```go
if err := role.SaveStore(ctx, fusionStore); err != nil {
	return err
}
```

高频对象也可以显式持有 Store，把多次修改合并到同一个 tracker，定时或达到阈值后写入：

```go
store := role.Store()
store.IncPower(10)
store.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 7})

if err := store.ApplyUpdate(ctx, fusionStore); err != nil {
	return err
}
role.ClearChanges()
```

这种写法适合 SLG/MMO 战斗、沙盘状态等有状态高频对象；关键是由业务循环明确控制“累计多久、何时保存、保存成功后何时清理”。

生成的加载方法在成功读取后会自动清空脏标记，避免“刚加载出来的数据”被误认为需要保存。

### ChangeSet 到后端文档

`change.Tracker` 底层记录的是：

- `KindSet`：设置字段值。
- `KindReplace`：整体替换字段值，常用于 map、slice、struct 指针。
- `KindClear`：清空字段，存储适配器会映射为字段移除语义。
- `FieldID`：生成代码提供的稳定字段编号，避免热路径反复保存字段名字符串。
- `Path`：字段内部的动态尾路径，例如 map key 或嵌套字段名。

生成代码会传入 `FieldResolver`，把字段编号解析为字段名：

```text
FieldID(fieldGDRoleName) + Path{} -> Name
FieldID(fieldGDRoleItems) + Path{"1001", "quantity"} -> Items.1001.quantity
```

变更落地和下行投影不会对整个数据对象做全量 `json.Marshal`、`bson.Marshal` 或
`msgpack.Marshal`。`ChangeSet` 或 `DeltaSet` 只遍历本保存周期内的脏字段，通过生成的 resolver 或
`TypeSpec` 转成目标后端字段名，再生成局部 patch/update document。若某个脏字段本身是
map、slice 或 struct 指针，具体字段值在最终编码时仍会交给对应 JSON/BSON/Msgpack 编码器展开。
完整对象读取、回源或整文档替换路径才会依赖序列化库按 tag 映射字段。

### Change Document 压缩

少数高频有状态场景可能会先在内存里累积多份 `change.Document`，再批量写入存储。比如战场对象一帧内多次修改
`Hp`、`Pos`、`State`，如果每份变更都立即写入，会放大 IO；如果直接丢弃中间态，又容易破坏删除或父子路径语义。
这里的 document 可以来自快照 diff 路线，也可以来自 Store dirty 路线：前者是 `BeginChange` 后对比对象得到一轮 diff，
后者是 `Store().ChangeDocument()` 得到一轮显式标脏 diff。`CompactDocuments` 处理的是多轮 diff 文档的压缩，
不属于某一个具体保存 API。

`change.MergeDocuments(base, next)` 提供保守合并原语：能无歧义表达最终结果时返回合并文档，不能安全表达时返回
`ok=false`。常见可合并示例：

```go
doc, ok := change.MergeDocuments(
	change.Document{Set: map[string]any{"Name": "old"}},
	change.Document{Set: map[string]any{"Name": "new"}},
)
// ok == true，最终只保留 Set["Name"] = "new"
```

嵌套文档也会在两边都是文档时递归合并：

```go
doc, ok := change.MergeDocuments(
	change.Document{Set: map[string]any{"Battle": map[string]any{"Hp": int64(10)}}},
	change.Document{Set: map[string]any{"Battle": map[string]any{"Mp": int64(5)}}},
)
// doc.Set["Battle"] == map[string]any{"Hp": int64(10), "Mp": int64(5)}
```

如果前一份文档把父路径写成标量，后一份又更新子路径，合并会失败：

```go
_, ok := change.MergeDocuments(
	change.Document{Set: map[string]any{"Battle": int64(1)}},
	change.Document{Set: map[string]any{"Battle.Hp": int64(10)}},
)
// ok == false，需要调用方分成两次写入，避免把顺序语义压错。
```

队列场景使用 `change.CompactDocuments` 更方便：

```go
docs := change.CompactDocuments([]change.Document{
	{Set: map[string]any{"Name": "old"}},
	{Set: map[string]any{"Name": "new"}},
	{Set: map[string]any{"Battle": int64(1)}},
	{Set: map[string]any{"Battle.Hp": int64(10)}},
})
```

上面会得到两段结果：第一段包含 `Name=new` 和 `Battle=1`，第二段从 `Battle.Hp=10` 开始。也就是说，
压缩会尽力减少写入次数，但不会跨越不安全边界。

一个内存延迟保存队列可以这样组织：

```go
type rolePatchWriter interface {
	ApplyRolePatches(ctx context.Context, roleID int64, docs []change.Document) error
}

type roleDelaySaveQueue struct {
	roleID int64
	docs   []change.Document
}

func (q *roleDelaySaveQueue) PushDocument(doc change.Document) {
	q.docs = append(q.docs, doc)
}

func (q *roleDelaySaveQueue) Flush(ctx context.Context, writer rolePatchWriter) error {
	if len(q.docs) == 0 {
		return nil
	}
	compacted := change.CompactDocuments(q.docs)
	if err := writer.ApplyRolePatches(ctx, q.roleID, compacted); err != nil {
		return err
	}
	q.docs = q.docs[:0]
	return nil
}
```

Store 路线可以把每轮显式脏标记转成 document 入队：

```go
store := role.Store()
store.IncPower(10)
store.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 7})

queue.PushDocument(store.ChangeDocument())
store.ClearChanges()
```

快照 diff 路线也可以把一轮 diff 结果转成 document 后入队。具体捕获方式应由业务队列决定，例如封装一个
`SaveChangeToQueue(role)`，在内部完成 `BeginChange` 后的 diff、`ChangeDocument` 生成和 snapshot 清理。
关键点是：入队的是“某一轮已经确定的 diff 文档”，flush 时再做跨轮压缩。

```go
if err := queue.Flush(ctx, battlefieldPatchWriter); err != nil {
	return err
}
```

这个例子里的 `rolePatchWriter` 是业务高频队列自己的写入器：它可以把 compact 后的 `change.Document`
转换成 Redis/Mongo 的局部 patch，也可以合并进战斗服自己的内存落盘格式。当前通用 `storage.ChangeApplier`
仍然接收 `change.ChangeSet`，不直接接收 `change.Document`；因此不要把上面的队列写入器误解为现有
Fusion 延迟回写入口。Fusion 默认延迟回写仍是“队列只保存记录定位信息，到期后读取 Redis 最新热数据并整文档写回 Mongo”。

## Redis 存储模型

Redis 使用 string JSON 文档模型。

当前主线选择普通 Redis string 存储 JSON 文档，并通过 Lua 在写入时合并或替换文档。RedisJSON/ReJSON
可以作为后续可选 backend 单独评估，但不是当前默认路径；引入前需要确认目标云 Redis、测试环境和
Redis Cluster 行为都支持对应 JSON 命令。

### key 结构

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

### 写入流程

生成 Store 会把业务变更转成 `DeltaSet`，并优先交给实现了 `storage.DeltaApplier` 的 Redis adapter：

```go
role := data.NewGDRole()
role.SetRoleId(470001)
role.SetName("alice")
role.SetPower(47)

err := role.Store().ApplyInsert(ctx, redisCluster)
```

`ApplyInsert` 会强制 `Upsert=true`，适合新建或首次缓存。Redis adapter 收到 Delta 变更后会：

1. 根据 `TypeSpec` 和 `RecordKey` 生成 Redis key 前缀，例如 `role:{470001}`。
2. 根据字段的 Go 字段名定位 `redis:"subKey:..."`，再用 JSON 字段名写入 main 文档或 subKey 文档。
3. 把每个文档的 patch 编码成 JSON 字符串。
4. 调用 Lua 脚本，在 Redis 内部读取旧 JSON、`cjson.decode`、merge/replace、`cjson.encode` 后写回。
5. 按 `ApplyOptions.TTL` 设置或续期 Redis 热数据。

更新已有记录使用 `ApplyUpdate`：

```go
role.SetName("bob")

err := role.Store().ApplyUpdate(ctx, redisCluster)
```

`ApplyUpdate` 会强制 `Upsert=false`。如果 Redis 中不存在该记录，Redis adapter 会返回
`storage.ErrNotExists`，避免普通更新意外创建新热数据。

整字段替换使用 `Replace*` 或 `ApplyReplace`：

```go
role.ReplaceItems(map[int32]*data.GDItem{
	1001: &data.GDItem{ItemId: 1001, Quantity: 3},
})

err := role.Store().ApplyReplace(ctx, redisCluster)
```

`ApplyReplace` 会强制 `Upsert=true`，适合全量刷新一条记录的当前状态。Fusion 回源 Mongo 后回填 Redis 也会走整文档替换。

### TTL

生成 Store 会把 `@storage.delayDelTime` 作为 Redis 默认 TTL：

```go
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
```

上面配置表示 Redis 热数据默认 86400 秒过期。写入时如果调用方没有显式传 `ApplyOptions.TTL`，生成代码会自动填入该值。
显式 TTL 会覆盖默认值：

```go
err := role.Store().ApplyUpdateWithOptions(ctx, redisCluster, storage.ApplyOptions{
	TTL: 3600,
})
```

如果 `TTL > 0`，Redis 使用该 TTL 写入或续期；如果 `TTL == 0`，生成 Store 会补默认 TTL；如果底层直接调用 runtime 且没有生成 Store 补默认值，
则需要调用方自己明确 TTL 策略。Fusion 从 Mongo 回填 Redis 时要求 `delayDelTime > 0`，避免生成没有过期时间的热数据。

### 读取流程

单条读取：

```go
role, exists, err := data.LoadGDRole(ctx, redisCluster, data.GDRoleRecordKey(470001))
```

只读取部分字段：

```go
role, exists, err := data.LoadGDRoleFields(ctx, redisCluster, data.GDRoleRecordKey(470001), "Name", "Power")
```

如果只请求 subKey 字段，例如 `Items`，Redis 可以只读取对应 subKey 文档：

```go
role, exists, err := data.LoadGDRoleFields(ctx, redisCluster, data.GDRoleRecordKey(470001), "Items")
```

批量读取：

```go
stores, exists, err := data.LoadGDRoleStoresByID(ctx, redisCluster, []int64{1001, 1002, 1003})
```

如果类型的 `routeBy != idField`，生成器不会生成简单的 `Load*ByID(ids)`，因为仅有 id 无法构造 Redis endpoint 和 hash tag。
这类类型需要业务传入包含 routeBy 的 `RecordKey` 列表，或使用生成器后续提供的 route-aware helper。

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

常见情况一：`routeBy == idField`。

```go
// @redis:`routeType:"role" routeBy:"RoleId" idField:"RoleId" key:"role"`
key := data.GDRoleRecordKey(470001)
```

对应：

```go
storage.RecordKey{
	TypeName: "github.com/example/game/gen/data.GDRole",
	ID:       int64(470001),
}
```

常见情况二：`routeBy != idField`。

```go
// @redis:`routeType:"arena" routeBy:"RoleId" idField:"ID" key:"arena"`
key := storage.RecordKey{
	TypeName: data.GDArenaRecordTypeSpec().Type.Ref.FullName,
	ID:       int64(90001),
	ShardKey: int64(470001),
}
```

这里 `ID=90001` 是记录主键，`ShardKey=470001` 用于选择 Redis/Mongo endpoint 和生成 Redis hash tag。

读取部分字段时，生成代码会自动补充 `idField`；当 `routeBy != idField` 时也会补充 `routeBy`。这样即使业务只请求
`Name`，加载回来的对象仍然保留后续保存所需的记录定位信息。

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

### Mongo 配置与装配示例

Mongo 和 Redis 共用 `storage/config.RouteTableConfig` 这套 routeType 配置结构。最简单的本地配置可以把多个 routeType 指向同一个 Mongo endpoint：

```yaml
main: "mongodb://127.0.0.1:27017/txgame"
role: "mongodb://127.0.0.1:27017/txgame"
rank: "mongodb://127.0.0.1:27017/txgame"
```

如果生产环境按业务或 routeBy 区间拆分多个 Mongo endpoint，可以写成规则数组：

```yaml
role:
  - min: 1
    max: 999999
    shards:
      - "mongodb://role-a.example.com:27017/game"
      - "mongodb://role-b.example.com:27017/game"
  - min: 1000000
    shards:
      - "mongodb://role-c.example.com:27017/game"
arena:
  - "mongodb://arena-a.example.com:27017/game"
  - "mongodb://arena-b.example.com:27017/game"
```

启动时由业务工程读取 yml，创建官方 MongoDB driver client，再交给 tx 的薄适配层：

```go
var mongoRoutes storageconfig.RouteTableConfig
if err := yaml.Unmarshal(body, &mongoRoutes); err != nil {
	return err
}

routeTable, err := mongoRoutes.RouteTable()
if err != nil {
	return err
}

shards := map[string]any{}
for endpoint := range mongoRoutes.Endpoints() {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(endpoint))
	if err != nil {
		return err
	}
	dbName := databaseNameFromURI(endpoint)
	shards[endpoint] = txmongodriver.NewDatabase(client.Database(dbName))
}

mongoCluster := txmongo.NewCluster(routeTable, shards)
```

对应 import 路径：

```go
import (
	"github.com/ngnl47/tx/storage"
	storageconfig "github.com/ngnl47/tx/storage/config"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txmongodriver "github.com/ngnl47/tx/storage/mongo/mongodriver"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v3"
)
```

上面示例中的 `databaseNameFromURI` 由业务工程实现，用来从项目配置或 URI path 中决定 database 名称。tx runtime 不读取工程 yml，也不持有具体 Mongo client 生命周期；业务工程应在服务关闭时断开官方 driver client。

批量读取时，`mongo.Cluster` 会先按 `routeType` 和 `RecordKey.ShardKey` 找到 endpoint，再对每个 endpoint 发起一批 Mongo 查询，最后按输入顺序还原：

```go
stores, exists, err := data.LoadGDRoleStoresByID(ctx, mongoCluster, []int64{1001, 1002, 1003})
```

如果某个类型 `routeBy != idField`，生成的 `ByID` 批量 helper 会被跳过。业务需要显式构造带 route 字段的 `RecordKey`，保证每条记录能路由到正确 Mongo endpoint：

```go
keys := []storage.RecordKey{
	data.GDArenaRecordRecordKey(90001, 1001),
	data.GDArenaRecordRecordKey(90002, 1002),
}
stores, exists, err := data.LoadGDArenaRecordStores(ctx, mongoCluster, keys)
```

### Mongo filter 和字段名

`@mongodb.idField` 使用 Go 字段名，runtime 会通过该字段的 `bson` tag 推导 Mongo filter 字段：

```go
// @mongodb:`routeType:"role" routeBy:"RoleId" idField:"RoleId" collection:"roles"`
type GDRole struct {
	RoleId int64  `json:"roleId" bson:"_id" msgpack:"roleId"`
	Name   string `json:"name" bson:"name" msgpack:"name"`
}
```

读取、更新或删除 `RoleId=470001` 时，Mongo filter 是：

```go
bson.M{"_id": int64(470001)}
```

如果字段写的是 `bson:"roleId"`：

```go
RoleId int64 `json:"roleId" bson:"roleId" msgpack:"roleId"`
```

Mongo filter 会变成：

```go
bson.M{"roleId": int64(470001)}
```

因此，`bson:"_id"` 表示“这个 Go 字段绑定 Mongo 物理主键”；`idField:"RoleId"` 表示“tx 用这个 Go 字段作为业务记录身份”。
两者可以配合，也可以分离。

### 写入示例

插入或全量替换时，生成 Store 会启用 upsert：

```go
role := data.NewGDRole()
role.SetRoleId(470001)
role.SetName("alice")

err := role.Store().ApplyInsert(ctx, mongoCluster)
```

普通更新会关闭 upsert：

```go
role.SetName("bob")

err := role.Store().ApplyUpdate(ctx, mongoCluster)
```

如果 Mongo 中没有对应记录，普通更新不会新建文档。需要明确创建或替换时使用 `ApplyInsert` 或 `ApplyReplace`。

字段删除会转成 Mongo `$unset`：

```go
role.ClearName()

err := role.Store().ApplyUpdate(ctx, mongoCluster)
```

对应 Mongo update 语义接近：

```go
bson.M{
	"$unset": bson.M{"name": ""},
}
```

### 读取示例

完整读取：

```go
role, exists, err := data.LoadGDRole(ctx, mongoCluster, data.GDRoleRecordKey(470001))
```

部分字段读取：

```go
role, exists, err := data.LoadGDRoleFields(ctx, mongoCluster, data.GDRoleRecordKey(470001), "Name", "Power")
```

传入字段使用 Go 字段名，runtime 会转换为 Mongo projection 字段名。上例中 Mongo projection 使用 `name` 和 `power`。
生成代码会自动补充 `idField`，避免部分读取后的对象缺少记录键。

批量读取：

```go
stores, exists, err := data.LoadGDRoleStoresByID(ctx, mongoCluster, []int64{1001, 1002, 1003})
```

`mongo.Cluster` 会先按 routeType 和 shardKey 分组，再在每个 Mongo endpoint 内使用 `$in` 查询，最后按输入顺序还原结果。

### Mongo TTL 索引

字段级 `mongodb:"index,expireAfter:0"` 表示创建 Mongo TTL 索引。TTL 到期后 Mongo 删除的是整条文档：

```go
ExpireAt int64 `json:"expireAt" bson:"expireAt" msgpack:"expireAt" mongodb:"index,expireAfter:0"`
```

这通常用于临时房间、临时任务等可物理过期的数据。不要把它理解成“删除 ExpireAt 字段”。同一类型多个 TTL 字段会产生歧义，
生成阶段应直接拒绝。

## 通用文档接口

`tx/storage` 提供 Redis、Mongo、Fusion 和生成代码共用的文档接口：

- `DocumentLoader`：按 `TypeSpec + RecordKey` 读取单条文档。
- `BatchDocumentLoader`：按 `TypeSpec + []RecordKey` 批量读取文档。
- `DocumentReplacer`：按 `TypeSpec + RecordKey` 整文档替换。
- `DocumentReplacerWithOptions`：整文档替换并接收 `storage.ApplyOptions`，用于需要 TTL/upsert 等写入语义的路径。

这些接口是 Store 读取、Fusion 回源、Redis 热数据回填和延迟保存回写的共同边界。具体后端仍负责把
`RecordKey` 转成自己的底层定位方式，例如 Redis key/hash tag 或 Mongo filter。

业务侧通常不需要自己实现这些接口；只需要把 Redis/Mongo/Fusion adapter 传给生成方法：

```go
role, exists, err := data.LoadGDRole(ctx, fusionStrategy, data.GDRoleRecordKey(470001))
err = role.Store().ApplyUpdate(ctx, fusionStrategy)
```

需要写测试时，可以用内存假实现，只实现最小接口：

```go
type fakeLoader struct {
	doc map[string]any
}

func (f fakeLoader) LoadDocument(context.Context, meta.TypeSpec, storage.RecordKey, ...string) (map[string]any, bool, error) {
	return f.doc, f.doc != nil, nil
}
```

这样生成 Store 的加载逻辑可以在不启动 Redis/Mongo 的情况下单测。

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
Fusion delayed task queue 接受保存 action 和 Redis 热数据延迟删除 action；其他未知 action 会返回错误。入队时必须带有明确的类型和记录 ID，避免无效任务进入 Redis 队列；handler 消费端也会拒绝缺少记录 ID 的任务，防止手工写入或遗留异常任务落到存储层。任务类型优先来自 `DelayedOperation.Type`，为空时使用 `DelayedOperation.Key.TypeName`。Fusion 延迟保存按类型、记录 ID、route/shard key 和 action 生成一条合并任务，不暴露额外 suffix，避免同一记录产生多条重复保存任务。延迟保存任务不携带 ChangeSet，回写时重新读取 Redis 当前热数据。显式删除仍走显式删除接口，不进入延迟队列。
Scheduler 的 `Start`、`RunOnce` 和 `Schedule` 在自身或 backend 未配置时都是 no-op，便于本地测试或按环境关闭后台回写组件；生产环境仍应配置 Redis delayed task backend。

生产环境的延迟回写 handler 只负责存储同步。由于任务可能因为网络抖动、进程重启或租约恢复被再次执行，Mongo 回写路径应按“同一记录的当前 Redis 热文档 replace 到 Mongo”设计为存储幂等操作。比如同一条 `GDRole` save 任务执行两次，结果应只是 Mongo 中 `roleId=1001` 的文档被相同或更新后的热数据覆盖两次；队列中不保存 `GDRole` 的中间态快照，也不按每次字段变更生成事件日志。

Redis 热数据延迟删除任务也是存储同步的一部分：handler 执行时读取当前 `@v`，再读取 Redis 热文档并回写 Mongo，最后带着读取到的 revision 删除 Redis。如果回写 Mongo 期间这条记录又发生新写入，`@v` 会变化，Lua 删除会拒绝删除，handler 会重新排一条延迟删除任务。
handler 会在这个流程外层持有同一条记录的 Redis 锁，避免多个服务实例同时执行同一条记录的回写/删除窗口。

运维侧应关注 Scheduler 错误日志、重试次数和队列积压时间。如果某类任务持续失败，优先排查对应类型的 Redis 热文档是否能正常反序列化、Mongo 路由/过滤条件是否正确、以及目标集合是否可写。队列中的失败原因不会写入 Redis 状态；需要更完整的告警和指标时，应通过项目自己的日志、监控或后续可插拔 observer 接入。

Fusion 读取会先查 Redis，未命中时回源 Mongo。默认回源会先获取记录级 Redis 锁，并在锁内再次读取 Redis：如果第二次读取命中，直接返回 Redis 热数据；只有第二次仍未命中时，才从 Mongo 全量读取文档，写回 Redis 热数据并使用 `delayDelTime` 续期，然后按请求字段返回结果。该路径要求 `delayDelTime` 必须大于 0，避免生成没有 TTL 的 Redis 热数据。显式使用 `LoadOptions.DontLoadToRedis` 时，才直接从 Mongo 按请求字段读取且不回填 Redis，适合低频只读场景。

部分字段读取的优化主要发生在 Redis 热数据层：运行时按 main/subKey key 粒度减少 Redis 读取，随后按字段过滤返回文档；Redis 和 Mongo 中保存的仍是全量数据。只请求 subKey 字段时，Redis 可以只读取对应 subKey 文档，不要求主文档同时命中。

Fusion 显式删除不会进入延迟队列。删除路径应先删除 Redis 热数据，再立即删除 Mongo 文档；如果只绕过 Fusion 删除 Mongo，而 Redis 热数据和延迟保存任务仍存在，后续保存任务可能按 Redis 当前数据重新写回 Mongo。

### Fusion 初始化示例

Fusion 需要三类组件：

- Redis：同步热数据读写。
- Mongo：未命中回源、延迟回写和显式删除。
- DelayQueue：把“这条记录稍后保存到 Mongo”写入延迟任务队列。

示例：

```go
fusionStore := fusion.Strategy{
	Redis:         redisCluster,
	RedisLoader:   redisCluster,
	RedisDeleter:  redisCluster,
	RedisReplacer: redisCluster,
	RedisLocker:   redisCluster,
	MongoLoader:   mongoCluster,
	MongoDeleter:  mongoCluster,
	MongoQueue:    delayQueue,
}
```

业务写入：

```go
role.SetName("bob")

err := role.Store().ApplyUpdate(ctx, fusionStore)
```

执行后：

1. Redis 立即写入热数据。
2. DelayQueue 记录一条保存任务，任务到期时间来自 `delaySaveTime`。
3. 如果配置了 `delayDelTime > 0`，DelayQueue 还会记录一条 Redis 热数据延迟删除任务，到期时间为 `delaySaveTime + delayDelTime`。
4. 本次调用不等待 Mongo 立即写入。

### 延迟回写任务

延迟任务不保存业务对象快照，也不保存 `ChangeSet`。任务只保存记录定位信息：

```text
type = GDRole
id = 470001
shardKey = 470001
action = save
due = now + delaySaveTime
```

Scheduler 到期 claim 任务后，`StorageTaskHandler` 会重新读取 Redis 当前热数据，再整文档 replace 到 Mongo。
这意味着 10 分钟内同一条 `GDRole` 修改多次，Mongo 回写只需要使用 Redis 中的最新状态。

如果配置了 `delayDelTime`，写入时还会额外排一条 Redis 热数据延迟删除任务：

```text
type = GDRole
id = 470001
shardKey = 470001
action = delete
due = now + delaySaveTime + delayDelTime
```

删除任务到期后，handler 会先把 Redis 当前热数据回写 Mongo，再按 `<mainKey>@v` 比对删除 Redis。比如 `role:{470001}@v` 在任务开始时是 `5`，Mongo 回写期间业务又调用 `role.SetName("new")` 导致 `@v` 变成 `6`，删除 Lua 会返回当前版本 `6`，handler 不会删除 Redis，而是重新排一条 `delete` 任务。

### 多实例消费

Redis delayed task backend 使用 ZSet/hash 保存 due、payload、busy、token、attempts 等状态，并通过 Lua 保证
claim/complete/retry 的原子性。多个服务实例可以同时跑 Scheduler：

```text
server-a claim bucket 1 中的一批任务
server-b claim bucket 2 中的一批任务
server-c claim bucket 1 中租约过期后可恢复的任务
```

整体语义是 at-least-once：同一任务可能因为网络失败、进程重启或租约恢复被再次执行。因此 Mongo 回写必须是幂等的。
当前设计是“按记录键把 Redis 当前热文档 replace 到 Mongo”，重复执行只会覆盖同一条记录，不应产生额外业务副作用。

### 读取和回填

默认读取顺序：

1. 先读 Redis。
2. Redis 命中，直接返回。
3. Redis 未命中，获取记录级 Redis 锁。
4. 锁内再次读 Redis。
5. 第二次 Redis 命中，直接返回。
6. 第二次 Redis 仍未命中，读 Mongo 完整文档。
7. 将 Mongo 完整文档写回 Redis，并使用 `delayDelTime` 作为 Redis TTL。
8. 按请求字段返回给调用方。

代码示例：

```go
role, exists, err := data.LoadGDRole(ctx, fusionStore, data.GDRoleRecordKey(470001))
```

部分字段读取：

```go
role, exists, err := data.LoadGDRoleFields(ctx, fusionStore, data.GDRoleRecordKey(470001), "Name", "Power")
```

即使只请求 `Name` 和 `Power`，Redis miss 后默认也会从 Mongo 读取完整文档并回填 Redis。这样 Redis 继续承担热数据缓存职责。

这个锁用于保护并发回填。例如 `game-a` 和 `game-b` 同时读取 `role:{1001}` 未命中；`game-a` 先拿到锁、从 Mongo 读取并回填 Redis；`game-b` 随后拿到锁时会先二次读取 Redis，发现已经有热数据，于是不会再用自己稍后读到的 Mongo 快照覆盖 Redis。

如果明确是低频只读，不希望回填 Redis，可以使用 runtime option：

```go
doc, exists, err := fusionStore.LoadDocumentWithOptions(
	ctx,
	data.GDRoleTypeSpec(),
	data.GDRoleRecordKey(470001),
	fusion.LoadOptions{DontLoadToRedis: true},
	"Name",
)
```

这个路径只从 Mongo 读取请求字段，不写 Redis。

## TTL 与删除

`delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写。Fusion 延迟保存队列使用这个时间。

`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据；写入后会续期。显式传入 `ApplyOptions.TTL` 时，以显式值为准。Fusion 默认 Mongo 回源并回填 Redis 时必须配置该值。

`delayDelTime` 不代表 Mongo 删除延迟，也不会触发 Mongo 删除。业务是否执行 Mongo 物理删除，仍由显式 delete 接口和业务代码决定。

常见配置：

```go
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
```

含义：

| 字段 | 含义 |
| --- | --- |
| `delaySaveTime:"600"` | Redis 热数据变更后，约 10 分钟触发一次 Mongo 回写。 |
| `delayDelTime:"86400"` | Redis 热数据写入后 24 小时过期；再次写入会续期。 |

如果玩家持续在线且频繁修改，Redis 热数据会不断续期；Mongo 回写按延迟任务节奏合并。玩家长时间不活跃后，Redis key 到期释放内存；
再次加载时从 Mongo 读取完整数据并重新缓存到 Redis。

显式删除：

```go
err := role.Store().ApplyDelete(ctx, fusionStore)
```

Fusion 显式删除会：

1. 删除 Redis 主文档、subKey 文档和版本键。
2. 立即删除 Mongo 文档。
3. 不写入延迟保存队列。

Redis 热数据还维护一个生命周期版本键 `<mainKey>@v`，例如 `role:{1001}@v`。这个键不是业务字段，也不会写入 Mongo；它只用于 Redis 热数据过期/删除窗口的并发保护。运行时的带版本删除原语会在 Lua 中先比对 `@v`，只有版本一致才删除 main/subKey/version key；如果版本已变化，说明这条热数据发生过新写入，删除方应跳过本次删除并稍后重试。所有附属 key 都从 mainKey 派生：

| Redis key | 示例 | 用途 |
| --- | --- | --- |
| mainKey | `role:{1001}` | Redis 主文档。 |
| subKey | `role:{1001}:items` | 字段级 `redis:"subKey:items"` 拆出的子文档。 |
| version | `role:{1001}@v` | Redis 热数据生命周期版本。 |
| lock | `role:{1001}@lock` | Redis miss 回填时的记录级锁。 |

这些 key 共享 `{1001}` 这个 hash tag；当 `routeBy != idField` 时也会共享 routeBy 生成的 hash tag，例如 `arena_record:{470001}:90001`、`arena_record:{470001}:90001@v` 和 `arena_record:{470001}:90001@lock`。

如果业务希望“逻辑删除”，推荐在业务数据里定义字段：

```go
role.SetDeleted(true)
err := role.Store().ApplyUpdate(ctx, fusionStore)
```

框架不会替业务决定逻辑删除字段，也不会把 `delayDelTime` 解释成 Mongo 删除时间。

## 测试

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./... -count=1
```

## 排查清单

### 读取不到数据

- 确认生成包已经调用 `RegisterStorageRuntime` 或对应的 `SetDefault*Reader`。
- 确认 `@storage.mode` 与实际注册的 reader 匹配；Fusion 类型默认读 Fusion reader，Redis 类型默认读 Redis reader，Mongo 类型默认读 Mongo reader。
- 确认 `idField` 和 `routeBy` 使用 Go 字段名，例如 `RoleId`，不是 `roleId`、`_id` 或 msgpack 名。
- 如果 `routeBy != idField`，确认调用的是带 route 字段参数的 helper，或手动构造了带 `ShardKey` 的 `RecordKey`。
- Fusion 从 Mongo 回填 Redis 时要求 `delayDelTime` 大于 0；如果缺失，默认回填路径会失败。
- 部分读取传入的是 Go 字段名或生成常量，例如 `data.GDRoleGoFieldName`，不是 JSON/BSON/Msgpack 字段名。

### Redis 中没有热数据

- Redis mode 或 Fusion mode 写入后才会产生 Redis 热数据；Mongo-only 类型不会写 Redis。
- Fusion 读取 Mongo 后默认会回填 Redis，但 `LoadOptions.DontLoadToRedis` 会关闭回填。
- 检查 Redis routeType 是否能在 yml 路由表中找到 endpoint。
- 检查 `RecordKey.ShardKey` 是否正确；`routeBy != idField` 时缺少 ShardKey 会导致路由或 hash tag 不符合预期。
- 检查 `delayDelTime`，Redis key 可能已经按 TTL 过期。

### Mongo 没有立即更新

- Fusion 写入路径先同步写 Redis，再通过延迟任务回写 Mongo；这不是立即写 Mongo。
- `delaySaveTime` 决定保存任务到期时间，同一条记录在延迟窗口内多次修改会合并为读取 Redis 当前热文档后 replace Mongo。
- 确认 Redis delayed task backend 和 Scheduler 已在服务中启动。
- 确认 Scheduler 日志没有持续 claim、retry、complete 错误。
- 如果业务绕过 Fusion 直接删 Mongo，而 Redis 热数据和延迟保存任务还在，后续回写可能重新写入 Mongo。

### 更新没有字段变化

- 直接字段赋值不会标脏，例如 `role.Name = "bob"` 只改内存，不会进入 `ChangeSet`。
- 使用生成代理方法：`role.SetName("bob")`、`role.TouchItems()`、`role.UpdateItems(...)`。
- 如果业务先细粒度 touch path，再 `TouchItems()` 或 `AllTouch()`，顶层 dirty 会覆盖并清理更细的 path 记录。
- 保存成功后如果继续复用对象，调用 `ClearChanges()` 或 Store 的 `AllUnTouch()` 清掉本轮 tracker。

### 批量读取报 Redis cross-slot

- 默认批量读取面向单节点或云 Redis 代理 endpoint，直接使用原生 `MGET`。
- 本地原生 Redis Cluster 或直连 cluster 节点调试时，使用 `redis.LoadOptions{ClusterSafe: true}`。
- 不要把跨记录批量读取包进 Lua；Lua 脚本的 `KEYS` 必须在同一个 slot，云代理也不能可靠拆分自定义脚本。
